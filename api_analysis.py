#!/usr/bin/env python3
"""
免费账号传播分析 — 读取已爬推文，获取转发者 + 回复/引用

并行版: 7 并发，用免费 X 账号直连 GraphQL

用法:
    ./venv/bin/python3 api_analysis.py                         # 单批运行
    ./venv/bin/python3 api_analysis.py --daemon                # 持续守护: 自动拾取新推文

断点续爬: analysis_done.txt (tweet_id，每搜完即刻落盘)
实时保存: scweet_retweeters.csv / scweet_replies.csv (每条推文即刻写入)
"""
import os, re, time, csv, json, threading, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from curl_cffi import requests

DAEMON = "--daemon" in sys.argv
DAEMON_SLEEP = int(os.environ.get("DAEMON_SLEEP", "15"))

MAX_WORKERS = 7

RT_FIELDS = ["paper_id", "文献标题", "source_tweet_id",
             "screen_name", "name", "description",
             "followers_count", "verified", "user_id", "账号类型"]
RP_FIELDS = ["paper_id", "文献标题", "source_tweet_id",
             "类型", "tweet_id", "text", "screen_name",
             "name", "followers_count", "verified", "description",
             "likes", "retweets", "comments", "timestamp", "账号类型"]

_csv_lock = threading.Lock()

BEARER = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs=1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
QID_RETWEETERS = "gClaCb5tCk0z2iwAis8CwA"
QID_TWEET_DETAIL = "jd3V43oDY9cY7obs1YMfbQ"

GQL_FEATURES = {
    "rweb_video_screen_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "responsive_web_profile_redirect_enabled": False,
    "rweb_tipjar_consumption_enabled": False,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "premium_content_api_read_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": True,
    "responsive_web_jetfuel_frame": True,
    "responsive_web_grok_share_attachment_enabled": True,
    "responsive_web_grok_annotations_enabled": False,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "responsive_web_grok_show_grok_translated_post": False,
    "responsive_web_grok_analysis_button_from_backend": True,
    "post_ctas_fetch_enabled": True,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_grok_image_annotation_enabled": True,
    "responsive_web_grok_imagine_annotation_enabled": True,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_enhance_cards_enabled": False,
}

log_lock = threading.Lock()


def log(msg):
    with log_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def classify(name, bio):
    s = f"{name or ''} {bio or ''}".lower()
    if "bot" in (name or "").lower():
        return "机器人"
    if any(k in s for k in [
        "professor", "prof.", "phd", "ph.d", "postdoc", "researcher", "scientist",
        "lecturer", "faculty", "research fellow", "academic", "教授", "博士",
    ]):
        return "学者"
    if any(k in s for k in [
        "journalist", "reporter", "editor", "news", "magazine", "media", "记者", "编辑",
    ]):
        return "媒体"
    if any(k in s for k in [
        "university", "universit", "institute", "laboratory", "research lab",
        "department of", "journal", "press", "society", "ieee", "acm",
        ".edu", "实验室", "大学", "研究院", "学报", "official",
    ]):
        return "机构"
    return "个人"


def load_accounts():
    """从 scweet_state.db 加载所有可用账号。"""
    import sqlite3
    if not os.path.exists("scweet_state.db"):
        log("缺少 scweet_state.db")
        return []
    db = sqlite3.connect(f"file:scweet_state.db?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT username, auth_token, csrf, cookies_json FROM accounts "
        "WHERE auth_token IS NOT NULL AND auth_token != ''"
    ).fetchall()
    db.close()
    accounts = []
    for r in rows:
        cookies = {}
        if r["cookies_json"]:
            try:
                cj = json.loads(r["cookies_json"])
                if isinstance(cj, list):
                    for c in cj:
                        cookies[c["name"]] = c["value"]
                elif isinstance(cj, dict):
                    cookies = cj
            except Exception:
                pass
        ct0 = cookies.get("ct0") or r["csrf"] or ""
        accounts.append({
            "username": r["username"],
            "auth_token": r["auth_token"],
            "ct0": ct0,
            "busy": False,
        })
    return accounts


def build_session(acct):
    s = requests.Session(impersonate="chrome120")
    s.headers.update({
        "authorization": f"Bearer {BEARER}",
        "x-csrf-token": acct["ct0"],
        "x-twitter-auth-type": "OAuth2Session",
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
        "referer": "https://x.com/",
    })
    s.cookies.set("auth_token", acct["auth_token"])
    s.cookies.set("ct0", acct["ct0"])
    return s


def graphql_get(session, url, variables, features, retries=3):
    for attempt in range(retries):
        try:
            params = {
                "variables": json.dumps(variables, separators=(",", ":")),
                "features": json.dumps(features, separators=(",", ":")),
            }
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json(), False
            if r.status_code == 429:
                return None, True
            if r.status_code in (401, 403):
                return None, False
            log(f"  HTTP {r.status_code} (重试 {attempt+1}/{retries})")
            time.sleep(3)
        except Exception as e:
            log(f"  网络错误: {str(e)[:60]} (重试 {attempt+1}/{retries})")
            time.sleep(5)
    return None, False


def fetch_retweeters(session, tweet_id):
    variables = {"tweetId": tweet_id, "count": 20, "includePromotedContent": False}
    data, limited = graphql_get(
        session,
        f"https://x.com/i/api/graphql/{QID_RETWEETERS}/Retweeters",
        variables,
        GQL_FEATURES,
    )
    if limited:
        return [], True
    if not data:
        return [], False
    retweeters = []
    try:
        instructions = (data["data"]["retweeters_timeline"]["timeline"]["instructions"])
    except (KeyError, TypeError):
        return [], False
    for inst in instructions:
        if not isinstance(inst, dict):
            continue
        for entry in inst.get("entries") or []:
            if "user" not in entry.get("entryId", ""):
                continue
            ic = entry.get("content", {}).get("itemContent", {})
            ur = ic.get("user_results", {}).get("result", {})
            legacy = ur.get("legacy", {})
            if not legacy:
                continue
            core = ur.get("core", {})
            retweeters.append({
                "screen_name": core.get("screen_name", ""),
                "name": core.get("name", ""),
                "description": (legacy.get("description") or "")[:200],
                "followers_count": legacy.get("followers_count", 0),
                "verified": legacy.get("verified", False),
                "user_id": ur.get("rest_id", ""),
            })
    return retweeters, False


def fetch_tweet_detail(session, tweet_id):
    features = {**GQL_FEATURES,
        "withArticleRichContentState": True,
        "withArticlePlainText": False,
        "withArticleSummaryText": True,
    }
    variables = {
        "focalTweetId": tweet_id,
        "with_rux_injections": False,
        "includePromotedContent": False,
        "withCommunity": True,
        "withQuickPromoteEligibilityTweetFields": True,
        "withBirdwatchNotes": True,
        "withVoice": True,
        "withV2Timeline": True,
    }
    data, limited = graphql_get(
        session,
        f"https://x.com/i/api/graphql/{QID_TWEET_DETAIL}/TweetDetail",
        variables,
        features,
    )
    if limited:
        return [], [], True
    if not data:
        return [], [], False
    replies, quotes = [], []
    try:
        instructions = (data["data"]["threaded_conversation_with_injections_v2"]["instructions"])
    except (KeyError, TypeError):
        return [], [], False
    for inst in instructions:
        if not isinstance(inst, dict):
            continue
        for entry in inst.get("entries") or []:
            entry_id = entry.get("entryId", "")
            if entry_id.startswith("cursor-"):
                continue
            content = entry.get("content", {})
            ic = content.get("itemContent", {})
            tr = ic.get("tweet_results", {}).get("result", {})
            if not tr:
                continue
            typename = tr.get("__typename", "")
            if typename == "TweetWithVisibilityResults":
                tr = tr.get("tweet", {})
            legacy = tr.get("legacy", {})
            core = tr.get("core", {})
            user_result = core.get("user_results", {}).get("result", {})
            user_core = user_result.get("core", {})
            user_legacy = user_result.get("legacy", {})
            text = legacy.get("full_text", "")
            tid = legacy.get("id_str") or tr.get("rest_id", "")
            if not tid or not text:
                continue
            item = {
                "tweet_id": tid,
                "text": text.replace("\n", " ")[:500],
                "screen_name": user_core.get("screen_name", ""),
                "name": user_core.get("name", ""),
                "followers_count": user_legacy.get("followers_count", 0),
                "verified": user_legacy.get("verified", False),
                "description": (user_legacy.get("description") or "")[:200],
                "likes": legacy.get("favorite_count", 0),
                "retweets": legacy.get("retweet_count", 0),
                "comments": legacy.get("reply_count", 0),
                "timestamp": legacy.get("created_at", ""),
            }
            if legacy.get("is_quote_status", False):
                quotes.append(item)
            else:
                replies.append(item)
    return replies, quotes, False


def fetch_user_bio(session, screen_name):
    """通过 GraphQL UserByScreenName 获取用户简介。"""
    data, _ = graphql_get(
        session,
        f"https://x.com/i/api/graphql/2qvSHpkWTMS9i0zJAwDNiA/UserByScreenName",
        {"screen_name": screen_name, "withGrokTranslatedBio": True},
        GQL_FEATURES,
        retries=1,
    )
    if not data:
        return ""
    try:
        result = data["data"]["user"]["result"]
        legacy = result.get("legacy", {})
        return (legacy.get("description") or "")[:200]
    except (KeyError, TypeError):
        return ""


def fetch_tweet_stats(session, tweet_id):
    """获取单条推文的引用数(quote_count)和浏览数(view_count)。"""
    features = {**GQL_FEATURES,
        "withArticleRichContentState": True,
        "withArticlePlainText": False,
    }
    variables = {
        "focalTweetId": int(tweet_id),
        "with_rux_injections": False,
        "includePromotedContent": False,
        "withCommunity": True,
        "withQuickPromoteEligibilityTweetFields": True,
        "withBirdwatchNotes": True,
        "withVoice": True,
        "withV2Timeline": True,
    }
    data, limited = graphql_get(
        session,
        f"https://x.com/i/api/graphql/{QID_TWEET_DETAIL}/TweetDetail",
        variables, features, retries=1,
    )
    if limited or not data:
        return None, None
    try:
        instructions = data["data"]["threaded_conversation_with_injections_v2"]["instructions"]
    except (KeyError, TypeError):
        return None, None
    for inst in instructions:
        if not isinstance(inst, dict):
            continue
        for entry in inst.get("entries") or []:
            if entry.get("entryId", "").startswith("cursor-"):
                continue
            ic = entry.get("content", {}).get("itemContent", {})
            tr = ic.get("tweet_results", {}).get("result", {})
            if not tr:
                continue
            typename = tr.get("__typename", "")
            if typename == "TweetWithVisibilityResults":
                tr = tr.get("tweet", {})
            legacy = tr.get("legacy", {})
            if legacy.get("id_str") == str(tweet_id) or tr.get("rest_id") == str(tweet_id):
                qc = legacy.get("quote_count")
                vc = legacy.get("ext_views", {}).get("count") if "ext_views" in legacy else None
                if vc is None:
                    vc = tr.get("views", {}).get("count")
                return (int(qc) if qc is not None else None), (int(vc) if vc is not None else None)
    return None, None


_STATS_FIX_FILE = "stats_fix.json"

def backfill_missing_stats():
    """补全旧数据中缺失的引用、浏览、链接。"""
    import pandas as pd
    import json as _json
    if not os.path.exists("api_hits.csv"):
        return

    df = pd.read_csv("api_hits.csv")
    old = pd.read_csv("命中数据_合并.csv") if os.path.exists("命中数据_合并.csv") else pd.DataFrame()
    full = pd.concat([df, old], ignore_index=True).drop_duplicates(subset="tweet_id")

    # 确保字段存在
    for c in ["引用", "浏览", "链接"]:
        if c not in full.columns:
            full[c] = None if c in ("引用", "浏览") else ""

    # 筛出需要补的推文（只补 NaN，不改已有值）
    miss_quote = full["引用"].isna()
    miss_view = full["浏览"].isna()
    miss_link = full["链接"].isna() | (full["链接"].astype(str).str.strip() == "")

    quote_tids = set(full[miss_quote]["tweet_id"].unique())
    view_tids = set(full[miss_view]["tweet_id"].unique())
    link_tids = set(full[miss_link]["tweet_id"].unique())
    all_need = quote_tids | view_tids | link_tids

    if not all_need:
        log("补统计: 全部已有数据")
        return

    log(f"补统计: {len(all_need)} 条推文 - 缺引用{len(quote_tids)} 浏览{len(view_tids)} 链接{len(link_tids)}")

    # 补链接（不需要API）
    link_fixed = 0
    for tid in link_tids:
        mask = full["tweet_id"] == tid
        for idx in full[mask].index:
            sn = full.at[idx, "发推者"]
            if sn:
                full.at[idx, "链接"] = f"https://x.com/{sn}/status/{tid}"
                link_fixed += 1

    # 补引用/浏览（需要API）
    quote_fixed = view_fixed = 0
    todo = sorted(quote_tids | view_tids)
    for i, tid in enumerate(todo):
        q_missing = tid in quote_tids
        v_missing = tid in view_tids
        if not q_missing and not v_missing:
            continue
        for _attempt in range(2):
            acct = next_account()
            if not acct:
                break
            try:
                s = build_session(acct)
                qc, vc = fetch_tweet_stats(s, tid)
                s.close()
                if qc is not None and q_missing:
                    full.loc[full["tweet_id"] == tid, "引用"] = qc
                    quote_fixed += 1
                if vc is not None and v_missing:
                    full.loc[full["tweet_id"] == tid, "浏览"] = vc
                    view_fixed += 1
                break
            except Exception:
                continue
        if (i+1) % 50 == 0:
            log(f"  [{i+1}/{len(todo)}] 引用+{quote_fixed} 浏览+{view_fixed}")

    # 写回 api_hits.csv
    full["引用"] = full["引用"].fillna(0).astype(int)
    full["浏览"] = full["浏览"].fillna(0).astype(int)
    full["链接"] = full["链接"].fillna("").astype(str)
    api_mask = full["tweet_id"].isin(df["tweet_id"].unique())
    full[api_mask].to_csv("api_hits.csv", index=False, encoding="utf-8-sig")

    # 保存 stats 映射（用于导出时补旧数据）
    stats_map = {}
    for _, row in full.iterrows():
        stats_map[str(row["tweet_id"])] = {
            "引用": int(row["引用"]),
            "浏览": int(row["浏览"]),
            "链接": str(row["链接"]),
        }
    _json.dump(stats_map, open(_STATS_FIX_FILE, "w"), ensure_ascii=False)
    log(f"补统计完成: 引用+{quote_fixed} 浏览+{view_fixed} 链接+{link_fixed} 映射已保存")


def backfill_descriptions():
    """补全所有数据源中缺失的简介，用 author_bios.json 作缓存。"""
    import pandas as pd
    import json as _json
    
    # 加载缓存
    bio_map = {}
    if os.path.exists("author_bios.json"):
        bio_map = _json.load(open("author_bios.json"))

    # 从已有数据源收集已知 bios
    if os.path.exists("scweet_retweeters.csv"):
        rt = pd.read_csv("scweet_retweeters.csv")
        for _, r in rt.iterrows():
            sn = str(r.get("screen_name", "")).strip()
            desc = r.get("description", "")
            if sn and desc and pd.notna(desc) and str(desc).strip():
                bio_map[sn] = str(desc)[:200]

    if os.path.exists("scweet_replies.csv"):
        rp = pd.read_csv("scweet_replies.csv")
        for _, r in rp.iterrows():
            sn = str(r.get("screen_name", "")).strip()
            desc = r.get("description", "")
            if sn and desc and pd.notna(desc) and str(desc).strip():
                bio_map[sn] = str(desc)[:200]

    # 收集所有缺简介的发推者
    need = set()
    
    if os.path.exists("api_hits.csv"):
        df = pd.read_csv("api_hits.csv")
        col = "简介" if "简介" in df.columns else "发推者"
        for _, r in df.iterrows():
            sn = str(r.get("发推者", "")).strip()
            val = str(r.get(col, "")).strip()
            if sn and sn not in need and (col == "发推者" or not val):
                if sn not in bio_map:
                    need.add(sn)

    if os.path.exists("scweet_replies.csv"):
        rp = pd.read_csv("scweet_replies.csv")
        for _, r in rp.iterrows():
            sn = str(r.get("screen_name", "")).strip()
            desc = r.get("description", "")
            if sn and (pd.isna(desc) or not str(desc).strip()):
                if sn not in bio_map:
                    need.add(sn)

    need = [s for s in need if s]
    if not need:
        log(f"补简介: 全部已完整 ({len(bio_map)} 个)")
        _json.dump(bio_map, open("author_bios.json", "w"), ensure_ascii=False)
        return

    log(f"补简介: {len(need)} 个新用户 (已缓存 {len(bio_map)})")
    for i, sn in enumerate(need):
        for _attempt in range(2):
            acct = next_account()
            if not acct:
                break
            try:
                s = build_session(acct)
                bio = fetch_user_bio(s, sn)
                bio_map[sn] = bio or ""
                s.close()
                break
            except Exception:
                continue

        if (i + 1) % 50 == 0:
            log(f"  [{i+1}/{len(need)}] 已补 {len(bio_map)} 个")
            _json.dump(bio_map, open("author_bios.json", "w"), ensure_ascii=False)

    _json.dump(bio_map, open("author_bios.json", "w"), ensure_ascii=False)

    # 写回 api_hits.csv
    if os.path.exists("api_hits.csv"):
        df = pd.read_csv("api_hits.csv")
        df["简介"] = df["发推者"].map(lambda x: str(bio_map.get(str(x).strip(), "") or ""))
        df["简介"] = df["简介"].fillna("")
        df.to_csv("api_hits.csv", index=False, encoding="utf-8-sig")

    # 写回 scweet_replies.csv
    if os.path.exists("scweet_replies.csv"):
        rp = pd.read_csv("scweet_replies.csv")
        rp["description"] = rp["screen_name"].map(lambda x: str(bio_map.get(str(x).strip(), "") or ""))
        rp["description"] = rp["description"].fillna("")
        rp.to_csv("scweet_replies.csv", index=False, encoding="utf-8-sig")

    log(f"补简介完成: 共 {len(bio_map)} 个, +{len(need)} 新增")


def load_pending():
    """加载所有未分析的推文（跳过已有转发者/回复数据的）。"""
    done = set()
    if os.path.exists("analysis_done.txt"):
        done = set(open("analysis_done.txt").read().split())
    # 已有转发者数据的也不重复分析
    if os.path.exists("scweet_retweeters.csv"):
        try:
            df_old = pd.read_csv("scweet_retweeters.csv")
            done.update(df_old["source_tweet_id"].astype(str).unique())
        except Exception:
            pass
    if os.path.exists("scweet_replies.csv"):
        try:
            df_old = pd.read_csv("scweet_replies.csv")
            done.update(df_old["source_tweet_id"].astype(str).unique())
        except Exception:
            pass

    tweets = []
    for src in ["api_hits.csv", "命中数据_合并.csv"]:
        if not os.path.exists(src):
            continue
        df = pd.read_csv(src)
        for _, r in df.iterrows():
            tid = str(r["tweet_id"]).strip()
            if tid in done:
                continue
            if not tid or tid == "nan":
                continue
            tweets.append({
                "paper_id": r.get("paper_id", ""),
                "文献标题": str(r.get("文献标题", ""))[:80],
                "被引数": r.get("被引数", 0),
                "tweet_id": tid,
                "点赞": r.get("点赞", 0),
                "转发": r.get("转发", 0),
                "评论": r.get("评论", 0),
            })
    log(f"待分析推文: {len(tweets)} 条 (已跳过 {len(done)} 条)")
    return tweets


def init_csv():
    for path, fields in [("scweet_retweeters.csv", RT_FIELDS),
                         ("scweet_replies.csv", RP_FIELDS)]:
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, fieldnames=fields).writeheader()


import itertools as _itertools

_account_iter_lock = threading.Lock()
_account_iter = None

def next_account():
    global _account_iter
    with _account_iter_lock:
        if _account_iter is None:
            accts = load_accounts()
            if not accts:
                return None
            _account_iter = _itertools.cycle(accts)
        return next(_account_iter)


def process_tweet(tweet):
    """分析单条推文: 轮询分配一个账号, 限流则换下一个。
    返回 (retweeters_rows, replies_rows)。"""
    tid = tweet["tweet_id"]
    pid = tweet["paper_id"]
    title = tweet["文献标题"]

    all_rts, all_rps = [], []

    for attempt in range(2):  # 最多试2个不同账号
        acct = next_account()
        if not acct:
            break
        session = None
        try:
            session = build_session(acct)
            rts, limited = fetch_retweeters(session, tid)
            if limited:
                continue
            for rt in rts:
                rt["source_tweet_id"] = tid
                rt["paper_id"] = pid
                rt["文献标题"] = title
                rt["账号类型"] = classify(rt.get("name"), rt.get("description"))
                all_rts.append(rt)

            replies, quotes, limited2 = fetch_tweet_detail(session, tid)
            if limited2:
                continue

            for r in replies:
                r["source_tweet_id"] = tid
                r["paper_id"] = pid
                r["文献标题"] = title
                r["类型"] = "回复"
                r["账号类型"] = classify(r.get("name"), r.get("description"))
                all_rps.append(r)
            for q in quotes:
                q["source_tweet_id"] = tid
                q["paper_id"] = pid
                q["文献标题"] = title
                q["类型"] = "引用"
                q["账号类型"] = classify(q.get("name"), q.get("description"))
                all_rps.append(q)
            break
        except Exception as e:
            log(f"  [{acct['username']}] 异常: {str(e)[:60]}, 换号")
            continue
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass

    return all_rts, all_rps


def save_propagation():
    """生成推文传播汇总。"""
    log("生成传播汇总...")
    if not os.path.exists("scweet_retweeters.csv"):
        return

    df_hits = pd.concat([
        pd.read_csv(f) for f in ["api_hits.csv", "命中数据_合并.csv"] if os.path.exists(f)
    ], ignore_index=True)
    df_rt = pd.read_csv("scweet_retweeters.csv")
    df_rp = pd.read_csv("scweet_replies.csv") if os.path.exists("scweet_replies.csv") else pd.DataFrame()

    rt_counts = df_rt.groupby("source_tweet_id").size().to_dict() if not df_rt.empty else {}
    rp_counts = df_rp.groupby("source_tweet_id").size().to_dict() if not df_rp.empty else {}

    rows = []
    for pid, group in df_hits.groupby("paper_id"):
        title = group["文献标题"].iloc[0]
        cited = group["被引数"].iloc[0]
        n_tweets = len(group)
        total_likes = int(group["点赞"].sum())
        total_rt = int(group["转发"].sum())
        total_cmt = int(group["评论"].sum())

        type_counts = group["账号类型"].value_counts().to_dict()
        verified_count = int(group["认证"].astype(str).str.lower().isin(["true", "1", "yes"]).sum())
        max_followers = int(group["粉丝数"].max())
        top_idx = group["粉丝数"].idxmax()
        top_spreader = str(group.loc[top_idx, "发推者"])
        top_spreader_type = str(group.loc[top_idx, "账号类型"])

        times = group["时间"].dropna()
        earliest = times.min() if not times.empty else ""
        latest = times.max() if not times.empty else ""

        paper_rt_count = sum(rt_counts.get(int(tid), 0) for tid in group["tweet_id"])
        paper_rp_count = sum(rp_counts.get(int(tid), 0) for tid in group["tweet_id"])

        rows.append({
            "paper_id": pid,
            "文献标题": str(title)[:80],
            "被引数": cited,
            "推文数": n_tweets,
            "总点赞": total_likes,
            "总转发": total_rt,
            "总评论": total_cmt,
            "转发者数": paper_rt_count,
            "回复引用数": paper_rp_count,
            "学者推文": type_counts.get("学者", 0),
            "媒体推文": type_counts.get("媒体", 0),
            "机构推文": type_counts.get("机构", 0),
            "个人推文": type_counts.get("个人", 0),
            "机器人推文": type_counts.get("机器人", 0),
            "认证账号数": verified_count,
            "最大粉丝数": max_followers,
            "顶级传播者": f"@{top_spreader}",
            "顶级传播者类型": top_spreader_type,
            "最早推文": earliest,
            "最新推文": latest,
        })

    df_prop = pd.DataFrame(rows)
    prop_fields = ["paper_id", "文献标题", "被引数", "推文数",
                   "总点赞", "总转发", "总评论",
                   "转发者数", "回复引用数",
                   "学者推文", "媒体推文", "机构推文", "个人推文", "机器人推文",
                   "认证账号数", "最大粉丝数",
                   "顶级传播者", "顶级传播者类型",
                   "最早推文", "最新推文"]
    df_prop = df_prop[[c for c in prop_fields if c in df_prop.columns]]
    df_prop.to_csv("api_propagation.csv", index=False, encoding="utf-8-sig")
    log(f"传播汇总已保存: {len(df_prop)} 篇论文 -> api_propagation.csv")


def run_analysis(tweets):
    """分析一批推文，返回处理数。"""
    if not tweets:
        return 0

    n_total = len(tweets)
    n_done = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_tweet, t): t for t in tweets}

        for future in as_completed(futures):
            tweet = futures[future]
            tid = tweet["tweet_id"]
            try:
                rts, rps = future.result()
            except Exception as e:
                log(f"  [{tid}] 分析异常: {e}")
                with open("analysis_done.txt", "a") as f:
                    f.write(tid + "\n")
                n_done += 1
                continue

            if rts:
                with _csv_lock:
                    with open("scweet_retweeters.csv", "a", newline="", encoding="utf-8-sig") as f:
                        csv.DictWriter(f, fieldnames=RT_FIELDS).writerows(rts)
            if rps:
                with _csv_lock:
                    with open("scweet_replies.csv", "a", newline="", encoding="utf-8-sig") as f:
                        csv.DictWriter(f, fieldnames=RP_FIELDS).writerows(rps)

            with open("analysis_done.txt", "a") as f:
                f.write(tid + "\n")
            n_done += 1

            if n_done % 10 == 0:
                elapsed = time.time() - t_start
                rate = n_done / elapsed if elapsed > 0 else 0
                log(f"[{n_done}/{n_total}] 转发者 {sum(1 for _ in open('scweet_retweeters.csv'))-1 if os.path.exists('scweet_retweeters.csv') else 0} 人 | "
                    f"{rate:.1f}条/秒 | ETA {(n_total-n_done)/rate:.0f}秒" if rate > 0 else "")

    elapsed = time.time() - t_start
    log(f"  批次完成: {n_done} 条, {n_done/elapsed:.1f}条/秒 ({elapsed:.0f}秒)")
    return n_done


def main():
    init_csv()
    tweets = load_pending()
    if not tweets:
        log("无待分析推文")
        save_propagation()
    else:
        run_analysis(tweets)
        save_propagation()
    backfill_descriptions()


def main_daemon():
    """持续守护模式: 定期检查新推文并分析。"""
    log("=== 持续分析守护启动 ===")
    init_csv()
    _bf_count = 0
    while True:
        tweets = load_pending()
        if tweets:
            run_analysis(tweets)
            save_propagation()
        else:
            time.sleep(DAEMON_SLEEP)

        # 每空闲轮补简介 + 补统计
        _bf_count += 1
        if _bf_count >= 5:
            _bf_count = 0
            backfill_descriptions()
            backfill_missing_stats()


if __name__ == "__main__":
    if DAEMON:
        main_daemon()
    else:
        main()
