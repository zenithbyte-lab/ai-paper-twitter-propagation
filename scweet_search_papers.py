#!/usr/bin/env python3
# MUST run with ./venv/bin/python3 (system python has buggy curl_cffi 0.15.0)
"""
免费搜索筛选:用 Scweet(自己的X账号)批量搜文献标题,找出哪些有传播。
读 parquet -> 按被引排序取 Top N -> 逐篇精确搜标题 -> 命中存 CSV。
零 API 费用,只用你的 X 账号。

用法:
    export SCWEET_AUTH_TOKEN="你的auth_token"
    python3 scweet_search_papers.py            # 默认前 LIMIT 篇
    LIMIT=0 python3 scweet_search_papers.py     # 全量
"""
import os, re, time, csv, sys, logging, sqlite3, json, urllib.parse
import pandas as pd
from curl_cffi import requests as curl_requests
import curl_cffi
_ver = tuple(int(x) for x in curl_cffi.__version__.split(".")[:2])
if _ver >= (0, 12):
    raise SystemExit(
        f"curl_cffi {curl_cffi.__version__} has BoringSSL TLS bug. "
        f"Run with: ./venv/bin/python3 {sys.argv[0]}"
    )
# Remove CWD from sys.path so the local Scweet/ git repo dir doesn't shadow
# the editable-installed Scweet package (namespace package vs real package).
_cwd = os.path.abspath(".")
_orig_path = sys.path[:]
sys.path = [p for p in sys.path if os.path.abspath(p) != _cwd]
from Scweet import Scweet
from Scweet.config import ScweetConfig
from Scweet.exceptions import AccountPoolExhausted, NetworkError, RateLimitError
sys.path = _orig_path  # restore

logging.basicConfig(
    filename="search.log", level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    filemode="a",
)
_logfile = open("search.log", "a")
def _log(msg):
    print(msg, flush=True)
    _logfile.write(msg + "\n")
    _logfile.flush()

ACCOUNTS = "accounts.json"          # 多账号文件(轮换)
PARQUET = "wos_ai_core_dedup_with_paper_id.parquet"
OUT_CSV = "scweet_hits.csv"
OUT_PROPAGATION = "scweet_propagation.csv"
OUT_RETWEETERS = "scweet_retweeters.csv"
OUT_REPLIES = "scweet_replies.csv"
OUT_EXCEL = "scweet_results.xlsx"
DONE    = "scweet_done.txt"
LIMIT   = int(os.environ.get("LIMIT", "20"))   # 0=全量
SINCE   = "2015-01-01"
PER_Q   = 10      # 每篇最多取多少推(降低以减少翻页消耗)
SLEEP   = 1.0     # 每篇间隔(秒)

if not os.path.exists(ACCOUNTS):
    raise SystemExit(f"缺少 {ACCOUNTS}")

# curl_cffi 0.11.4 可以直连 x.com, 不需要代理(代理反而导致 TLS impersonation 失败)
# 如果直连被墙, 可通过 SCWEET_PROXY 环境变量设置代理

# 调高每日上限 + 把冷却设到 ~15分钟,贴合 X 真实限流窗口,避免无效重试
# transient_cooldown_s=60: 599 是瞬时网络抖动, 60s 足够恢复, 不用等 15 分钟
cfg = ScweetConfig(daily_requests_limit=2000, daily_tweets_limit=40000,
                   cooldown_default_s=900, transient_cooldown_s=60,
                   lease_ttl_s=60,
                   proxy_check_on_lease=False,
                   manifest_scrape_on_init=True)
s = Scweet(cookies_file=ACCOUNTS, config=cfg)
EXHAUST_BASE = 60       # 全限流时递增进度的起始等待秒数
EXHAUST_MAX  = 960      # 最长等待秒数(单账号恢复约 900s, 乘3号错峰)
DB_PATH = "scweet_state.db"

def _fmt_duration(secs):
    if secs < 60: return f"{int(secs)}s"
    m, s = divmod(int(secs), 60)
    return f"{m}m{s}s"

def query_account_status():
    """查询 Scweet 状态数据库, 返回每个账号的冷却信息和最短剩余冷却时间。"""
    if not os.path.exists(DB_PATH):
        return None, []
    try:
        db = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        now = time.time()
        rows = db.execute(
            "SELECT username, status, available_til, cooldown_reason, "
            "last_error_code, daily_requests, daily_tweets, busy "
            "FROM accounts ORDER BY available_til"
        ).fetchall()
        db.close()

        accounts = []
        min_remaining = None
        for r in rows:
            avail = r["available_til"]
            if avail and avail > now:
                remaining = avail - now
                if min_remaining is None or remaining < min_remaining:
                    min_remaining = remaining
            else:
                remaining = 0
            accounts.append({
                "username": r["username"],
                "status": r["status"],
                "reason": r["cooldown_reason"] or "-",
                "error_code": r["last_error_code"],
                "remaining": remaining,
                "daily_req": r["daily_requests"],
                "daily_tw": r["daily_tweets"],
            })
        return min_remaining, accounts
    except Exception as e:
        _log(f"  [DB] 查询账号状态失败: {e}")
        return None, []

def log_account_status():
    """打印每个账号的冷却状态表。"""
    min_remaining, accounts = query_account_status()
    if not accounts:
        return min_remaining
    _log(f"  ┌─ 账号冷却状态 ({len(accounts)} 个账号) ─────────────────────")
    for a in accounts:
        rem_str = _fmt_duration(a["remaining"]) if a["remaining"] > 0 else "✓ 可用"
        err = str(a["error_code"]) if a["error_code"] else "-"
        _log(f"  │ {a['username']:<8} {a['reason']:<15} err={err:<4} "
             f"冷却={rem_str:<8} 今日={a['daily_req']}req/{a['daily_tw']}tw")
    if min_remaining:
        _log(f"  └─ 最短冷却: {_fmt_duration(min_remaining)}")
    else:
        _log(f"  └─ 所有账号均已恢复")
    return min_remaining

# ════════════════════════════════════════════════════════════════════
# 直连 GraphQL 层 — 绕过 Scweet 账号池, per-endpoint 独立限流追踪
# ════════════════════════════════════════════════════════════════════

BEARER = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs=1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
QID_RETWEETERS = "gClaCb5tCk0z2iwAis8CwA"
QID_TWEET_DETAIL = "jd3V43oDY9cY7obs1YMfbQ"
EP_RETWEETERS = "https://x.com/i/api/graphql/{qid}/Retweeters"
EP_TWEET_DETAIL = "https://x.com/i/api/graphql/{qid}/TweetDetail"

# 从 default_manifest.json 提取的 features(2026-07-02)
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

# Per-endpoint per-account 429 冷却追踪 (独立于 Scweet 账号池)
# {endpoint: {username: cooldown_until_timestamp}}
_prop_cooldown = {}
_prop_rr_idx = 0  # round-robin 索引

def _prop_is_cooled(endpoint, username):
    """检查某账号在某端点是否仍在冷却。"""
    key = f"{endpoint}:{username}"
    until = _prop_cooldown.get(key, 0)
    return time.time() >= until

def _prop_set_cool(endpoint, username, secs=900):
    """设置某账号在某端点的冷却时间。"""
    key = f"{endpoint}:{username}"
    _prop_cooldown[key] = time.time() + secs

def _prop_get_available():
    """获取所有可用账号(有 auth_token)及其各端点冷却状态。"""
    if not os.path.exists(DB_PATH):
        return []
    try:
        db = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT username, auth_token, csrf, cookies_json "
            "FROM accounts WHERE auth_token IS NOT NULL AND auth_token != ''"
        ).fetchall()
        db.close()
        now = time.time()
        accounts = []
        for r in rows:
            uname = r["username"]
            cookies = {}
            if r["cookies_json"]:
                try:
                    cj = json.loads(r["cookies_json"])
                    if isinstance(cj, list):
                        for c in cj:
                            cookies[c.get("name", "")] = c.get("value", "")
                    elif isinstance(cj, dict):
                        cookies = cj
                except Exception:
                    pass
            ct0 = cookies.get("ct0") or r["csrf"] or ""
            accounts.append({
                "username": uname,
                "auth_token": r["auth_token"],
                "ct0": ct0,
                "cookies": cookies,
                "cd_retweeters": max(0, _prop_cooldown.get(f"retweeters:{uname}", 0) - now),
                "cd_tweet_detail": max(0, _prop_cooldown.get(f"tweet_detail:{uname}", 0) - now),
            })
        return accounts
    except Exception as e:
        _log(f"  [PROP] 读取账号失败: {e}")
        return []

def _prop_pick_account(endpoint):
    """选择一个在指定端点未冷却的账号(round-robin)。"""
    global _prop_rr_idx
    accts = _prop_get_available()
    if not accts:
        return None
    n = len(accts)
    for _ in range(n):
        idx = _prop_rr_idx % n
        _prop_rr_idx += 1
        acct = accts[idx]
        if _prop_is_cooled(endpoint, acct["username"]):
            return acct
    return None

def _prop_build_session(acct):
    """用账号凭据构建 curl_cffi Session(直连, 不走代理)。"""
    session = curl_requests.Session(impersonate="chrome120")
    headers = {
        "authorization": f"Bearer {BEARER}",
        "x-csrf-token": acct.get("ct0", ""),
        "x-twitter-auth-type": "OAuth2Session",
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
        "x-client-transaction-id": "",
        "referer": "https://x.com/",
        "sec-fetch-site": "same-origin",
    }
    session.headers.update(headers)
    cookies = {"auth_token": acct["auth_token"]}
    if acct.get("ct0"):
        cookies["ct0"] = acct["ct0"]
    # 合并原始 cookies (可能有额外的 twid 等)
    for k, v in (acct.get("cookies") or {}).items():
        if k not in cookies and k in ("twid", "guest_id", "guest_id_ads", "guest_id_marketing"):
            cookies[k] = v
    for k, v in cookies.items():
        session.cookies.set(k, v)
    return session

def _graphql_get_direct(endpoint, qid, variables, features=None, *,
                        retries=3, max_wait=120):
    """直连 GraphQL GET 请求, per-endpoint per-account 429 追踪。
    返回 (data_dict, rate_limited_bool)。
    """
    for attempt in range(retries):
        acct = _prop_pick_account(endpoint)
        if acct is None:
            # 所有账号在此端点都冷却了
            min_cd = float("inf")
            for a in _prop_get_available():
                cd = a.get(f"cd_{endpoint}", 0)
                if cd < min_cd:
                    min_cd = cd
            if min_cd < float("inf") and min_cd > 0:
                wait = min(min_cd + 5, max_wait)
                _log(f"  [PROP] 所有账号 {endpoint} 冷却中, 等 {_fmt_duration(wait)}")
                time.sleep(wait)
                continue
            _log(f"  [PROP] {endpoint} 无可用账号")
            return None, True

        session = _prop_build_session(acct)
        url = EP_RETWEETERS.format(qid=qid) if endpoint == "retweeters" \
              else EP_TWEET_DETAIL.format(qid=qid)
        params = {
            "variables": json.dumps(variables, separators=(",", ":")),
            "features": json.dumps(features or GQL_FEATURES, separators=(",", ":")),
        }
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json(), False
            if resp.status_code == 429:
                _prop_set_cool(endpoint, acct["username"], 900)
                _log(f"  [PROP] {acct['username']} {endpoint} 429, 冷却15min "
                     f"(该端点独立于搜索, 不影响搜索)")
                session.close()
                continue
            if resp.status_code in (401, 403):
                _log(f"  [PROP] {acct['username']} {endpoint} auth err {resp.status_code}")
                _prop_set_cool(endpoint, acct["username"], 300)
                session.close()
                continue
            _log(f"  [PROP] {acct['username']} {endpoint} HTTP {resp.status_code}")
            session.close()
            if attempt < retries - 1:
                time.sleep(3)
                continue
            return None, False
        except Exception as e:
            _log(f"  [PROP] {acct['username']} {endpoint} 网络错误: {str(e)[:80]}")
            session.close()
            if attempt < retries - 1:
                time.sleep(3)
                continue
            return None, False
        finally:
            try:
                session.close()
            except Exception:
                pass
    return None, True

def _extract_retweeters_from_data(data):
    """从 Retweeters GraphQL 响应中提取转发者列表和下页 cursor。"""
    retweeters = []
    cursor = None
    try:
        instructions = (data.get("data", {})
                        .get("retweeters_timeline", {})
                        .get("timeline", {})
                        .get("instructions", []))
    except Exception:
        return retweeters, cursor
    for inst in instructions:
        if not isinstance(inst, dict):
            continue
        entries = inst.get("entries") or inst.get("items") or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_id = entry.get("entryId", "")
            # Cursor entries
            if "cursor-bottom" in entry_id or "cursor-show-more" in entry_id:
                try:
                    cursor = entry["content"]["value"]
                except (KeyError, TypeError):
                    pass
                continue
            # User entry
            ic = entry.get("content", {}).get("itemContent", {})
            if not ic:
                continue
            ur = ic.get("user_results", {}).get("result", {})
            if not ur:
                continue
            legacy = ur.get("legacy", {})
            if not legacy:
                continue
            retweeters.append({
                "screen_name": legacy.get("screen_name", ""),
                "name": legacy.get("name", ""),
                "description": (legacy.get("description") or "")[:200],
                "followers_count": legacy.get("followers_count", 0),
                "verified": legacy.get("verified", False),
                "user_id": ur.get("rest_id", ""),
            })
    return retweeters, cursor

def _fetch_retweeters_direct(tweet_ids, max_per_tweet=50):
    """直连获取转发者列表(绕过 Scweet 账号池)。
    返回 (all_retweeters_list, was_rate_limited_bool)。
    """
    all_retweeters = []
    rate_limited = False
    for tid in tweet_ids:
        cursor = None
        collected = 0
        page = 0
        while page < 5:  # 每条推文最多翻5页
            variables = {
                "tweetId": tid,
                "count": 20,
                "includePromotedContent": False,
            }
            if cursor:
                variables["cursor"] = cursor
            data, rl = _graphql_get_direct("retweeters", QID_RETWEETERS, variables)
            if rl:
                rate_limited = True
                break
            if data is None:
                break
            rts, next_cursor = _extract_retweeters_from_data(data)
            if not rts:
                break
            for rt in rts:
                rt["source_tweet_id"] = tid
            all_retweeters.extend(rts)
            collected += len(rts)
            page += 1
            if not next_cursor or next_cursor == cursor or collected >= max_per_tweet:
                break
            cursor = next_cursor
        if rate_limited:
            break
        time.sleep(0.5)  # 推文间短暂间隔
    return all_retweeters, rate_limited

def _extract_tweet_detail_entries(data):
    """从 TweetDetail GraphQL 响应中提取回复和引用推文。
    响应结构: data.threaded_conversation_with_injections_v2.instructions
    """
    replies = []
    quotes = []
    try:
        instructions = (data.get("data", {})
                        .get("threaded_conversation_with_injections_v2", {})
                        .get("instructions", []))
    except Exception:
        return replies, quotes
    for inst in instructions:
        if not isinstance(inst, dict):
            continue
        entries = inst.get("entries") or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_id = entry.get("entryId", "")
            if entry_id.startswith("cursor-"):
                continue
            content = entry.get("content", {})
            ic = content.get("itemContent", {})
            if not ic:
                continue
            tweet_result = ic.get("tweet_results", {}).get("result", {})
            if not tweet_result:
                continue
            info = _parse_tweet_detail_result(tweet_result)
            if not info:
                continue
            # 判断是回复还是引用
            if info.get("is_quote"):
                quotes.append(info)
            else:
                replies.append(info)
            if len(replies) >= 30 and len(quotes) >= 20:
                break
        if len(replies) >= 30 and len(quotes) >= 20:
            break
    return replies, quotes

def _parse_tweet_detail_result(tweet_result):
    """解析单条 TweetDetail 结果, 提取推文信息。"""
    if not tweet_result or not isinstance(tweet_result, dict):
        return None
    # __typename 可能是 Tweet 或 TweetWithVisibilityResults
    typename = tweet_result.get("__typename", "")
    if typename == "TweetWithVisibilityResults":
        tweet_result = tweet_result.get("tweet", {})
    legacy = tweet_result.get("legacy", {})
    core = tweet_result.get("core", {})
    user_result = core.get("user_results", {}).get("result", {})
    user_legacy = user_result.get("legacy", {})
    text = legacy.get("full_text", "")
    tid = legacy.get("id_str") or tweet_result.get("rest_id", "")
    if not tid or not text:
        return None
    return {
        "tweet_id": tid,
        "text": text.replace("\n", " ")[:500],
        "screen_name": user_legacy.get("screen_name", ""),
        "name": user_legacy.get("name", ""),
        "followers_count": user_legacy.get("followers_count", 0),
        "verified": user_legacy.get("verified", False),
        "likes": legacy.get("favorite_count", 0),
        "retweets": legacy.get("retweet_count", 0),
        "comments": legacy.get("reply_count", 0),
        "timestamp": legacy.get("created_at", ""),
        "is_quote": legacy.get("is_quote_status", False),
    }

def _fetch_tweet_detail_direct(tweet_ids):
    """直连获取推文详情(回复+引用)。
    返回 (replies_list, quotes_list, was_rate_limited_bool)。
    """
    all_replies = []
    all_quotes = []
    rate_limited = False
    tweet_detail_toggles = {
        "withArticleRichContentState": True,
        "withArticlePlainText": False,
        "withArticleSummaryText": True,
        "withArticleVoiceOver": True,
        "withGrokAnalyze": False,
        "withDisallowedReplyControls": False,
    }
    merged_features = {**GQL_FEATURES, **tweet_detail_toggles}
    for tid in tweet_ids:
        variables = {
            "focalTweetId": tid,
            "with_rux_injections": False,
            "includePromotedContent": False,
            "withCommunity": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withBirdwatchNotes": True,
            "withVoice": True,
            "withV2Timeline": True,
        }
        data, rl = _graphql_get_direct("tweet_detail", QID_TWEET_DETAIL,
                                       variables, features=merged_features)
        if rl:
            rate_limited = True
            break
        if data is None:
            continue
        replies, quotes = _extract_tweet_detail_entries(data)
        for r in replies:
            r["source_tweet_id"] = tid
        for q in quotes:
            q["source_tweet_id"] = tid
        all_replies.extend(replies)
        all_quotes.extend(quotes)
        time.sleep(0.5)
    return all_replies, all_quotes, rate_limited

def classify(name, bio):
    s = f"{name or ''} {bio or ''}".lower()
    if "bot" in (name or "").lower(): return "机器人"
    if any(k in s for k in ["professor","prof.","phd","ph.d","postdoc","researcher","scientist",
        "lecturer","faculty","research fellow","academic","教授","博士"]): return "学者"
    if any(k in s for k in ["journalist","reporter","editor","news","magazine","media","记者","编辑"]): return "媒体"
    if any(k in s for k in ["university","universit","institute","laboratory","research lab",
        "department of","journal","press","society","ieee","acm",".edu","实验室","大学","研究院","学报","official"]): return "机构"
    return "个人"

def relevant(title, text):
    w = {x.lower() for x in re.findall(r"[A-Za-z]{4,}", title)}
    if not w: return True
    t = (text or "").lower()
    return sum(1 for x in w if x in t) / len(w) >= 0.5

def load_done():
    return set(open(DONE).read().split()) if os.path.exists(DONE) else set()

def generate_excel():
    """生成多 sheet Excel (匹配命中数据_合并.xlsx 格式 + 转发者 + 回复引用)。"""
    _log(f"\n生成 Excel: {OUT_EXCEL}")
    sheets = {}

    # Sheet 0: 说明
    desc_data = {
        "Sheet名": ["0_说明", "1_文献汇总", "2_推文明细", "3_转发者", "4_回复引用", "5_账号类型统计"],
        "内容": [
            "各sheet说明",
            "每篇命中文献的传播汇总(含转发者/回复引用计数)",
            "所有命中的推文明细",
            "转发者信息(来源推文+转发者账号)",
            "回复和引用推文信息",
            "按账号类型聚合统计",
        ],
        "生成时间": [time.strftime("%Y-%m-%d %H:%M:%S")] + [""] * 5,
    }
    sheets["0_说明"] = pd.DataFrame(desc_data)

    # Sheet 1: 文献汇总
    if os.path.exists(OUT_PROPAGATION) and os.path.getsize(OUT_PROPAGATION) > 0:
        prop_df = pd.read_csv(OUT_PROPAGATION, encoding="utf-8-sig")
        # 从 CSV 补充转发者/回复引用计数
        rt_counts = {}
        rp_counts = {}
        if os.path.exists(OUT_RETWEETERS):
            try:
                rt_df = pd.read_csv(OUT_RETWEETERS, encoding="utf-8-sig")
                rt_counts = rt_df.groupby("source_tweet_id").size().to_dict()
            except Exception:
                pass
        if os.path.exists(OUT_REPLIES):
            try:
                rp_df = pd.read_csv(OUT_REPLIES, encoding="utf-8-sig")
                rp_counts = rp_df.groupby("source_tweet_id").size().to_dict()
            except Exception:
                pass
        # 汇总每篇论文的转发者数和回复数
        if os.path.exists(OUT_CSV):
            try:
                hits_df = pd.read_csv(OUT_CSV, encoding="utf-8-sig")
                paper_rt = {}
                paper_rp = {}
                for _, row in hits_df.iterrows():
                    tid = str(row.get("tweet_id", ""))
                    pid = row.get("paper_id", "")
                    if tid in rt_counts:
                        paper_rt[pid] = paper_rt.get(pid, 0) + rt_counts[tid]
                    if tid in rp_counts:
                        paper_rp[pid] = paper_rp.get(pid, 0) + rp_counts[tid]
                prop_df["转发者数"] = prop_df["paper_id"].map(paper_rt).fillna(0).astype(int)
                prop_df["回复引用数"] = prop_df["paper_id"].map(paper_rp).fillna(0).astype(int)
            except Exception:
                pass
        sheets["1_文献汇总"] = prop_df
    else:
        sheets["1_文献汇总"] = pd.DataFrame()

    # Sheet 2: 推文明细
    if os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0:
        sheets["2_推文明细"] = pd.read_csv(OUT_CSV, encoding="utf-8-sig")
    else:
        sheets["2_推文明细"] = pd.DataFrame()

    # Sheet 3: 转发者
    if os.path.exists(OUT_RETWEETERS) and os.path.getsize(OUT_RETWEETERS) > 0:
        sheets["3_转发者"] = pd.read_csv(OUT_RETWEETERS, encoding="utf-8-sig")
    else:
        sheets["3_转发者"] = pd.DataFrame()

    # Sheet 4: 回复引用
    if os.path.exists(OUT_REPLIES) and os.path.getsize(OUT_REPLIES) > 0:
        sheets["4_回复引用"] = pd.read_csv(OUT_REPLIES, encoding="utf-8-sig")
    else:
        sheets["4_回复引用"] = pd.DataFrame()

    # Sheet 5: 账号类型统计
    if "2_推文明细" in sheets and not sheets["2_推文明细"].empty:
        hits_df = sheets["2_推文明细"]
        type_stats = hits_df.groupby("账号类型").agg(
            推文数=("tweet_id", "count"),
            平均粉丝数=("粉丝数", lambda x: int(x.mean()) if len(x) else 0),
            认证账号数=("认证", "sum"),
        ).reset_index()
        type_stats["占比"] = (type_stats["推文数"] / type_stats["推文数"].sum() * 100).round(1)
        sheets["5_账号类型统计"] = type_stats
    else:
        sheets["5_账号类型统计"] = pd.DataFrame()

    with pd.ExcelWriter(OUT_EXCEL, engine="openpyxl") as ew:
        for name, df in sheets.items():
            df.to_excel(ew, sheet_name=name, index=False)
    _log(f"Excel 已保存: {OUT_EXCEL} ({len(sheets)} sheets)")

def main():
    df = pd.read_parquet(PARQUET)
    df = df[df["TI"].astype(str).str.split().str.len() >= 4]
    df = df.sort_values("_times_cited_num", ascending=False)
    done = load_done()
    df = df[~df["paper_id"].isin(done)]
    if LIMIT:
        df = df.head(LIMIT)

    csv_exists = os.path.exists(OUT_CSV)
    fout = open(OUT_CSV, "a", newline="", encoding="utf-8-sig")
    writer = None

    prop_exists = os.path.exists(OUT_PROPAGATION)
    prop_fout = open(OUT_PROPAGATION, "a", newline="", encoding="utf-8-sig")
    prop_writer = None

    # ── 转发者 CSV ──
    rt_exists = os.path.exists(OUT_RETWEETERS)
    rt_fout = open(OUT_RETWEETERS, "a", newline="", encoding="utf-8-sig")
    rt_writer = None

    # ── 回复引用 CSV ──
    rp_exists = os.path.exists(OUT_REPLIES)
    rp_fout = open(OUT_REPLIES, "a", newline="", encoding="utf-8-sig")
    rp_writer = None

    n_hit = n_done = 0
    n_rt_total = 0   # 转发者总数
    n_rp_total = 0   # 回复引用总数
    backoff = 0  # 全限流时递增等待, 按指数增长
    total_papers = len(df)
    _log(f"\n{'='*60}")
    _log(f"开始搜索: {total_papers} 篇文献 (已跳过 {len(done)} 篇已处理)")
    _log(f"{'='*60}")

    for _, p in df.iterrows():
        pid = p["paper_id"]
        if pid in done:
            continue
        title = str(p["TI"]).strip()
        net_retries = 0          # 网络瞬时错误连续计数
        NET_RETRY_MAX = 3       # 网络错误最多短重试几次后才走长退避
        NET_RETRY_WAIT = 5      # 网络错误短重试等待秒数
        for attempt in range(10):
            try:
                tweets = s.search(query=f'"{title}"', since=SINCE,
                                  display_type="Top", limit=PER_Q)
                backoff = 0
                net_retries = 0
                break
            except AccountPoolExhausted:
                _log(f"  [!] 全限流 — 查询各账号状态...")
                min_wait = log_account_status()
                if min_wait and min_wait > 0:
                    wait_s = min_wait + 5  # 加 5s 缓冲
                    _log(f"  [!] 智能等待 {_fmt_duration(wait_s)} (最短冷却+5s)")
                else:
                    # DB 查不到或已全部恢复, 回退到指数退避
                    backoff = backoff * 2 if backoff else EXHAUST_BASE
                    backoff = min(backoff, EXHAUST_MAX)
                    wait_s = backoff
                    _log(f"  [!] 回退指数退避, 等 {_fmt_duration(wait_s)}")
                time.sleep(wait_s)
                continue
            except NetworkError as e:
                # SSL/连接瞬时错误 — 短暂重试, 不用长退避
                net_retries += 1
                if net_retries <= NET_RETRY_MAX:
                    _log(f"  [NET] 网络错误({net_retries}/{NET_RETRY_MAX}), {NET_RETRY_WAIT}s后重试: {str(e)[:60]}")
                    time.sleep(NET_RETRY_WAIT)
                    continue
                # 连续网络错误超限, 当作限流处理
                backoff = backoff * 2 if backoff else EXHAUST_BASE
                backoff = min(backoff, EXHAUST_MAX)
                _log(f"  [NET→限流] 网络错误持续, 等 {backoff}s ...")
                net_retries = 0
                time.sleep(backoff)
                continue
            except RateLimitError as e:
                _log(f"  [429-REAL] X API 限流: {str(e)[:60]}")
                min_wait = log_account_status()
                if min_wait and min_wait > 0:
                    wait_s = min_wait + 5
                    _log(f"  [429-REAL] 智能等待 {_fmt_duration(wait_s)}")
                else:
                    backoff = backoff * 2 if backoff else EXHAUST_BASE
                    backoff = min(backoff, EXHAUST_MAX)
                    wait_s = backoff
                    _log(f"  [429-REAL] 回退指数退避, 等 {_fmt_duration(wait_s)}")
                time.sleep(wait_s)
                continue
            except Exception as e:
                em = str(e)
                if "429" in em or "rate limit" in em.lower() or "exhaust" in em.lower():
                    backoff = backoff * 2 if backoff else EXHAUST_BASE
                    backoff = min(backoff, EXHAUST_MAX)
                    _log(f"  [429] 第{attempt+1}次重试, 等 {backoff}s ...")
                    time.sleep(backoff)
                    continue
                _log(f"  [{pid}] 出错 {em[:60]}")
                time.sleep(SLEEP)
                break
        else:
            _log(f"  [{pid}] 重试耗尽, 跳过")
            with open(DONE, "a") as f: f.write(pid + "\n")
            n_done += 1
            continue

        if 'tweets' not in dir() or not tweets:
            with open(DONE, "a") as f: f.write(pid + "\n")
            n_done += 1
            continue

        rows = []
        for t in tweets:
            txt = (t.get("text") or "").replace("\n", " ")
            if not relevant(title, txt): continue
            u = t.get("user") or {}
            rows.append({
                "paper_id": pid, "文献标题": title[:80], "被引数": p["_times_cited_num"],
                "tweet_id": t.get("tweet_id"), "推文内容": txt,
                "点赞": t.get("likes"), "转发": t.get("retweets"), "评论": t.get("comments"),
                "发推者": u.get("screen_name"), "昵称": u.get("name"),
                "时间": t.get("timestamp"), "链接": t.get("tweet_url"),
                "粉丝数": u.get("followers_count"), "认证": u.get("verified"),
                "账号类型": classify(u.get("name"), u.get("description")),
            })
        if rows:
            n_hit += 1
            if writer is None:
                writer = csv.DictWriter(fout, fieldnames=list(rows[0].keys()))
                if not csv_exists: writer.writeheader()
            writer.writerows(rows); fout.flush()

            # ── 传播链分析 ──
            total_likes = sum(int(r.get("点赞") or 0) for r in rows)
            total_rt = sum(int(r.get("转发") or 0) for r in rows)
            total_cmt = sum(int(r.get("评论") or 0) for r in rows)
            type_counts = {}
            for r in rows:
                t = r.get("账号类型", "个人")
                type_counts[t] = type_counts.get(t, 0) + 1
            timestamps = [r.get("时间") for r in rows if r.get("时间")]
            earliest = min(timestamps) if timestamps else ""
            latest = max(timestamps) if timestamps else ""
            max_followers = max(int(r.get("粉丝数") or 0) for r in rows)
            top_spreader = max(rows, key=lambda r: int(r.get("粉丝数") or 0))
            verified_count = sum(1 for r in rows if r.get("认证"))

            prop_row = {
                "paper_id": pid, "文献标题": title[:80], "被引数": p["_times_cited_num"],
                "推文数": len(rows), "总点赞": total_likes, "总转发": total_rt, "总评论": total_cmt,
                "学者推文": type_counts.get("学者", 0), "媒体推文": type_counts.get("媒体", 0),
                "机构推文": type_counts.get("机构", 0), "个人推文": type_counts.get("个人", 0),
                "机器人推文": type_counts.get("机器人", 0),
                "认证账号数": verified_count, "最大粉丝数": max_followers,
                "顶级传播者": f"@{top_spreader.get('发推者', '')}",
                "顶级传播者粉丝": max_followers,
                "顶级传播者类型": top_spreader.get("账号类型", ""),
                "最早推文": earliest, "最新推文": latest,
            }
            if prop_writer is None:
                prop_writer = csv.DictWriter(prop_fout, fieldnames=list(prop_row.keys()))
                if not prop_exists: prop_writer.writeheader()
            prop_writer.writerow(prop_row); prop_fout.flush()

            _log(f"  [{pid}] 命中 {len(rows)} 条 | {title[:45]}")
            _log(f"         传播: {total_likes}赞/{total_rt}转/{total_cmt}评 "
                 f"学者{type_counts.get('学者',0)}/媒体{type_counts.get('媒体',0)}/"
                 f"机构{type_counts.get('机构',0)}/个人{type_counts.get('个人',0)} "
                 f"最大粉{max_followers} @{top_spreader.get('发推者','')}")

            # ── 传播链抓取(独立限流, 不影响搜索账号池) ──
            tweet_ids_for_prop = [str(r["tweet_id"]) for r in rows if r.get("tweet_id")]
            if tweet_ids_for_prop:
                # 转发者
                try:
                    retweeters, rt_limited = _fetch_retweeters_direct(tweet_ids_for_prop)
                    if retweeters:
                        n_rt_total += len(retweeters)
                        for rt in retweeters:
                            rt["paper_id"] = pid
                            rt["文献标题"] = title[:80]
                            rt["账号类型"] = classify(rt.get("name"), rt.get("description"))
                        if rt_writer is None:
                            rt_fields = ["paper_id", "文献标题", "source_tweet_id",
                                         "screen_name", "name", "description",
                                         "followers_count", "verified", "user_id", "账号类型"]
                            rt_writer = csv.DictWriter(rt_fout, fieldnames=rt_fields,
                                                       extrasaction="ignore")
                            if not rt_exists:
                                rt_writer.writeheader()
                        rt_writer.writerows(retweeters)
                        rt_fout.flush()
                        _log(f"         转发者: {len(retweeters)} 人"
                             + (" (部分限流)" if rt_limited else ""))
                except Exception as e:
                    _log(f"         转发者抓取失败: {str(e)[:80]}")

                # 回复+引用
                try:
                    replies, quotes, rp_limited = _fetch_tweet_detail_direct(tweet_ids_for_prop)
                    rp_all = []
                    for r in replies:
                        r["paper_id"] = pid
                        r["文献标题"] = title[:80]
                        r["类型"] = "回复"
                        r["账号类型"] = classify(r.get("name"), "")
                        rp_all.append(r)
                    for q in quotes:
                        q["paper_id"] = pid
                        q["文献标题"] = title[:80]
                        q["类型"] = "引用"
                        q["账号类型"] = classify(q.get("name"), "")
                        rp_all.append(q)
                    if rp_all:
                        n_rp_total += len(rp_all)
                        if rp_writer is None:
                            rp_fields = ["paper_id", "文献标题", "source_tweet_id",
                                         "类型", "tweet_id", "text", "screen_name",
                                         "name", "followers_count", "verified",
                                         "likes", "retweets", "comments",
                                         "timestamp", "账号类型"]
                            rp_writer = csv.DictWriter(rp_fout, fieldnames=rp_fields,
                                                       extrasaction="ignore")
                            if not rp_exists:
                                rp_writer.writeheader()
                        rp_writer.writerows(rp_all)
                        rp_fout.flush()
                        _log(f"         回复引用: {len(replies)}回复/{len(quotes)}引用"
                             + (" (部分限流)" if rp_limited else ""))
                except Exception as e:
                    _log(f"         回复引用抓取失败: {str(e)[:80]}")
        else:
            _log(f"  [{pid}] 无传播 | {title[:45]}")

        with open(DONE, "a") as f: f.write(pid + "\n")
        n_done += 1
        if n_done % 50 == 0:
            hit_rate = n_hit / n_done * 100 if n_done else 0
            _log(f"  ── 进度 [{n_done}/{total_papers}] "
                 f"命中 {n_hit} 篇 ({hit_rate:.1f}%) "
                 f"转发者 {n_rt_total} / 回复引用 {n_rp_total} "
                 f"剩余 {total_papers - n_done} ──")
        time.sleep(SLEEP)

    fout.close()
    prop_fout.close()
    rt_fout.close()
    rp_fout.close()
    _log(f"\n完成: 处理 {n_done} 篇, 命中 {n_hit} 篇 -> {OUT_CSV}")
    _log(f"传播分析: {n_hit} 篇 -> {OUT_PROPAGATION}")
    _log(f"转发者: {n_rt_total} 人 -> {OUT_RETWEETERS}")
    _log(f"回复引用: {n_rp_total} 条 -> {OUT_REPLIES}")

    # 生成多 sheet Excel
    try:
        generate_excel()
    except Exception as e:
        _log(f"Excel 生成失败: {e}")

if __name__ == "__main__":
    main()
