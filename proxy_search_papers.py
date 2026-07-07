#!/usr/bin/env python3
"""
通过 Cloudflare Worker 代理池批量搜索 X/Twitter 文献标题。
每个 X 账号固定走一个 Worker（不同出站 IP），避免同 IP 批量操作被风控。

用法:
    export LIMIT=20  # 0=全量
    python3 proxy_search_papers.py
"""
import json, os, re, sys, time, csv, secrets
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
import requests as req

SKILL_DIR = os.path.expanduser("~/.codex/skills/cf-worker-proxy-pool")
sys.path.insert(0, f"{SKILL_DIR}/scripts")
sys.path.insert(0, SKILL_DIR)
from proxy_client import ProxyClient

ACCOUNTS = "accounts.json"
PARQUET = "wos_ai_core_dedup_with_paper_id.parquet"
OUT_CSV = "scweet_hits.csv"
DONE = "scweet_done.txt"

POOL_MAP = f"{SKILL_DIR}/proxy_pool_map.json"
WORKERS_FILE = f"{SKILL_DIR}/proxy_workers.json"
TOKEN_FILE = f"{SKILL_DIR}/proxy_token.txt"

LIMIT = int(os.environ.get("LIMIT", "20"))
SINCE = "2015-01-01"
PER_Q = 20
SLEEP = 2.0

BEARER = ("AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
          "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA")

FEATURES = {
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

ct0_cache: dict[str, str] = {}

def load_accounts():
    with open(ACCOUNTS) as f:
        records = json.load(f)
    out = []
    for r in records:
        cookies = r.get("cookies") or {}
        token = cookies.get("auth_token") or r.get("auth_token")
        name = r.get("username") or r.get("name") or "unknown"
        if token:
            out.append({"name": name, "auth_token": token})
    return out

def get_unique_workers(pool_map_path, n):
    with open(pool_map_path) as f:
        pm = json.load(f)
    ip_to_worker = {}
    for w, info in pm.items():
        ip = info.get("ip")
        if ip and info.get("ok"):
            if ip not in ip_to_worker:
                ip_to_worker[ip] = w
    ips = list(ip_to_worker.keys())
    return [ip_to_worker[ips[i % len(ips)]] for i in range(n)]

def bootstrap_ct0(auth_token):
    if auth_token in ct0_cache:
        return ct0_cache[auth_token]
    s = req.Session()
    s.cookies.set("auth_token", auth_token, domain=".x.com")
    s.headers["User-Agent"] = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/125.0.0.0 Safari/537.36")
    try:
        s.get("https://x.com/home", timeout=15, allow_redirects=True)
    except Exception:
        pass
    ct0 = ""
    for c in s.cookies:
        if c.name == "ct0" and c.domain and "x.com" in c.domain:
            ct0 = c.value
            break
    ct0_cache[auth_token] = ct0
    return ct0

def extract_tweets(data):
    tweets = []
    instructions = (data.get("data", {}).get("search_by_raw_query", {})
                    .get("search_timeline", {}).get("timeline", {})
                    .get("instructions", []))
    for instr in instructions:
        entries = instr.get("entries", []) if isinstance(instr, dict) else []
        for ent in entries:
            eid = str(ent.get("entryId") or "")
            if not eid.startswith("tweet-"):
                continue
            content = ent.get("content", {}) or {}
            ic = content.get("itemContent", {}) or {}
            result = (ic.get("tweet_results", {}) or {}).get("result", {}) or {}
            if "tweet" in result and isinstance(result.get("tweet"), dict):
                result = result["tweet"]
            legacy = result.get("legacy", {}) or {}
            user_res = (result.get("core", {}) or {}).get("user_results", {}) or {}
            user_legacy = (user_res.get("result", {}) or {}).get("legacy", {}) or {}
            screen_name = user_legacy.get("screen_name") or ""
            user_name = user_legacy.get("name") or ""
            tweet_id = legacy.get("id_str") or result.get("rest_id") or eid.replace("tweet-", "")
            note_text = ((result.get("note_tweet", {}) or {})
                         .get("note_tweet_results", {}) or {}
                         .get("result", {}) or {}).get("text")
            text = note_text or legacy.get("full_text") or ""
            ts = legacy.get("created_at") or ""
            tweets.append({
                "tweet_id": tweet_id,
                "text": text,
                "screen_name": screen_name,
                "name": user_name,
                "timestamp": ts,
                "likes": legacy.get("favorite_count", 0),
                "retweets": legacy.get("retweet_count", 0),
                "comments": legacy.get("reply_count", 0),
                "tweet_url": f"https://x.com/{screen_name}/status/{tweet_id}" if screen_name and tweet_id else "",
            })
    return tweets

def search_title(px, auth_token, ct0, title, since, limit=20):
    query = f'"{title}" since:{since}'
    variables = {
        "rawQuery": query,
        "count": limit,
        "querySource": "typed_query",
        "product": "Top",
        "withGrokTranslatedBio": False,
    }
    params = {
        "variables": json.dumps(variables, separators=(",", ":")),
        "features": json.dumps(FEATURES, separators=(",", ":")),
    }
    url = ("https://x.com/i/api/graphql/rkp6b4vtR9u7v3naGoOzUQ/SearchTimeline?"
           + urlencode(params))

    if not ct0:
        ct0 = "x" * 32
    headers = {
        "Authorization": f"Bearer {BEARER}",
        "X-Csrf-Token": ct0,
        "X-Twitter-Auth-Type": "OAuth2Session",
        "X-Twitter-Active-User": "yes",
        "X-Twitter-Client-Language": "en",
        "Referer": "https://x.com/",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/125.0.0.0 Safari/537.36"),
        "Cookie": f"auth_token={auth_token}; ct0={ct0}",
    }
    resp = px.fetch(url, headers=headers, tries=3)
    if resp is None:
        return None, "proxy_fail"
    if resp.status_code != 200:
        return None, f"http_{resp.status_code}"
    try:
        data = resp.json()
    except Exception as e:
        return None, f"json_decode_{e}"
    if "errors" in data and isinstance(data["errors"], list):
        return None, f"graphql_{data['errors']}"
    tweets = extract_tweets(data)
    return tweets, "ok"

def relevant(title, text):
    w = {x.lower() for x in re.findall(r"[A-Za-z]{4,}", title)}
    if not w:
        return True
    t = (text or "").lower()
    return sum(1 for x in w if x in t) / len(w) >= 0.5

def load_done():
    return set(open(DONE).read().split()) if os.path.exists(DONE) else set()

def main():
    accounts = load_accounts()
    if not accounts:
        raise SystemExit("没有带 auth_token 的账号")
    print(f"[info] 加载 {len(accounts)} 个账号")

    worker_urls = get_unique_workers(POOL_MAP, len(accounts))
    for i, acc in enumerate(accounts):
        print(f"  {acc['name']} -> {worker_urls[i].split('.')[0]}")
    account_proxies = []
    for i, acc in enumerate(accounts):
        t = bootstrap_ct0(acc["auth_token"])
        print(f"  bootstrap {acc['name']}: ct0={'yes' if t else 'no'}")
        with open(TOKEN_FILE) as f:
            tok = f.read().strip()
        px = ProxyClient(workers=[worker_urls[i]], token=tok, pool_map=POOL_MAP)
        account_proxies.append((acc, t, px))

    df = pd.read_parquet(PARQUET)
    df = df[df["TI"].astype(str).str.split().str.len() >= 4]
    df = df.sort_values("_times_cited_num", ascending=False)
    done = load_done()
    df = df[~df["paper_id"].isin(done)]
    if LIMIT:
        df = df.head(LIMIT)
    print(f"[info] 待搜索: {len(df)} 篇")

    csv_exists = os.path.exists(OUT_CSV)
    fout = open(OUT_CSV, "a", newline="", encoding="utf-8-sig")
    writer = None
    n_hit = n_done = 0
    aidx = 0
    rate_limit_waits = {}

    for _, p in df.iterrows():
        pid = p["paper_id"]
        if pid in done:
            continue
        title = str(p["TI"]).strip()
        acc, ct0, px = account_proxies[aidx % len(account_proxies)]
        aidx += 1

        # 每个账号每 15 分钟约 45 次 = 20 秒间隔
        now = time.time()
        last = rate_limit_waits.get(acc["name"], 0)
        wait = max(0, last + 20 - now)
        if wait > 0:
            time.sleep(wait)
        rate_limit_waits[acc["name"]] = time.time()

        tweets, status = search_title(px, acc["auth_token"], ct0, title, SINCE, PER_Q)

        if status == "proxy_fail" or (status and status.startswith("http_5")):
            print(f"  [{pid}] Worker 错误({status})，换账号重试")
            aidx += 1
            acc2, ct02, px2 = account_proxies[aidx % len(account_proxies)]
            tweets, status = search_title(px2, acc2["auth_token"], ct02, title, SINCE, PER_Q)

        print(f"  [{pid}] {status} | {title[:40]} | via {acc['name']}")

        if tweets:
            rows = []
            for tw in tweets:
                txt = tw["text"].replace("\n", " ")
                if not relevant(title, txt):
                    continue
                rows.append({
                    "paper_id": pid, "文献标题": title[:80], "被引数": p["_times_cited_num"],
                    "tweet_id": tw["tweet_id"], "推文内容": txt,
                    "点赞": tw["likes"], "转发": tw["retweets"], "评论": tw["comments"],
                    "发推者": tw["screen_name"], "昵称": tw["name"],
                    "时间": tw["timestamp"], "链接": tw["tweet_url"],
                })
            if rows:
                n_hit += 1
                if writer is None:
                    writer = csv.DictWriter(fout, fieldnames=list(rows[0].keys()))
                    if not csv_exists:
                        writer.writeheader()
                writer.writerows(rows)
                fout.flush()

        with open(DONE, "a") as f:
            f.write(pid + "\n")
        n_done += 1
        time.sleep(SLEEP)

    fout.close()
    print(f"\n完成: 处理 {n_done} 篇, 命中 {n_hit} 篇 -> {OUT_CSV}")

if __name__ == "__main__":
    main()
