#!/usr/bin/env python3
"""
TwitterAPI.io 批量搜索 — 按被引降序爬论文推文

用法: python3 test_crawl.py [篇数]
  默认爬200篇，可指定: python3 test_crawl.py 500

断点续爬: 已爬的 paper_id 记录在 api_done.txt，自动跳过
实时保存: 每搜到一条命中立刻写 api_hits.csv
"""
import os, re, time, csv, sys
import requests
import pandas as pd

# ── 配置 ──
API_KEY = "new1_c0a0b032afec48949d0c5e68c8672e8f"
BASE = "https://api.twitterapi.io"
HEADERS = {"x-api-key": API_KEY}
PARQUET = "wos_ai_core_dedup_with_paper_id.parquet"
OUT_HITS = "api_hits.csv"
OUT_DONE = "api_done.txt"

# 从命令行参数获取本次要爬的篇数，默认200
BATCH_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 200

QUERY_TPL = '"{title}"'

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def relevant(title, text):
    words = {x.lower() for x in re.findall(r"[A-Za-z]{4,}", title)}
    if not words:
        return True
    t = (text or "").lower()
    return sum(1 for w in words if w in t) / len(words) >= 0.5


def search(title, retries=3):
    """搜索一篇论文，返回命中推文列表，余额不足返回 None"""
    query = QUERY_TPL.format(title=title)
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                f"{BASE}/twitter/tweet/advanced_search",
                headers=HEADERS,
                params={"query": query, "queryType": "Top"},
                timeout=30,
            )
            if r.status_code == 402:
                log(f"❌ 余额不足 (402)，停止")
                return None
            if r.status_code == 429:
                wait = min(10 * attempt, 60)
                log(f"⏳ 限流 (429)，等待 {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log(f"⚠️ HTTP {r.status_code} (重试 {attempt}/{retries}): {r.text[:80]}")
                time.sleep(3)
                continue
            data = r.json()
            return data.get("tweets", [])
        except requests.exceptions.Timeout:
            log(f"⏰ 超时 (重试 {attempt}/{retries})")
            continue
        except Exception as e:
            log(f"⚠️ 异常: {e} (重试 {attempt}/{retries})")
            time.sleep(3)
            continue
    log(f"✋ 重试耗尽，跳过")
    return []


def load_done(path):
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(f.read().split())


def main():
    log(f"📚 加载语料库...")
    df = pd.read_parquet(PARQUET)
    df = df.sort_values("_times_cited_num", ascending=False)
    total_all = len(df)

    # ── 收集已搜索过的 paper_id ──
    log(f"🔍 检查已有进度...")
    done = load_done(OUT_DONE)
    log(f"  api_done.txt: {len(done)} 篇")

    for src in ["命中数据_合并.csv", "scweet_hits.csv"]:
        if os.path.exists(src):
            d = pd.read_csv(src)
            ids = set(d["paper_id"].dropna().unique())
            done.update(ids)
            log(f"  {src}: {len(ids)} 篇")

    scweet_done = load_done("scweet_done.txt")
    done.update(scweet_done)
    log(f"  scweet_done.txt: {len(scweet_done)} 篇")
    log(f"  ── 合计已搜索: {len(done)} 篇")

    remaining = df[~df["paper_id"].isin(done)]
    log(f"  ── 剩余待搜索: {len(remaining)} 篇")
    log(f"")

    take = min(BATCH_SIZE, len(remaining))
    log(f"🚀 本次计划爬取: {take} 篇（按被引降序）")
    log(f"=" * 50)

    # ── 准备输出文件 ──
    fout = open(OUT_HITS, "a", newline="", encoding="utf-8-sig")
    writer = None
    csv_exists = os.path.exists(OUT_HITS)

    n_search = n_hit = n_skip = n_err = 0
    total_tweets = 0
    t_start = time.time()
    last_log = time.time()

    for idx, (_, p) in enumerate(remaining.head(take).iterrows(), 1):
        pid = p["paper_id"]
        title = str(p["TI"]).strip()

        # 标题太短（<4词）跳过
        words = title.split()
        if len(words) < 4:
            n_skip += 1
            with open(OUT_DONE, "a") as f:
                f.write(pid + "\n")
            continue

        # ── 搜索 ──
        tweets = search(title)

        # 余额不足跳出
        if tweets is None:
            log(f"⚠️ 余额用尽，已搜 {n_search} 篇")
            break

        # ── 命中处理 ──
        rows = []
        for t in tweets:
            author = t.get("author", {})
            text = (t.get("text") or "").replace("\n", " ")
            if not relevant(title, text):
                continue
            rows.append({
                "paper_id": pid,
                "文献标题": title[:80],
                "被引数": p["_times_cited_num"],
                "tweet_id": t["id"],
                "推文内容": text,
                "点赞": t.get("likeCount", 0),
                "转发": t.get("retweetCount", 0),
                "评论": t.get("replyCount", 0),
                "引用": t.get("quoteCount", 0),
                "浏览": t.get("viewCount", 0),
                "发推者": author.get("userName", ""),
                "昵称": author.get("name", ""),
                "粉丝数": author.get("followers", 0),
                "认证": author.get("isBlueVerified", False),
                "账号类型": "",
                "时间": t.get("createdAt", ""),
                "链接": t.get("url", ""),
            })

        # 实时存命中
        if rows:
            n_hit += 1
            total_tweets += len(rows)
            if writer is None:
                writer = csv.DictWriter(fout, fieldnames=list(rows[0].keys()))
                if not csv_exists:
                    writer.writeheader()
            writer.writerows(rows)
            fout.flush()
            log(f"  ✅ #{idx} 命中 {len(rows)} 条 | 被引 {p['_times_cited_num']} | {title[:50]}...")
        elif len(tweets) > 0:
            # 有推文但不相关
            n_skip += 1
        else:
            n_skip += 1

        # 实时存已爬标记
        with open(OUT_DONE, "a") as f:
            f.write(pid + "\n")
        n_search += 1

        # 限速
        time.sleep(0.25)

        # 每30秒打印进度摘要
        now = time.time()
        if now - last_log >= 30:
            elap = now - t_start
            rate = n_search / elap if elap > 0 else 0
            eta_s = (take - n_search) / rate if rate > 0 else 0
            log(f"📊 [{n_search}/{take}] 命中 {n_hit} ({total_tweets} 条) | "
                f"{rate:.1f}篇/秒 | ETA {eta_s/60:.0f}分")
            last_log = now

    # ── 收尾 ──
    fout.close()
    elapsed = time.time() - t_start
    log(f"=" * 50)
    log(f"🏁 完成!")
    log(f"   搜索: {n_search} 篇")
    log(f"   命中: {n_hit} 篇 ({total_tweets} 条推文)")
    log(f"   跳过: {n_skip} 篇")
    log(f"   耗时: {elapsed:.0f} 秒 ({elapsed/60:.1f} 分)")
    log(f"   速率: {n_search/elapsed:.1f} 篇/秒" if elapsed > 0 else "")
    log(f"   输出: {OUT_HITS}")
    
    # 查余额
    try:
        bal = requests.get(f"{BASE}/oapi/my/info", headers=HEADERS).json()
        log(f"   余额: 充值 {bal['recharge_credits']:,} + 赠送 {bal['total_bonus_credits']:,} 积分")
    except:
        pass


if __name__ == "__main__":
    main()
