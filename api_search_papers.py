#!/usr/bin/env python3
"""
TwitterAPI.io 批量搜索 — 按被引降序爬论文推文

并行版: 10 并发, 实测 ~3 篇/秒 (API 响应 ~3s, 10 并发 ≈ 3.3 QPS, 远低于 20 QPS 上限)

用法:
    python3 api_search_papers.py [篇数]
      默认爬 200 篇

断点续爬: api_done.txt (已搜 paper_id, 每篇写完即刻落盘)
实时保存: api_hits.csv (每命中一条即刻写入)
"""
import os, re, time, csv, sys, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd

API_KEY = "new1_c0a0b032afec48949d0c5e68c8672e8f"
BASE = "https://api.twitterapi.io"
HEADERS = {"x-api-key": API_KEY}
PARQUET = "wos_ai_core_dedup_with_paper_id.parquet"
OUT_HITS = "api_hits.csv"
OUT_DONE = "api_done.txt"
# 参数解析: python3 api_search_papers.py [篇数] [--daemon] [--papers-file 文件路径]
DAEMON = "--daemon" in sys.argv
_num_args = [a for a in sys.argv[1:] if a.isdigit()]
BATCH_SIZE = int(_num_args[0]) if _num_args else (5000 if DAEMON else 200)
try:
    PAPERS_FILE = sys.argv[sys.argv.index("--papers-file") + 1]
except (ValueError, IndexError):
    PAPERS_FILE = os.environ.get("PAPERS_FILE", "")
MAX_WORKERS = 10
MIN_CITED = int(os.environ.get("MIN_CITED", "0"))   # 最低被引过滤, e.g. MIN_CITED=10
CITED_SAMPLE = int(os.environ.get("CITED_SAMPLE", "0"))  # 从过滤后随机抽样, e.g. CITED_SAMPLE=10000
DAEMON_SLEEP = int(os.environ.get("DAEMON_SLEEP", "30"))  # 无剩余论文时等待秒数

FIELD_NAMES = [
    "paper_id", "文献标题", "被引数", "tweet_id", "推文内容",
    "点赞", "转发", "评论", "引用", "浏览",
    "发推者", "昵称", "粉丝数", "认证", "账号类型", "简介",
    "时间", "链接", "来源",
]
HEADER_LINE = ",".join(FIELD_NAMES)

_csv_lock = threading.Lock()


def log(msg):
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


def relevant(title, text):
    words = {x.lower() for x in re.findall(r"[A-Za-z]{4,}", title)}
    if not words:
        return True
    t = (text or "").lower()
    return sum(1 for w in words if w in t) / len(words) >= 0.5


def search(title):
    query = '"{title}"'.format(title=title)
    for attempt in range(1, 6):
        try:
            r = requests.get(
                f"{BASE}/twitter/tweet/advanced_search",
                headers=HEADERS,
                params={"query": query, "queryType": "Top"},
                timeout=30,
            )
            if r.status_code == 402:
                return None
            if r.status_code == 429:
                wait = min(20 * attempt, 120)
                log(f"限流 (429)，等待 {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log(f"HTTP {r.status_code} (重试 {attempt}/5): {r.text[:80]}")
                time.sleep(5)
                continue
            data = r.json()
            return data.get("tweets", [])
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                requests.exceptions.SSLError) as e:
            wait = min(5 * 2 ** attempt, 60)
            log(f"网络错误 ({type(e).__name__})，{wait}s 后重试 ({attempt}/5)")
            time.sleep(wait)
            continue
        except Exception as e:
            log(f"异常: {e} (重试 {attempt}/5)")
            time.sleep(5)
            continue
    return []


def process_paper(pid, title, cited):
    """处理单篇论文: 搜索 → 过滤 → 返回 (rows列表, 是否余额不足)。"""
    if len(title.split()) < 4:
        return [], False
    tweets = search(title)
    if tweets is None:
        return None, True  # 余额不足
    rows = []
    for t in tweets:
        author = t.get("author", {})
        text = (t.get("text") or "").replace("\n", " ")
        if not relevant(title, text):
            continue
        bio = (author.get("description") or "")[:200]
        rows.append({
            "paper_id": pid,
            "文献标题": title[:80],
            "被引数": cited,
            "tweet_id": t["id"],
            "推文内容": text[:2000],
            "点赞": t.get("likeCount", 0),
            "转发": t.get("retweetCount", 0),
            "评论": t.get("replyCount", 0),
            "引用": t.get("quoteCount", 0),
            "浏览": t.get("viewCount", 0),
            "发推者": author.get("userName", ""),
            "昵称": author.get("name", ""),
            "粉丝数": author.get("followers", 0),
            "认证": author.get("isBlueVerified", False),
            "账号类型": classify(author.get("name", ""), bio),
            "简介": bio,
            "时间": t.get("createdAt", ""),
            "链接": t.get("url", ""),
            "来源": "TwitterAPI",
        })
    return rows, False


def init_csv():
    """确保 CSV 文件有正确表头（兼容旧版无表头文件）。"""
    if not os.path.exists(OUT_HITS):
        with open(OUT_HITS, "w", newline="", encoding="utf-8-sig") as f:
            f.write(HEADER_LINE + "\n")
        return
    with open(OUT_HITS, newline="", encoding="utf-8-sig") as f:
        first_line = f.readline().strip()
    if "paper_id" not in first_line:
        old_len = os.path.getsize(OUT_HITS)
        with open(OUT_HITS, newline="", encoding="utf-8-sig") as f:
            old = f.read()
        with open(OUT_HITS, "w", newline="", encoding="utf-8-sig") as f:
            f.write(HEADER_LINE + "\n")
            f.write(old)
        log(f"已修复 CSV 表头 (+{old_len} 字节旧数据)")


def load_done(path):
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(f.read().split())


def process_batch(remaining):
    """处理一批论文，返回 (已搜篇数, 是否因余额用尽退出)。"""
    take = min(BATCH_SIZE, len(remaining))

    if CITED_SAMPLE > 0:
        if len(remaining) > take:
            remaining = remaining.sample(n=take, random_state=42)
        log(f"本次抽样: {take} 篇（{MAX_WORKERS} 并发, 随机抽样）")
    else:
        log(f"本次爬取: {take} 篇（{MAX_WORKERS} 并发, 按被引降序）")
    log("=" * 50)

    n_hit = n_search = 0
    total_tweets = 0
    t_start = time.time()

    papers_iter = list(remaining.head(take).iterrows())
    futures = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for _, p in papers_iter:
            pid = p["paper_id"]
            title = str(p["TI"]).strip()
            cited = p["_times_cited_num"]
            future = executor.submit(process_paper, pid, title, cited)
            futures[future] = (pid, title, cited)

        for future in as_completed(futures):
            pid, title, cited = futures[future]
            try:
                result, exhausted = future.result()
            except Exception as e:
                log(f"  [{pid}] 异常: {e}")
                with open(OUT_DONE, "a") as f:
                    f.write(pid + "\n")
                n_search += 1
                continue

            if result is None or exhausted:
                log(f"余额用尽 (pid={pid})，停止")
                return n_search, True

            rows = result
            if rows:
                n_hit += 1
                total_tweets += len(rows)
                with _csv_lock:
                    with open(OUT_HITS, "a", newline="", encoding="utf-8-sig") as f:
                        writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
                        writer.writerows(rows)
                log(f"  命中 {len(rows)} 条 | 被引 {cited} | {title[:50]}...")

            with open(OUT_DONE, "a") as f:
                f.write(pid + "\n")
            n_search += 1

            if n_search % 50 == 0:
                elapsed = time.time() - t_start
                rate = n_search / elapsed if elapsed > 0 else 0
                eta_s = (take - n_search) / rate if rate > 0 else 0
                log(f"[{n_search}/{take}] 命中 {n_hit} ({total_tweets} 条) | "
                    f"{rate:.1f}篇/秒 | ETA {eta_s:.0f}秒")

    elapsed = time.time() - t_start
    log(f"  批次完成: {n_search} 篇, 命中 {n_hit} 篇, "
        f"{n_search/elapsed:.1f}篇/秒 ({elapsed:.0f}秒)")
    return n_search, False


def load_corpus_and_done():
    """加载语料库和去重进度，返回 (排序后的 df, 已搜 paper_id 集合)。"""
    df = pd.read_parquet(PARQUET)
    df = df.sort_values("_times_cited_num", ascending=False)

    # 如果指定了采样文件，只保留文件中的论文
    if PAPERS_FILE:
        sample_ids = set(open(PAPERS_FILE).read().split())
        df = df[df["paper_id"].isin(sample_ids)]
        log(f"  采样文件: {PAPERS_FILE} ({len(sample_ids)} 篇, 匹配 {len(df)} 篇)")

    done = load_done(OUT_DONE)
    for src in ["命中数据_合并.csv", "scweet_hits.csv"]:
        if os.path.exists(src):
            d = pd.read_csv(src)
            ids = set(d["paper_id"].dropna().unique())
            done.update(ids)
    done.update(load_done("scweet_done.txt"))
    return df, done


def get_remaining(df, done):
    """返回过滤后的剩余论文 DataFrame。"""
    remaining = df[~df["paper_id"].isin(done)]

    if MIN_CITED > 0:
        remaining = remaining[remaining["_times_cited_num"] >= MIN_CITED]
    elif MIN_CITED == 0 and CITED_SAMPLE > 0:
        remaining = remaining[remaining["_times_cited_num"] == 0]

    return remaining


def main():
    log("加载语料库...")
    df, done = load_corpus_and_done()
    remaining = get_remaining(df, done)
    log(f"  总论文: {len(df)}, 已搜: {len(done)}, 剩余: {len(remaining)}")

    init_csv()
    process_batch(remaining)

    try:
        bal = requests.get(f"{BASE}/oapi/my/info", headers=HEADERS).json()
        log(f"  余额: 充值 {bal['recharge_credits']:,} + 赠送 {bal['total_bonus_credits']:,}")
    except Exception:
        pass


def main_daemon():
    """持续守护模式: 一批接一批，直到全部搜完。"""
    log("=== 持续搜索守护启动 ===")
    exhausted = False
    while not exhausted:
        log("加载语料库...")
        df, done = load_corpus_and_done()
        remaining = get_remaining(df, done)

        if remaining.empty:
            log(f"全部完成! ({len(done)} 篇) 等待 {DAEMON_SLEEP} 秒后重新检查...")
            time.sleep(DAEMON_SLEEP)
            continue

        log(f"  总论文: {len(df)}, 已搜: {len(done)}, 剩余: {len(remaining)}")
        init_csv()
        n_searched, exhausted = process_batch(remaining)

        try:
            bal = requests.get(f"{BASE}/oapi/my/info", headers=HEADERS).json()
            log(f"  余额: 充值 {bal['recharge_credits']:,} + 赠送 {bal['total_bonus_credits']:,}")
        except Exception:
            pass

    log("=== 搜索守护退出（余额用尽）===")


if __name__ == "__main__":
    if DAEMON:
        main_daemon()
    else:
        main()
