#!/usr/bin/env python3
"""合并所有命中CSV -> 去重 -> 出成品 Excel + CSV。
按 tweet_id 去重(同一推文可能被多版本爬到),保留字段最全的那条。"""
import glob, pandas as pd

TWEX_FILES = [
    "twex_hits.csv", "twex_hits_0.csv", "twex_hits_1.csv", "twex_hits_2.csv", "twex_hits_3.csv", "twex_hits_proxy.csv",
    "_archive/old_scripts/twex_hits.csv", "_archive/old_scripts/twex_hits_0.csv", "_archive/old_scripts/twex_hits_1.csv",
    "_archive/old_scripts/twex_hits_2.csv", "_archive/old_scripts/twex_hits_3.csv", "_archive/old_scripts/twex_hits_proxy.csv"
]
frames = []
for f in TWEX_FILES:
    try:
        d = pd.read_csv(f)
        d["来源"] = "TwexAPI"
        frames.append(d)
        print("成功加载", f)
    except Exception as e:
        pass

# Scweet(字段较少,补齐)
try:
    sc = pd.read_csv("scweet_hits.csv")
    sc["来源"] = "Scweet"
    frames.append(sc)
except Exception as e:
    print("跳过 scweet", e)

df = pd.concat(frames, ignore_index=True)
print("合并原始行数:", len(df))

df["tweet_id"] = df["tweet_id"].astype(str)
# 每个tweet_id保留非空字段最多的一条
df["_完整度"] = df.notna().sum(axis=1)
df = df.sort_values("_完整度", ascending=False).drop_duplicates("tweet_id").drop(columns="_完整度")

# 列顺序
cols = ["paper_id","文献标题","被引数","tweet_id","推文内容","点赞","转发","评论","引用",
        "浏览","发推者","昵称","粉丝数","认证","账号类型","时间","链接","来源"]
df = df[[c for c in cols if c in df.columns]]
df["被引数"] = pd.to_numeric(df["被引数"], errors="coerce")
df = df.sort_values(["被引数","paper_id"], ascending=[False, True])

n_papers = df["paper_id"].nunique()
n_tweets = len(df)
print(f"去重后: {n_papers} 篇有传播的文献, {n_tweets} 条推文")

df.to_csv("命中数据_合并.csv", index=False, encoding="utf-8-sig")

# 文献级汇总
g = df.groupby(["paper_id","文献标题","被引数"], dropna=False).agg(
    推文数=("tweet_id","count"),
    总点赞=("点赞","sum"), 总转发=("转发","sum"), 总评论=("评论","sum"),
).reset_index().sort_values("被引数", ascending=False)

with pd.ExcelWriter("命中数据_合并.xlsx", engine="openpyxl") as w:
    pd.DataFrame({"说明":[
        f"WoS AI/ML 语料库 Twitter 传播搜索结果(免费额度阶段)",
        f"有Twitter传播的文献: {n_papers} 篇",
        f"命中推文总数: {n_tweets} 条",
        f"已搜索文献(去重): 2457 篇(按被引降序优先)",
        f"命中率: 约 {n_papers/2457*100:.1f}%",
        "—— 字段说明 ——",
        "被引数=WoS被引; 点赞/转发/评论/引用/浏览=推文互动; 发推者=博主A; 粉丝数/账号类型=博主画像",
        "账号类型: 机构/学者/媒体/个人/机器人(按bio关键词判定)",
        "注: 转发者/回复/引用的二级传播链为后续付费阶段",
    ]}).to_excel(w, sheet_name="0_说明", index=False)
    g.to_excel(w, sheet_name="1_文献汇总", index=False)
    df.to_excel(w, sheet_name="2_推文明细", index=False)
    if "账号类型" in df.columns:
        df["账号类型"].value_counts().rename_axis("账号类型").reset_index(name="推文数").to_excel(w, sheet_name="3_账号类型统计", index=False)

print("输出: 命中数据_合并.csv / 命中数据_合并.xlsx")
