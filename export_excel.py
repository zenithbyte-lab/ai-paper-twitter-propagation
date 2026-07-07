#!/usr/bin/env python3
"""导出 Excel — 匹配参考格式（6个Sheet）。"""
import os, json, pandas as pd

PARQUET = "wos_ai_core_dedup_with_paper_id.parquet"
OUT = "twitter_diffusion.xlsx"

def load_parquet_years():
    """从语料库加载 paper_id → 年份 映射。"""
    df = pd.read_parquet(PARQUET, columns=["paper_id", "PY"])
    return dict(zip(df["paper_id"], df["PY"]))

def main():
    if not os.path.exists("api_hits.csv"):
        print("缺少 api_hits.csv")
        return

    years = load_parquet_years()

    # ── 命中推文 ──
    hits = pd.read_csv("api_hits.csv")
    old = pd.read_csv("命中数据_合并.csv") if os.path.exists("命中数据_合并.csv") else pd.DataFrame()
    hits = pd.concat([hits, old], ignore_index=True).drop_duplicates(subset="tweet_id")

    # 补简介 (from author_bios.json)
    bio_map = {}
    if os.path.exists("author_bios.json"):
        bio_map = json.load(open("author_bios.json"))
    if "简介" not in hits.columns:
        hits["简介"] = ""
    hits["简介"] = hits["简介"].fillna("")
    mask = hits["简介"].str.strip() == ""
    hits.loc[mask, "简介"] = hits.loc[mask, "发推者"].map(lambda x: bio_map.get(str(x), ""))

    # 补统计 (from stats_fix.json)
    stats_fix = {}
    if os.path.exists("stats_fix.json"):
        stats_fix = json.load(open("stats_fix.json"))
    tid_to_fix = {str(tid): stats_fix.get(str(tid), {}) for tid in hits["tweet_id"]}
    for c, fix_key in [("引用", "引用"), ("浏览", "浏览"), ("链接", "链接")]:
        if c in hits.columns:
            mask = hits[c].isna() | (hits[c].astype(str).str.strip() == "")
            if mask.any():
                hits.loc[mask, c] = hits.loc[mask, "tweet_id"].astype(str).map(
                    lambda x: tid_to_fix.get(x, {}).get(fix_key, "")
                )
    hits["引用"] = pd.to_numeric(hits["引用"], errors="coerce").fillna(0).astype(int)
    hits["浏览"] = pd.to_numeric(hits["浏览"], errors="coerce").fillna(0).astype(int)
    hits["链接"] = hits["链接"].fillna("").astype(str)

    # 年份
    hits["年份"] = hits["paper_id"].map(years).fillna(0).astype(int)

    # 推文内容不截断
    hits["推文内容"] = hits["推文内容"].fillna("")

    # ── Sheet 1: 文献概览 ──
    sheet1 = hits.groupby("paper_id").agg(
        标题=("文献标题", "first"),
        年份=("年份", "first"),
        被引数=("被引数", "first"),
        直接推文数=("tweet_id", "nunique"),
    ).reset_index()
    sheet1 = sheet1[["paper_id", "标题", "年份", "被引数", "直接推文数"]]

    # ── Sheet 2: 推文明细 ──
    hits["层级"] = "L1"
    hits["角色"] = "直接发推"
    hits["发布时间"] = hits.get("时间", "")
    sheet2_cols = [
        "paper_id", "文献标题", "层级", "角色", "tweet_id", "推文内容",
        "点赞", "转发", "评论", "引用", "浏览", "发布时间",
        "发推者", "粉丝数", "账号类型", "简介", "链接",
    ]
    sheet2 = pd.DataFrame()
    for c in sheet2_cols:
        sheet2[c] = hits.get(c, "").fillna("").astype(str).replace("nan", "")

    # ── Sheet 3: 传播链 ──
    # 转发者
    rt = pd.read_csv("scweet_retweeters.csv") if os.path.exists("scweet_retweeters.csv") else pd.DataFrame()
    # 回复/引用
    rp = pd.read_csv("scweet_replies.csv") if os.path.exists("scweet_replies.csv") else pd.DataFrame()

    chain_rows = []
    # 源推文查找
    src_map = dict(zip(hits["tweet_id"].astype(str), zip(hits["发推者"], hits["推文内容"])))

    if not rt.empty:
        for _, r in rt.iterrows():
            src_id = str(r.get("source_tweet_id", ""))
            src_author, src_text = src_map.get(src_id, ("", ""))
            desc = str(r.get("description", "") or "")
            chain_rows.append({
                "源推ID": r.get("source_tweet_id", 0),
                "源作者": src_author,
                "关系": "转发",
                "层级": "L1->放大",
                "目标": str(r.get("screen_name", "") or ""),
                "目标粉丝": int(r.get("followers_count", 0) or 0),
                "目标类型": str(r.get("账号类型", "") or ""),
                "内容": desc[:200],
                "简介": desc[:200],
            })

    if not rp.empty:
        for _, r in rp.iterrows():
            src_id = str(r.get("source_tweet_id", ""))
            src_author, src_text = src_map.get(src_id, ("", ""))
            rel = str(r.get("类型", "回复") or "")
            txt = str(r.get("text", "") or "")
            chain_rows.append({
                "源推ID": r.get("source_tweet_id", 0),
                "源作者": src_author,
                "关系": rel,
                "层级": "L1->放大",
                "目标": str(r.get("screen_name", "") or ""),
                "目标粉丝": int(r.get("followers_count", 0) or 0),
                "目标类型": str(r.get("账号类型", "") or ""),
                "内容": txt[:200],
                "简介": str(r.get("description", "") or ""),
            })

    sheet3 = pd.DataFrame(chain_rows)

    # ── Sheet 4: 账号类型统计 ──
    all_authors = pd.concat([
        hits[["账号类型"]].rename(columns={"账号类型": "类型"}),
        rt[["账号类型"]].rename(columns={"账号类型": "类型"}) if not rt.empty else pd.DataFrame(),
        rp[["账号类型"]].rename(columns={"账号类型": "类型"}) if not rp.empty else pd.DataFrame(),
    ], ignore_index=True)
    sheet4 = all_authors["类型"].value_counts().reset_index()
    sheet4.columns = ["账号类型", "数量"]

    # ── Sheet 5: 传播关系统计 ──
    rel_counts = {"转发": len(rt) if not rt.empty else 0,
                  "回复": len(rp[rp.get("类型", "") == "回复"]) if not rp.empty else 0,
                  "引用": len(rp[rp.get("类型", "") == "引用"]) if not rp.empty else 0}
    sheet5 = pd.DataFrame(list(rel_counts.items()), columns=["传播关系", "数量"])

    # ── Sheet 0: 说明 ──
    total_tweets = len(hits)
    total_papers = hits["paper_id"].nunique()
    total_retweeters = len(rt) if not rt.empty else 0
    total_replies = len(rp) if not rp.empty else 0
    notes = [
        f"数据来源: Web of Science 文献库(AI/ML) × X(Twitter) 实时数据",
        f"说明: 本文件展示论文-推文多级传播关系",
        f"搜索范围: WoS AI/ML 论文 + TwitterAPI.io 搜索 + 免费账号传播分析",
        f"论文数: {total_papers}",
        f"推文数: {total_tweets}",
        f"转发者数: {total_retweeters}",
        f"回复/引用数: {total_replies}",
        "Sheet说明:",
        "  1_文献概览: 论文级聚合统计",
        "  2_推文明细: 每条推文详情",
        "  3_传播链: 转发/回复/引用的传播关系",
        "  4_账号类型统计: 参与账号的分类分布",
        "  5_传播关系统计: 转发/回复/引用数量",
        "",
    ]
    sheet0 = pd.DataFrame({"": notes})

    # ── 写入 Excel ──
    with pd.ExcelWriter(OUT, engine="openpyxl") as w:
        sheet0.to_excel(w, sheet_name="0_说明", index=False)
        sheet1.to_excel(w, sheet_name="1_文献概览", index=False)
        sheet2.to_excel(w, sheet_name="2_推文明细", index=False)
        sheet3.to_excel(w, sheet_name="3_传播链", index=False)
        sheet4.to_excel(w, sheet_name="4_账号类型统计", index=False)
        sheet5.to_excel(w, sheet_name="5_传播关系统计", index=False)

    print(f"导出完成: {OUT}")
    print(f"  Sheet0 说明")
    print(f"  Sheet1 文献概览: {len(sheet1)} 篇")
    print(f"  Sheet2 推文明细: {len(sheet2)} 条")
    print(f"  Sheet3 传播链: {len(sheet3)} 条关系")
    print(f"  Sheet4 账号类型: {len(sheet4)} 类")
    print(f"  Sheet5 传播关系: {len(sheet5)} 类")

if __name__ == "__main__":
    main()
