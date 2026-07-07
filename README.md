# 文献推特传播分析

**目标**：从 WoS AI/ML 语料库（~125K 篇论文）中，找出哪些在 Twitter/X 上有传播，并分析传播特征。

## 路线图

```
Phase 1 (已完成) ─── TwexAPI 免费额度 → 搜了 2,457 篇高被引，命中 732 篇 (29.8%)
Phase 2 (进行中) ─── Scweet (自有 X 账号) → 已搜 82 篇
Phase 3 (当前)   ─── 付费 API ($20)      → 目标：搜完全部高被引 + 抽样低被引
```

## 目录说明

### 核心数据
| 文件 | 说明 |
|------|------|
| `wos_ai_core_dedup_with_paper_id.parquet` | 源数据：124,975 篇 AI/ML 论文 |
| `命中数据_合并.csv` / `.xlsx` | Phase 1 结果：732 篇有传播论文，3,023 条推文 |
| `scweet_hits.csv` | Phase 2 部分结果：当前 Scweet 命中的推文 |
| `api_hits.csv` | Phase 3 结果（付费 API 搜索的输出） |

### 脚本
| 脚本 | 用途 |
|------|------|
| `api_search_papers.py` | **主要**：付费 API 批量搜索论文标题，按被引降序优先 |
| `scweet_search_papers.py` | 备用：用自有 X 账号免费搜索（留作备选） |
| `merge_hits.py` | 合并所有命中 CSV，去重，产出最终报告 |

### 进度追踪
| 文件 | 说明 |
|------|------|
| `scweet_done.txt` | Scweet 已搜索的 paper_id |
| `api_done.txt` | 付费 API 已搜索的 paper_id（自动生成） |

### 配置
| 文件 | 说明 |
|------|------|
| `accounts.json` | X 账号 auth_token（供 Scweet 使用） |
| `venv/` | Python 虚拟环境 |

### 归档 (`_archive/`)
旧的实验脚本和数据，仅供参考。

## 策略

按被引数分级处理：

| 优先级 | 被引数 | 篇数 | 策略 |
|--------|--------|------|------|
| P0 | ≥10 | 57,945 | **全部搜索** — 高被引，全量查询 |
| P1 | 5-9 | 21,072 | 剩余额度抽样 |
| P2 | 1-4 | 31,968 | 剩余额度抽样 |
| P3 | 0 | 13,237 | 随缘 |

## 用法

### 1. 买 API 并填入 Key

编辑 `api_search_papers.py` 顶部 `CONFIG`，填入你的 API Key 和接口地址：

```python
CONFIG = {
    "api_key": "你的API_KEY",
    "base_url": "https://api.xxx.com",
    "search_endpoint": "/twitter/tweet/advanced_search",
    ...
}
```

也可以在环境变量中设置：
```bash
export TWITTER_API_KEY="你的API_KEY"
export TWITTER_API_URL="https://api.xxx.com"
```

### 2. 运行搜索

```bash
cd 文献推特传播
venv/bin/python api_search_papers.py
```

脚本会自动：
- 按被引降序排列论文
- 跳过已搜过的（来自 `scweet_done.txt` + 历史结果）
- 以 5 QPS 速率搜索
- 结果写入 `api_hits.csv`，进度写入 `api_done.txt`
- 每 1000 篇汇报一次进度和额度消耗

### 3. 合并结果

搜索结束后（或随时）：
```bash
venv/bin/python merge_hits.py
```
生成完整的 `命中数据_合并.csv` / `.xlsx`。

## 预算估算

| 方案 | 价格 | Credits | 可搜篇数（含命中） | 覆盖 |
|------|------|---------|-------------------|------|
| 一次性 $20 | **$20** | 200万 | ~95,000 篇 | 搜完 ≥10 被引 + 部分低的 |
| 月付 $50 | $50 | 500万 | 全部 125K 篇 | 一网打尽 |

建议：**先 $20 跑一次**，搜完 58K 高被引 + 37K 低被引。不够再补。

## 已有成果

- Phase 1：2,457 篇高被引论文 → **732 篇有传播 (29.8%)** → 3,023 条推文
- Phase 2 进行中：Scweet 已搜 82 篇
- 待搜索：~124,222 篇（主要靠 Phase 3 付费 API）
