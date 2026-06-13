# SDTM 全域自动化平台 — 开发指南

> 基于 PRD v2.0（PRD-SDTM-2026-002）

## 项目概述

本项目是一套覆盖 CDISC SDTM 全域的自动化转换系统，支持任意格式 EDC 原始数据输入，内置 LLM 原生调用接口（MCP 协议），满足 21 CFR Part 11 合规要求。

**三层架构：** Vue 3 前端 → FastAPI 网关 → R MCP Server

---

## 目录结构

```
sdtm-platform/
├── frontend/                  # Vue 3 + Vite + Pinia + ECharts
│   └── src/
│       ├── router/            # Vue Router 4
│       ├── stores/            # session / dag / mappings / audit
│       ├── views/             # 页面视图
│       ├── components/        # DagGraph / MappingTable / AuditTimeline / QualityDashboard
│       └── api/               # Axios + TanStack Query
├── gateway/                   # FastAPI + Uvicorn (Python 3.11+)
│   ├── main.py
│   ├── routers/               # dag / upload / detect / mappings / build / audit
│   ├── mcp/                   # 异步 MCP SSE 客户端 + R 进程池
│   ├── middleware/            # cors / audit_inject（自动审计注入）
│   ├── models/                # Pydantic v2 模型
│   └── pyproject.toml         # uv 管理
└── r-mcp-server/              # R MCP Server（SSE + stdio 双模式）
    ├── server.R
    ├── tools/                 # detect / transform / audit / registry 工具
    ├── domains/               # 每个 SDTM 域一个 R 文件
    ├── dag/dag_schema.json    # DAG 权威来源（单一真实来源）
    ├── context_store/         # Layer1 IG / Layer2 Define-XML / Layer3 对话摘要
    ├── registry/              # mapping_registry.yaml
    ├── audit/                 # audit_trail.ndjson
    └── renv.lock
```

---

## 核心技术栈

| 层 | 选型 | 版本要求 |
|---|---|---|
| 前端 | Vue 3 + Vite + Pinia + Vue Router 4 | Vue 3.4+，Vite 5+ |
| 图表 | ECharts 5（DAG）+ Vega-Lite 5（质量看板） | ECharts 5.5+ |
| HTTP 客户端 | Axios + TanStack Query | Axios 1.7+，TQ 5+ |
| API 网关 | FastAPI + Uvicorn + Pydantic v2 | FastAPI 0.115+ |
| Python 包管理 | `uv` | 最新版 |
| SDTM 转换 | `sdtm.oak` ≥ 0.2.0 | **最低版本要求** |
| 元数据 | `metacore` + `metatools` | 0.1.0+ |
| XPT 输出 | `xportr` + `haven` | xportr 0.3.0+ |
| 审计完整性 | `digest`（SHA-256）| 0.6.31+ |
| R 包管理 | `renv` | 最新版 |

---

## SDTM DAG 依赖结构

域按以下层级顺序实现，同层域可并发处理：

```
L0（无依赖）: TA · TE · TV · TI · TS · TM · TD · CO
L1（依赖L0）: DM + SUPPDM
L2（依赖DM）: EX/EC · DS · AE · CM · MH · DV · BE · CE · HO · AG · PR · SU · ML
L3（依赖L2）: VS · LB · EG · PE · QS · IE · PC · PP · TU · TR · RS · MB · MS · DA · DD · BS · GF
L4（依赖L3）: FA
L5（派生域）: SV · SE · SM
L6（关系集）: RELREC · RELSUB · RELSPEC
```

**v2.0 实现优先级（P0/P1）：**
- P0：TA/TE/TV/TI/TS → DM
- P1：EX → DS → AE → CM → MH

**关键链式依赖（不可颠倒）：**
- `DM.RFSTDTC` ← `min(EX.EXSTDTC)` per USUBJID
- `DM.RFENDTC` ← `max(DS.DSSTDTC)` per USUBJID
- `BE → PC`，`TU → TR → RS`，`MB → MS`，`AE+DS → DD`

---

## 五层自动侦测引擎

综合得分 = `0.35×A + 0.50×B + 0.15×C`，Layer D 命中时优先覆盖：

| 层 | 机制 | 权重 |
|---|---|---|
| A | 同义词字典 + Jaro-Winkler 距离 | 35% |
| B | CT 值域匹配（唯一值集合） | 50% |
| C | 日期格式识别（ISO 8601） | 15% |
| D | 多维语义匹配（五子层：D1列名精确/D2内容指纹/D3 EDC描述互比/D4 EDC→IG Notes/D5 IG Notes→EDC） | 覆盖 |
| E | LLM 辅助（Claude API，置信度<0.6时触发） | — |

**路由决策：**
- D1/D2 精确命中 → `REGISTRY`（0.99/0.88）
- ≥ 0.90 → `AUTO`（自动接受）
- 0.60–0.89 → `SUGGEST`（需用户确认）
- < 0.60 → `MANUAL` + Layer E

---

## Specs 规划层（三步预分析）

映射执行前必须完成：

1. **Step 1 Bottom-up**：上传原始数据 → Schema Profiling → Domain Allocation（数据集→SDTM域）→ 变量四路分类（Standard / Non-standard / FA / Excluded）
2. **Step 2 Top-down**：从 SDTM IG 检查 Required/Expected 变量覆盖度 → Coverage Report（Hard Flag / Soft Flag）
3. **Step 3 Gap Analysis**：综合生成变量五分类（DirectMap / Derive / Hardcode / Gap / NotCollected）→ Mapping Plan YAML

**门控规则：数据源清单确认完整前，不得开始任何变量映射。**

---

## 三层上下文管理系统（CMS）

每个（域、变量）在侦测和 Specs 生成前构建 `VariableContext`：

| 层 | 来源 | 优先级 |
|---|---|---|
| Layer 1 | SDTM IG v3.4 标准元数据（CDISC Library）| 基线 |
| Layer 2 | 历史 Define-XML 提取的申办方规格 | 覆盖 L1 |
| Layer 3 | HITL 对话摘要（LLM 生成，Claude API）| 最高优先 |

三层合并为 `VariableContext` 对象，注入：Specs 生成（预填字段）/ LLM 提示（系统上下文）/ HITL 界面（参考面板）。

---

## 映射执行：三层循环架构

| 层 | 粒度 | 并发策略 |
|---|---|---|
| Layer 1 | 全域 Orchestration（DAG 调度） | 拓扑并发（无依赖域并行，默认最大4个） |
| Layer 2 | 单域 Domain Loop | 域内变量按分类批量处理 |
| Layer 3 | 单变量 Variable Loop | React 状态机 + 审查中间件 + HITL |

**Variable React 状态：** `idle → loading_ctx → detecting → [auto/registry/awaiting_user] → ct_validating → accepted`

**审查中间件**在每次变量 `accepted` 时自动执行：写审计日志 + 更新注册表 + 更新 Specs 草稿。

---

## 21 CFR Part 11 审计追踪（强制合规）

> ⚠️ 所有 P0 审计需求为强制要求，v2.0 发布前必须全部实现。

- **存储格式**：Append-only NDJSON 日志（只追加不修改）
- **SHA-256 哈希链**：每条记录含 `prev_hash`，首条为 `"GENESIS"`
- **电子签名**（§11.50）：姓名 + UTC 时间 + 签名含义，触发创建映射快照
- **关键事件类型**：`SESSION_START` / `MAPPING_APPROVED` / `MAPPING_MODIFIED` / `DOMAIN_BUILD_COMPLETE` / `ESIGN_APPLIED` / `SNAPSHOT_CREATED`
- **AuditMiddleware**：FastAPI 中间件自动拦截所有 `/api/v2/` 请求注入审计事件

---

## REST API 基本约定

```
Base URL:  http://localhost:8000
前缀:      /api/v2/
响应结构:  { success, data, meta: { session_id, domain, timestamp_utc, api_version }, error }
```

**主要路由模块：**
- `/api/v2/dag/` — DAG 状态 + 依赖检查
- `/api/v2/upload/` — 文件上传（EDC / 外部数据 / DTS）
- `/api/v2/detect/{domain}` — 五层侦测
- `/api/v2/mappings/{domain}/` — 映射审核 CRUD
- `/api/v2/build/{domain}` — XPT 生成 + 验证
- `/api/v2/audit/` — 审计日志 + 快照 + CSV 导出
- `/api/v2/context/` — CMS 三层上下文 + Specs 管理
- `/api/v2/plan/` — Specs 规划层三步

---

## 性能目标

| 指标 | 目标 |
|---|---|
| 自动侦测（单域，≤100列）| < 5 秒 |
| XPT 生成（≤10,000 受试者）| < 30 秒 |
| API 响应 P95 | < 500 ms |
| 审计日志写入 | < 100 ms（不阻塞主流程）|
| DAG 状态查询 | < 200 ms |

---

## 新增 SDTM 域的最小变更集

1. 添加 `r-mcp-server/domains/{domain}.R`（遵循标准模板）
2. 在 `r-mcp-server/tools/transform_tools.R` 注册 MCP 工具
3. 在 `r-mcp-server/dag/dag_schema.json` 添加节点定义（含 `depends_on`）
4. 在 `r-mcp-server/tools/detect_tools.R` 添加域特有同义词字典

**无需改动**：FastAPI 网关 / Vue 3 前端 / 审计系统

---

## 本地开发启动顺序

```bash
# 1. R MCP Server（端口 8001）
cd r-mcp-server && Rscript server.R --mode sse --port 8001

# 2. FastAPI 网关（端口 8000）
cd gateway && uv run uvicorn main:app --reload --port 8000

# 3. Vue 3 前端（端口 5173）
cd frontend && npm run dev
```

---

## v2.0 实现路线图

**架构迁移（前置）**
- [ ] R MCP Server SSE + stdio 双模式
- [ ] FastAPI 网关 + MCP 客户端 + AuditMiddleware
- [ ] Vue 3 + Pinia + ECharts DAG 图
- [ ] DAG 引擎 + `dag_schema.json`

**Specs 规划层**
- [ ] Schema Profiler + Domain Allocation
- [ ] SDTM IG Coverage Checker（Step 2）
- [ ] 变量五分类 + Derive 规则编辑器 + Gap Resolution

**上下文管理（CMS）**
- [ ] Layer 1：SDTM IG v3.4 元数据解析
- [ ] Layer 2：Define-XML 解析器（v2.0 + v2.1）
- [ ] Layer 3：HITL 对话摘要（Claude API）
- [ ] `VariableContext` 合并引擎 + Specs 编辑器

**域实现（按 DAG 顺序）**
- [ ] Trial Design：TA / TE / TV / TI / TS
- [ ] EX（提供 RFSTDTC）
- [ ] DS（提供 RFENDTC）
- [ ] AE + SUPPAE
- [ ] CM + SUPPCM
- [ ] MH

---

## 安全注意事项

- 平台仅在本地单机运行（`127.0.0.1`），数据不出本机
- LLM API 调用（Layer E）**只发送字段名和匿名化样本值**，不发送受试者 PII
- 审计日志文件设置为 OS 级 append-only 权限
- 所有时间戳以 UTC ISO 8601 格式记录

---

> PRD 版本：v2.0（PRD-SDTM-2026-002）· 2026 年 6 月
