# pentene_zsm5 研究生产线

本项目把“选题 -> 检索 -> 卡片化 -> 筛选 -> 论证 -> 写作 -> 审计”拆成可追溯的阶段。  
默认优先结构化中间层，不默认下载全部 PDF。

- 统一入口：`python scripts/reference_pipeline.py run ...`
- 保留细粒度入口：每个阶段仍可单独执行
- 关键日志：每阶段 `manifest.json` + 单入口 `pipeline_run_summary.json/.md`

---
## Disclaimer / 免责声明

This project is intended only as an **academic writing and research assistance tool**. It helps users organize references, structure ideas, generate outlines, and draft LaTeX-based documents, but it is **not** a substitute for independent thinking, original analysis, or responsible academic writing.

本项目仅作为**学术写作与研究辅助工具**，用于帮助用户整理文献、组织思路、生成大纲以及搭建 LaTeX 草稿框架；**并不能替代**用户本人的独立思考、原创分析和负责任的学术写作。

Users are solely responsible for verifying generated content, checking citations, revising drafts, and complying with the academic integrity requirements of their institution, course, journal, or supervisor.

用户应自行负责核查生成内容、检查引用来源、修改完善草稿，并遵守所在学校、课程、期刊或导师关于学术诚信的要求。

This project advocates **academic honesty, responsible AI use, and proper attribution of sources**. It must not be used for plagiarism, ghostwriting, or dishonest submission.

本项目倡导**学术诚信、负责任地使用 AI，以及规范引用来源**，不得用于抄袭、代写或任何不诚信的提交行为。

## Part A：新手用户（从 0 到可运行）

### A1. 先决条件

1. 进入仓库根目录：`D:\Dev\paper\pentene_zsm5`
2. Python 建议 3.10+
3. 先看命令是否可用：

```powershell
python --version
python scripts/reference_pipeline.py -h
python scripts/reference_pipeline.py run -h
```

如果以上命令能输出帮助信息，环境基本正常。

### A2. 什么是 TFR-1（必须先准备）

`run` 在默认阶段链中会调用 `search-candidates` 和 `outline-from-evidence`，这两步都依赖 `--topic-frame-json`。  
代码里校验的最小必填字段是：

- `topic`
- `research_questions`（对象）
- `keywords`（对象）
- `search_constraints`（对象）

#### A2.1 最小可运行 TFR-1（JSON）

把下面内容保存为 `config/topic_frame.json`：

```json
{
  "topic_frame": {
    "version": "TFR-1",
    "topic": "Pentene isomerization over ZSM-5",
    "objective": "Build an evidence-grounded draft for mechanism and condition analysis",
    "research_questions": {
      "primary": "How does ZSM-5 regulate pentene isomerization pathways?",
      "sub_questions": [
        "Which pathways dominate under low temperature?",
        "Which operating conditions shift selectivity?",
        "What are the major limitations in current evidence?"
      ]
    },
    "scope": {
      "in_scope": ["pentene", "ZSM-5", "isomerization", "mechanism/condition evidence"],
      "out_of_scope": ["non-zeolite catalysts", "C6+ unrelated systems"]
    },
    "keywords": {
      "core_concepts": ["pentene isomerization", "ZSM-5"],
      "domain_terms": ["zeolite catalysis", "olefin conversion"],
      "methods_or_mechanisms": ["reaction mechanism", "kinetic modeling", "DFT"],
      "bilingual_synonyms": ["戊烯异构化", "分子筛 ZSM-5"]
    },
    "exclusions": {
      "topics": ["patent-only records"],
      "document_types": ["editorial"],
      "low_priority_terms": ["ads", "teaching note"]
    },
    "output": {
      "type": "review",
      "expected_sections": ["Mechanism", "Condition effects", "Limitations"]
    },
    "search_constraints": {
      "time_range": {
        "enabled": true,
        "start_year": 2015,
        "end_year": 2026
      },
      "language_range": {
        "enabled": true,
        "languages": ["en"]
      },
      "venue_preference": {
        "mode": "balanced",
        "prioritize": ["journal", "conference"]
      }
    },
    "assumptions": ["Prefer mechanism-oriented evidence over broad survey statements"]
  }
}
```

#### A2.2 快速创建文件（PowerShell 一次粘贴）

```powershell
@"
{
  "topic_frame": {
    "version": "TFR-1",
    "topic": "Pentene isomerization over ZSM-5",
    "research_questions": {
      "primary": "How does ZSM-5 regulate pentene isomerization pathways?",
      "sub_questions": ["Which pathways dominate under low temperature?"]
    },
    "keywords": {
      "core_concepts": ["pentene isomerization", "ZSM-5"],
      "domain_terms": ["zeolite catalysis"],
      "methods_or_mechanisms": ["reaction mechanism"],
      "bilingual_synonyms": []
    },
    "search_constraints": {
      "time_range": {"enabled": true, "start_year": 2015, "end_year": 2026},
      "language_range": {"enabled": true, "languages": ["en"]},
      "venue_preference": {"mode": "balanced", "prioritize": ["journal", "conference"]}
    }
  }
}
"@ | Set-Content -Path config/topic_frame.json -Encoding UTF8
```

#### A2.3 TFR 文件自检（建议先跑）

```powershell
python scripts/reference_pipeline.py run `
  --topic-frame-json config/topic_frame.json `
  --from-stage search-candidates `
  --to-stage search-candidates
```

如果这一步失败，先看：

- `draft/runs/run_*/pipeline_run_summary.json`
- 该 run 下对应阶段 `manifest.json` 的 `errors`

### A3. 一条命令跑默认流程（推荐）

```powershell
python scripts/reference_pipeline.py run --topic-frame-json config/topic_frame.json
```

默认等价于：

- `from-stage=search-candidates`
- `to-stage=full-draft`（映射到 `assemble-full-draft`）
- `with-fulltext=false`（不下载 PDF）
- `strictness=soft`
- `continue-on-error=false`（失败即停）

### A4. 新手常见场景（直接复制）

#### 场景 1：只跑到“章节级审计”

```powershell
python scripts/reference_pipeline.py run `
  --topic-frame-json config/topic_frame.json `
  --to-stage section-citation-audit
```

#### 场景 2：失败后从中间继续

```powershell
python scripts/reference_pipeline.py run `
  --from-stage generate-paragraph-plans `
  --to-stage full-draft
```

#### 场景 3：把全文下载阶段也加上

```powershell
python scripts/reference_pipeline.py run `
  --topic-frame-json config/topic_frame.json `
  --with-fulltext
```

#### 场景 4：高风险就阻断（hard）

```powershell
python scripts/reference_pipeline.py run `
  --topic-frame-json config/topic_frame.json `
  --strictness hard
```

#### 场景 5：出错也继续跑后续阶段（排查用）

```powershell
python scripts/reference_pipeline.py run `
  --topic-frame-json config/topic_frame.json `
  --continue-on-error
```

### A5. 结果怎么看（按这个顺序）

1. 看本次总览：`draft/runs/run_*/pipeline_run_summary.md`
2. 看机器可解析总览：`draft/runs/run_*/pipeline_run_summary.json`
3. 看具体阶段详情：该阶段自己生成的 `draft/runs/run_*/manifest.json`

### A6. 关键产物地图（你应该关心哪些文件）

#### 检索/筛选层

- `references/index/candidates.csv`
- `references/index/cards.jsonl`
- `references/index/screening_decisions.csv`
- `references/index/included_candidates.csv`

#### 论证层

- `references/index/claims.jsonl`
- `references/index/argument_graph.json`
- `outline/generated_outline.md`

#### 写作中间层

- `draft/paragraph_plans/sec_*.json|md`
- `draft/evidence_packets/<section>/<paragraph>.json`
- `draft/latex/sections/sec_*.tex`

#### 审计/放行层

- `draft/latex/audit/section_*_audit.json|md`
- `draft/gates/section_*_gate.json`
- `draft/gates/section_*_fixlist.md`

#### 全稿层

- `draft/main.tex`
- `draft/full_draft_v1.tex`
- `draft/reports/full_draft_review.md`

### A7. 新手高频问题

#### 为什么默认不下载全文？

因为流程强调先做结构化判断，减少无效下载与外部网络失败。只有 `--with-fulltext` 才会插入下载阶段。

#### 一篇下载失败会不会中断？

不会。下载阶段策略是“单篇失败记录并继续下一篇”，最终状态会写进日志和 manifest。

#### 我不想每次都从头跑怎么办？

用 `--from-stage` 续跑。例如从 `generate-section-drafts` 继续到 `full-draft`。

#### 哪些错误最常见？

1. `topic_frame_json` 缺字段或路径不对。  
2. 网络限制导致检索/下载失败。  
3. 中间产物不存在（比如直接从后置阶段启动）。

---

## Part B：开发者与测试者

这部分面向实现和维护流程的人。

### B1. 当前架构与职责边界

核心文件：`scripts/reference_pipeline.py`

职责：

1. 子命令解析（`argparse`）
2. 各阶段执行函数（每阶段独立）
3. run 目录和 `manifest.json` 生成
4. 单入口 `run` 编排（阶段链 + 批处理 + 汇总）

本地技能（项目内）放在 `skills/`，不要写全局 skills。

### B2. 阶段链（实现级）

`run` 的标准顺序：

1. `search-candidates`
2. `cardify-candidates`
3. `screen-candidates`
4. `fetch-fulltext`（仅 `--with-fulltext`）
5. `outline-from-evidence`
6. `generate-paragraph-plans`
7. `assemble-evidence-packets`
8. `generate-section-drafts`
9. `revise-section-consistency`
10. `section-citation-audit`（在 `run` 中按 `sec_*.tex` 自动批处理）
11. `section-release-gate`（在 `run` 中按 `sec_*.tex` 自动批处理）
12. `generate-cross-section-bridges`
13. `export-claim-trace-matrix`
14. `ground-figure-table-links`
15. `generate-latex-draft`
16. `assemble-full-draft`
17. `citation-audit`
18. `latex-build-qa`

说明：`full-draft` 是 `assemble-full-draft` 的别名，仅用于 `run --to-stage`。

### B3. `run` 编排语义（重要）

- 默认失败策略：fail-fast（某阶段 failed 后停止）
- `--continue-on-error`：失败后继续后续阶段，但总状态仍会标记 failed
- 参数透传：
  - `strictness` 透传给支持该参数的阶段
  - `overwrite` 透传给支持覆盖语义的阶段
  - `run-compiler` 仅透传到 `latex-build-qa`
- topic frame 必填条件：
  - 选择范围包含 `search-candidates` 或 `outline-from-evidence` 时，必须给 `--topic-frame-json`

### B4. 运行产物契约（run 入口）

每次 `run` 会新建一个 `draft/runs/run_*`，并写：

- `manifest.json`：run 入口自己的 manifest
- `pipeline_run_summary.json`：阶段执行明细
- `pipeline_run_summary.md`：人读摘要

`pipeline_run_summary.json` 关注字段：

- `selected_stages`
- `executed_stage_count`
- `stage_results[]`（`stage/status/exit_code/manifest/duration_seconds/error`）
- `failure_point`

### B5. 测试建议（先快后全）

#### 快速检查

```powershell
python -m py_compile scripts/reference_pipeline.py
python -m unittest tests.test_pipeline_run_entry
```

#### 与 run 编排强相关的回归

```powershell
python -m unittest tests.test_section_release_gate
python -m unittest tests.test_literature_section_citation_audit
```

#### 全量回归（按需）

```powershell
python -m unittest discover -s tests -p "test_*.py"
```

### B6. 新增阶段/技能的落地要求

1. 先定义中间产物契约（JSON/CSV 字段）再写文本生成。
2. 阶段应保持单一职责，不把下载、审计、写作混成一个黑盒。
3. 每阶段都写独立 `manifest.json`，至少包含：
   - `inputs`
   - `status`
   - `outputs`
   - `warnings`
   - `errors`
4. `overwrite=false` 下不得覆盖人工改写。
5. Guardrail：
   - 不在无关阶段改 `references/index/*`
   - 不触发不属于该阶段的副作用

### B7. 常见调试路径

1. 先看 `pipeline_run_summary.json` 找失败 stage。
2. 打开该 stage 的 manifest 看 `errors`。
3. 若是章节批处理问题，检查：
   - `draft/latex/sections/sec_*.tex` 是否存在
   - 对应 `draft/latex/audit/section_*_audit.json` 是否产出
4. 若是续跑问题，检查 `from-stage` 是否早于 `to-stage`。

---

## 附录：命令帮助与参考

查看全部命令：

```powershell
python scripts/reference_pipeline.py -h
```

查看单入口参数：

```powershell
python scripts/reference_pipeline.py run -h
```

查看章节审计参数：

```powershell
python scripts/reference_pipeline.py section-citation-audit -h
```

查看阶段放行参数：

```powershell
python scripts/reference_pipeline.py section-release-gate -h
```
