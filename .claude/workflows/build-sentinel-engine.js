export const meta = {
  name: 'build-sentinel-engine',
  description: 'Build Component B (sentinel-engine.md) end-to-end against the live backbone: the full CrewAI hierarchical crew (A1 Manager, A2 Log Analyst, A3 Data Profiler, A4 Data Engineer, A5 Incident Commander), the B1 trigger, scoring vs the I4 injected_incidents oracle, handling all 14 generator failures. Then e2e inject->detect->score tests against the live pipeline + closer gate (simplify->review->fix->document) until clean. /goal: end-to-end code AND tests done against the entire pipeline.',
  phases: [
    { title: 'Design',    detail: 'crewai-architect: crew topology, scoring rubric, B1 trigger, 14-failure map' },
    { title: 'Fix+Base',  detail: 'fix sentinel import bug; build A1 + A2 + A3 + scoring' },
    { title: 'Resolve',   detail: 'A4 Data Engineer + A5 Incident Commander + B1 trigger' },
    { title: 'Verify',    detail: 'e2e inject->detect->score tests vs live pipeline, all 14 failures' },
    { title: 'Gate',      detail: 'simplify -> review -> fix -> document until clean' },
  ],
}

const REPO = '/Users/luanmorenomaciel/GitHub/ws-3-crew-ai-multi-agent'

const GROUND = [
  'REPO: ' + REPO + '. venv: .venv/bin/python, .venv/bin/pytest. ALWAYS set PYTHONPATH=<repo-root>, DUCKDB_DATABASE=<repo>/platform/warehouse/warehouse.duckdb, DAGSTER_HOME=<repo>/.dagster_home. crewai 0.100.0 installed.',
  '',
  'CRITICAL — DuckDB SINGLE-WRITER. Before any warehouse-touching command: `lsof -t platform/warehouse/warehouse.duckdb | xargs -r kill -9`. Never run two warehouse readers/writers concurrently. The Sentinel READS the warehouse (gold/silver) + Postgres (injected_incidents) read-only — but a concurrent backbone write will still lock it, so serialize. Do NOT start `dagster dev`.',
  '',
  'COMPONENT B IS PROBABILISTIC (R5): verify B by SCORING against ground truth (did the crew diagnosis match the injected_incidents failure_key?), NEVER by asserting output is "correct". The I4 ledger (injected_incidents, 15+ rows) is the oracle.',
  'R3 ONE-WAY DEPENDENCY: B reads A read-only via the interface. A never imports/calls B. The Sentinel must NOT modify platform/ except a *proposed* gated patch from A4 (never auto-applied).',
  '',
  'EXISTING sentinel/ (top-level, NOT src/sentinel) — extend, do not rebuild:',
  '- sentinel/crew.py: @CrewBase SentinelCrew, ONE placeholder manager agent + observe_task, Process.hierarchical, manager_llm="gpt-4o-mini". A SHELL.',
  '- sentinel/scoring.py: score_diagnosis(diagnosis_key) -> "correct"/"incorrect" by exact failure_key match vs latest injected_incidents row. Works but exact-match only.',
  '- sentinel/config/agents.yaml (manager only), sentinel/config/tasks.yaml (observe_task only).',
  '- sentinel/__init__.py: BUG — imports `from src.sentinel.crew import SentinelCrew` but the package is top-level `sentinel`, not `src.sentinel`. FIX to `from sentinel.crew import SentinelCrew`.',
  '- tests/sentinel/test_scoring.py exists.',
  '',
  'THE INTERFACE B CONSUMES (sentinel-engine.md I1-I5):',
  '- I1 Dagster run logs/asset status (from C2) -> A2 Log Analyst. Dagster runs recorded in DAGSTER_HOME (.dagster_home/history). The backbone_failure_logger run_failure_sensor emits "BACKBONE_RUN_FAILURE run_id=... job=... error=..." lines.',
  '- I2 dbt run results (from C3) -> A2. platform/transform/target/run_results.json.',
  '- I3 DuckDB gold_/silver_ tables (from C4) -> A3 Data Profiler. silver_*_rejects hold caught defects (U3).',
  '- I4 injected_incidents + failure signature (from C1 Postgres) -> scoring oracle. src/gen/repository.session() / scoring.py.',
  '- I5 incident RAG store -> A5 (B2, self-built; cold-starts empty).',
  '',
  'THE 14 FAILURES + capability map (src/gen/failures.py `unlocks` fields):',
  '- A3 Profiler (data-quality, base): negative_price, missing_customer, invalid_quantity, duplicate_order, late_arrival, volume_spike, orphan_payment.',
  '- A2 Log Analyst (pipeline errors, base): schema_drift, slow_source.',
  '- Advanced (each unlocks one CrewAI feature): recurring_incident->Memory; ambiguous_anomaly->Knowledge/RAG; destructive_fix->Human-in-the-loop; malformed_data->Guardrails+output_pydantic; slow_source->tool reliability (max_retry/timeout); multi_failure_cascade->Flows+conditional routing (A1 Manager routes sub-failures).',
  'Inject via `make inject FAILURE=<key>` or src.gen.cli. Reset clean via `make reset-schema` / reseed (R7: every inject->detect->score restores baseline first — reproducibility).',
  '',
  'SCOPE (user-locked): build the FULL crew A1-A5 + B1 trigger + scoring, handling all 14 failures. End-to-end code AND tests.',
  'HONESTY RULE: never claim a detection/score you did not observe. Score against I4, report real match/no-match. crewai agents make real LLM calls — if no API key is available in this env, build the crew + a deterministic stub-LLM test path and SAY SO; do not fake live-LLM results.',
].join('\n')

const SCHEMA = {
  type: 'object', additionalProperties: false,
  required: ['summary', 'files', 'how_to_run', 'self_check'],
  properties: {
    summary: { type: 'string' },
    files: { type: 'array', items: { type: 'string' } },
    how_to_run: { type: 'string' },
    self_check: { type: 'string', description: 'Real commands run + observed result, or honest skip-with-reason. No unobserved claims.' },
  },
}

// ── Phase 1: Design (crewai-architect, no Bash) ─────────────────────────────
phase('Design')
const design = await agent(
  GROUND + '\n\nYou are the crewai-architect. Design the full Sentinel crew per sentinel-engine.md. Decide + justify (ground in .claude/kb/crewai + MCP /websites/crewai_en):\n' +
  '1. Crew topology: hierarchical Process, manager_llm vs manager_agent for A1; how A2/A3 (investigation) and A4/A5 (resolution) are wired; agents.yaml/tasks.yaml structure for all 5.\n' +
  '2. The 14-failure routing: which agent detects which failure, and how A1 routes multi_failure_cascade (Flows/conditional). Map every failure to a detection path + the CrewAI feature it unlocks.\n' +
  '3. The scoring rubric (sketch flags this unresolved): extend scoring.py beyond exact failure_key match — define what "correct diagnosis" means (exact key? key + evidence? partial credit?). Recommend a concrete, testable rubric.\n' +
  '4. B1 trigger: webhook on Dagster run failure vs poll injected_incidents — recommend one, with the detection-latency tradeoff.\n' +
  '5. The agent TOOLS each needs (ReadDagsterLogs/ReadDbtRunResults for A2; QueryDuckDB/ProfileTable for A3; etc) and how they read I1-I5 read-only.\n' +
  '6. The stub-LLM test strategy so e2e is verifiable without burning live API calls.\n' +
  'Output a precise file manifest + key decisions + red flags. No code.',
  { label: 'design:crew', agentType: 'crewai-architect', phase: 'Design', schema: {
    type: 'object', additionalProperties: false,
    required: ['topology', 'failure_routing', 'scoring_rubric', 'trigger_design', 'tools_design', 'test_strategy', 'file_manifest', 'red_flags'],
    properties: {
      topology: { type: 'string' }, failure_routing: { type: 'string' }, scoring_rubric: { type: 'string' },
      trigger_design: { type: 'string' }, tools_design: { type: 'string' }, test_strategy: { type: 'string' },
      file_manifest: { type: 'string' }, red_flags: { type: 'string' },
    } } })

const D = '\n\nARCHITECT DESIGN (follow it):\n' + JSON.stringify(design, null, 2)

// ── Phase 2: Fix + base crew (A1+A2+A3+scoring) ─────────────────────────────
phase('Fix+Base')
const base = await agent(
  GROUND + D + '\n\nYou are the crewai-developer. Build the base + investigation crew:\n' +
  '1. FIX sentinel/__init__.py import bug (src.sentinel -> sentinel).\n' +
  '2. Implement A1 Manager (already shell — flesh out), A2 Log Analyst (reads I1 Dagster logs in .dagster_home + I2 dbt run_results.json), A3 Data Profiler (reads I3 DuckDB gold/silver + silver_*_rejects). Add their agents.yaml/tasks.yaml entries + the tools they need (read-only).\n' +
  '3. Extend sentinel/scoring.py per the architect rubric (beyond exact-match if recommended), keeping the existing API working.\n' +
  '4. Wire detection for the ~9 base failures (A3: negative_price/missing_customer/invalid_quantity/duplicate_order/late_arrival/volume_spike/orphan_payment; A2: schema_drift/slow_source).\n' +
  '5. Verify the crew BUILDS (instantiate SentinelCrew().crew(), assert Process.hierarchical, agents present) and scoring works against the live I4 ledger. Use the stub-LLM path for any agent run. Run `.venv/bin/pytest tests/sentinel/ -q`.\n' +
  'Report files + real verification output. R3: do not modify platform/.',
  { label: 'build:base-crew', agentType: 'crewai-developer', phase: 'Fix+Base', schema: SCHEMA })

// ── Phase 3: Resolution squad + trigger (A4+A5+B1) ──────────────────────────
phase('Resolve')
const resolve = await agent(
  GROUND + D + '\n\nBase crew built:\n' + JSON.stringify(base) + '\n\nYou are the crewai-developer. Build the resolution squad + trigger + advanced features:\n' +
  '1. A4 Data Engineer: produces a *PROPOSED* dbt/Dagster patch for a diagnosed root cause — GATED, written to a proposals/ dir or returned as text, NEVER auto-applied to platform/ (R3).\n' +
  '2. A5 Incident Commander: B2 incident RAG (cold-starts empty; injected_incidents can bootstrap) + writes a blameless post-mortem (markdown). Use guardrails/output_pydantic for the typed post-mortem (malformed_data unlock).\n' +
  '3. B1 trigger per the architect design (webhook-on-dagster-failure or poll injected_incidents): a runnable entrypoint that detects a new incident and invokes the crew.\n' +
  '4. The advanced-feature unlocks per the 14-failure map: Memory (recurring_incident), Knowledge/RAG (ambiguous_anomaly), HITL (destructive_fix), Guardrails+output_pydantic (malformed_data), tool max_retry/timeout (slow_source), Flows/conditional routing (multi_failure_cascade via A1).\n' +
  '5. Verify each new piece builds + the stub-LLM path runs. R3: A4 patches are proposals only.\n' +
  'Report files + real verification.',
  { label: 'build:resolve+trigger', agentType: 'crewai-developer', phase: 'Resolve', schema: SCHEMA })

// ── Phase 4: Verify (e2e inject->detect->score, all 14) ─────────────────────
phase('Verify')
const verify = await agent(
  GROUND + D + '\n\nCrew built:\n' + JSON.stringify({ base, resolve }) + '\n\nYou are the crewai-developer. Build AND RUN the end-to-end Sentinel test against the LIVE pipeline. The loop per R7: reset clean -> inject ONE known failure (make inject) -> run C2 ingest + dbt so the defect surfaces in I1/I2/I3 -> run the crew to diagnose -> score vs I4 (injected_incidents) -> assert the score is correct -> reset clean.\n' +
  '1. tests/sentinel/test_e2e_diagnose.py: parametrized over the 14 failure keys (or the base 9 live + the 5 advanced via stub where live LLM is needed). Each: inject -> surface -> diagnose -> score -> assert match. SERIALIZE (single DuckDB writer); reset-to-clean between cases (reproducibility).\n' +
  '2. Use the stub-LLM path so the test is deterministic and does not require live API keys; if a live-LLM smoke is possible (key present), run ONE case live and report. If no key, say so.\n' +
  '3. Report: how many of the 14 failures the crew correctly diagnoses (scored against I4), per-failure PASS/FAIL, and the honest caveat about stub vs live LLM.\n' +
  'Return the real scored results. This is the probabilistic-verification proof (R5).',
  { label: 'verify:e2e-score', agentType: 'crewai-developer', phase: 'Verify', schema: {
    type: 'object', additionalProperties: false,
    required: ['failures_scored', 'per_failure_results', 'commands_run', 'stub_or_live', 'caveats'],
    properties: {
      failures_scored: { type: 'string', description: 'e.g. "9/9 base correct, 5/5 advanced via stub"' },
      per_failure_results: { type: 'string' },
      commands_run: { type: 'string' },
      stub_or_live: { type: 'string' },
      caveats: { type: 'string' },
    } } })

// ── Phase 5: Closer gate ────────────────────────────────────────────────────
phase('Gate')
let round = 0, reviewVerdict = null
const gateLog = []
while (round < 3) {
  round++
  const simplify = await agent(
    GROUND + '\n\nRound ' + round + '. You are the code-simplifier. Behavior-preserving cleanup over sentinel/ + tests/sentinel/ only. Apply; run `.venv/bin/pytest tests/sentinel/ -q` to prove green. Revert anything that breaks a test. Report.',
    { label: 'gate:simplify#' + round, agentType: 'code-simplifier', phase: 'Gate', schema: {
      type: 'object', additionalProperties: false, required: ['changes_applied', 'tests_green', 'summary'],
      properties: { changes_applied: { type: 'number' }, tests_green: { type: 'boolean' }, summary: { type: 'string' } } } })
  const review = await agent(
    GROUND + '\n\nRound ' + round + '. You are the code-reviewer. Review ALL of sentinel/ + tests/sentinel/. Ground in .claude/kb/crewai. Focus: R3 one-way dependency (B must NOT import/call/mutate platform/ except gated A4 proposals); scoring correctness vs I4; reset-to-clean reproducibility (R7); the hierarchical-process wiring; guardrails/output_pydantic on the post-mortem; that detection is SCORED not asserted (R5). Classify BLOCKER/IMPORTANT/NIT. Recommend APPROVE/APPROVE_WITH_FIXES/BLOCK.',
    { label: 'gate:review#' + round, agentType: 'code-reviewer', phase: 'Gate', schema: {
      type: 'object', additionalProperties: false, required: ['blockers', 'important', 'nits', 'recommendation', 'findings'],
      properties: { blockers: { type: 'number' }, important: { type: 'number' }, nits: { type: 'number' },
        recommendation: { type: 'string', enum: ['APPROVE', 'APPROVE_WITH_FIXES', 'BLOCK'] }, findings: { type: 'string' } } } })
  gateLog.push({ round, blockers: review.blockers, rec: review.recommendation })
  reviewVerdict = review
  log('Sentinel gate round ' + round + ': ' + review.blockers + ' blockers, rec=' + review.recommendation)
  if (review.blockers === 0 && review.recommendation !== 'BLOCK') break
  await agent(
    GROUND + '\n\nRound ' + round + '. Fix these reviewer findings:\n' + review.findings + '\n\nFix every BLOCKER + safe IMPORTANTs. Run `.venv/bin/pytest tests/sentinel/ -q` green. Report.',
    { label: 'gate:fix#' + round, agentType: 'crewai-developer', phase: 'Gate', schema: {
      type: 'object', additionalProperties: false, required: ['fixed', 'tests_green', 'summary'],
      properties: { fixed: { type: 'string' }, tests_green: { type: 'boolean' }, summary: { type: 'string' } } } })
}
const docs = await agent(
  GROUND + '\n\nReview-clean. You are the code-documenter. Document Component B: docstrings on public APIs in sentinel/; write sentinel/README.md (WHAT/WHY/HOW: the 5 agents, B1 trigger, scoring vs I4, the 14-failure map, how to run inject->detect->score); write docs/adrs/0002-sentinel-engine.md (hierarchical crew, scoring rubric chosen, B1 trigger mechanism, R3 gated-proposal seam, stub-vs-live LLM testing). Reference CLAUDE.md + sentinel-engine.md. Report files.',
  { label: 'gate:document', agentType: 'code-documenter', phase: 'Gate', schema: {
    type: 'object', additionalProperties: false, required: ['docstrings_added', 'readme', 'adr', 'summary'],
    properties: { docstrings_added: { type: 'number' }, readme: { type: 'string' }, adr: { type: 'string' }, summary: { type: 'string' } } } })

return {
  design: { topology: design.topology, scoring_rubric: design.scoring_rubric, trigger: design.trigger_design },
  base: base.summary,
  resolve: resolve.summary,
  verify: { scored: verify.failures_scored, per_failure: verify.per_failure_results, mode: verify.stub_or_live, caveats: verify.caveats },
  gate: { rounds: gateLog, finalRec: reviewVerdict?.recommendation, blockers: reviewVerdict?.blockers },
  docs: docs.summary,
}
