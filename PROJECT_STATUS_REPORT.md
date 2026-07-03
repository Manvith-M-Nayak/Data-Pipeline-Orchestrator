# Data Pipeline Orchestrator: Agent Architecture, Implementation & Status

**Date:** 2026-06-26
**Scope:** the orchestration backend and its web dashboard.
**Overall progress:** 4 of 9 agents complete (Planner, Central Manager, Executor, Monitor).
**Resource Agent is almost done.** Performance Prediction is in early build. Cost Optimization,
Assurance, and Learning & Policy Update remain (see Section 3 Roadmap). **This is not the finished system.**

---

## 1. The Agent-Based Approach (Detailed Plan)

**What it does, in plain terms:**

- You give it the data and a plain-English prompt.
- It figures out the pipeline, builds it on Azure, runs it, and keeps an eye on it.
- The data flows through the usual stages: `raw → bronze → silver → gold`.
- Don't like the stages it picked? You can change them yourself before it runs.

**How it's built:**

- Instead of one big program, it's a team of small AI agents, each with one job.
- One plans, one sizes the hardware, one runs things, one watches.
- They talk to each other by passing clean hand-offs: a plan, an allocation, the run state.
- They learn from how past runs actually went.
- Splitting it this way keeps each agent simple to build and test on its own.
- The Central Manager keeps them all in sync from start to finish.

### Roles

The target system is **9 agents**. Five are built; four remain (see Section 3).

| Agent | Role | Agent type | Status |
|-------|------|------------|--------|
| **Planner** | Reasons over schema + prompt to decide pipeline shape: stages, containers, transforms, compute settings | LLM reasoning agent | Done |
| **Central Manager** | Orchestrating agent: validates plan, runs pre-checks, drives execution w/ retry, runs assurance, records feedback | Orchestration agent | Done |
| **Resource Agent** | Works out the hardware and time each stage needs, checks it fits the limits, right-sizes it, and learns from real runs | Planning / optimization agent | Almost done |
| **Executor** | Builds and runs the pipeline on the cloud, then returns the result | Acting agent | Done |
| **Monitor** | Watches live runs, flags anomalies, explains each finished run, streams live state | Perception agent | Done |
| **Performance Prediction** | Forecasts whole-plan runtime/throughput/outcome (success/slowdown/failure) before run | Predictive agent | In progress |
| **Cost Optimization** | Cheapest config meeting performance targets | Optimization agent | Planned |
| **Assurance** | Validates a completed run is correct + meets quality bars | Verification agent | Planned |
| **Learning & Policy Update** | Learns from all agents' outcomes to improve future plans | Learning agent | Planned |

> **Note on interim coverage:** the Central Manager currently does a *basic* inline cost
> estimate (Phase 2b) and *basic* assurance checks (Phase 4). These are placeholders; the
> dedicated **Cost Optimization** and **Assurance** agents will supersede them with full logic.

### Managed run lifecycle (Central Manager, 5 phases)

1. **Validate:** check the plan is complete and coherent: every stage is well-formed, the declared
   run order only points at real stages, and each stage has what it needs. Abort on hard problems.
2. **Pre-checks:**
   - **Parallelism analysis**: work out which stages depend on each other (a stage depends on
     another if it reads what that one produces), and group the independent ones to run in parallel.
   - **Resource prediction** (Resource Agent): estimate the hardware and time each stage needs, and
     confirm it fits the limits. Abort if it doesn't.
   - **Cost estimate**: add up orchestration, compute, and storage costs into a rough dollar total
     and flag if it's over budget.
3. **Execute:** hand the plan to the Executor, with a couple of automatic retries (and a short
   wait between them) if a run fails.
4. **Assurance:** after the run, sanity-check it: did it finish in a reasonable time, did every
   stage complete, is there output, and did it need too many retries.
5. **Feedback:** record how the run actually went and feed the real timings back to the Resource
   Agent so its future estimates get better.

### How the agents work together

- Each agent has its own goal and works off what it can see.
- The Planner does the creative thinking (that's the LLM).
- The specialists (Resource, Performance, Cost, Assurance) each handle one narrow problem.
- The Monitor watches things as they run.
- The Central Manager keeps the order straight and passes results between them.
- Every decision gets logged (what it did, why, and how it turned out), so the reasoning is always reviewable.
- Every run feeds back into the system, so the predictions get a little better over time.

---

## 2. Implementation: What's Built So Far

### 2.1 Planner Agent: Complete

- The creative step, where an LLM decides how the pipeline should look.
- Reads the shape of the incoming data plus the user's prompt.
- Decides: how many stages, what each is named, which ones just move data vs transform it, and what compute settings suit the data size.
- The user can review and adjust any of it before the run.
- If the LLM is unavailable, it falls back to a safe default plan so the system still works.

### 2.2 Central Manager Agent: Complete (integration ongoing)

- Runs the whole show, carrying each run through the five phases (validate → pre-checks → execute → assurance → feedback).
- Keeps a single live record of the run's status, every decision made, and every check's result.
- That record is what the dashboard shows, so anyone can watch a run progress and see why each decision was taken.

### 2.3 Resource Agent: Almost done (core built, tuning ongoing)

- Takes a plan and works out the actual hardware each stage needs, making sure it fits the limits.
- Core logic is done and already plugged into the Manager's pre-checks.
- Still left: the dashboard view, the live-adjustment hookup, and tuning its estimates against real run data.

What it does, end to end:

- Estimates the compute and time each stage needs (separately for move-style and transform-style stages).
- Checks whether the plan fits the hardware limits, and refuses plans that can't.
- Proposes a concrete amount of hardware per stage and trims anything over-provisioned.
- When stages want to run in parallel but together exceed the limits, scales them back or runs some one after another.
- Can recommend adjustments mid-run (scale up a slow stage, free a finished one).
- Learns from each run by comparing predicted against actual, then nudges its future estimates.
- Works within fixed ceilings (workers, parallel stages, total memory) suited to our current cloud tier.

### 2.4 Executor Agent: Complete

- The part that actually builds and runs the pipeline on the cloud.
- Signs in, sets up the storage areas, and uploads the data.
- Generates the transformation code for each transform stage.
- Runs the data-movement steps and the compute steps, then returns a clear result.
- Reports progress as it goes; tells the Monitor when it finishes so the run gets tracked.

### 2.5 Monitor Agent: Complete

- Watches runs as they happen, checking in on active runs regularly.
- Compares each run against its own history and flags anything running unusually slowly.
- When a run finishes, an LLM writes a short plain-language explanation of how it went and why it took as long as it did.
- Streams live updates straight to the dashboard.

### 2.6 Platform & Dashboard: Complete

- Reads an uploaded file and infers its structure.
- Lets the user download the finished output.
- Pushes live updates to the screen.
- The dashboard gives each agent its own view, plus pages for live runs, predictions, anomalies, and logs.

### 2.7 Performance Prediction Agent: In progress (design + early build)

- **The one we're building next.** The Resource Agent tells you how long each *stage* takes; this
  agent steps back and looks at the *whole plan*: total runtime, where it'll get stuck, and
  whether it's likely to succeed, all before anything runs.
- **What it uses:** the approved plan, the Resource Agent's per-stage estimates, and the history
  of how past runs actually went (collected by the Executor and Monitor).
- **What it will produce:**
  - a forecast of total runtime and throughput for the plan,
  - the stage most likely to be the bottleneck,
  - a likely outcome (**success / slowdown / failure**) with a confidence score,
  - a warning if the run risks missing its target time.
- **How:** start simple with a transparent formula built on the Resource Agent's estimates, then
  improve it with a model trained on the growing history of real runs.
- **Where it fits:** the Central Manager will call it during pre-checks, right after resource
  prediction, so its forecast can stop a doomed run early and later feed the Cost agent.
- **Status:** design is settled and unblocked; early build underway. Not yet wired into the run flow.

---

## 3. Roadmap: Remaining Agents

- Four agents to go, and the order matters, because each one needs what the one before it produces.
- So we build them front to back instead of jumping ahead.

### 3.1 Performance Prediction Agent: **In progress (current milestone)**

- **What:** forecasts whole-plan runtime, throughput, and likely outcome
  (success / slowdown / failure) *before* the plan runs.
- **Distinct from Resource Agent:** the Resource Agent says "*this allocation* will take ~X min";
  the Performance Agent reasons about the *plan as a whole*: bottleneck stages, SLA breaches,
  failure risk.
- **Why now:** the natural next link in the chain. It consumes the Resource Agent's allocation
  estimates and is unblocked by the real execution data the Executor + Monitor are already
  collecting (to train + validate against). It is a prerequisite for the Cost Agent.
- **Depends on:** Resource Agent (allocations), Executor + Monitor (real run data).
- **Build detail:** see Section 2.7.

### 3.2 Cost Optimization Agent

- **What:** computes cost trade-offs of a plan; proposes the cheapest config that still meets
  performance targets.
- **Why after Performance:** needs both *how much hardware* (Resource Agent) and *how long / how
  fast* (Performance Agent) to compute cost, so it must come after both.
- **Depends on:** Resource Agent + Performance Prediction Agent.

### 3.3 Assurance Agent

- **What:** validates that a completed run is correct and meets quality bars.
- **Why after Cost:** needs the full **plan → predict → optimize → execute** loop running
  end-to-end, which exists only once the three above are in place. Supersedes the Manager's
  current basic Phase-4 assurance checks.
- **Depends on:** full pipeline loop.

### 3.4 Learning & Policy Update Agent: **LAST**

- **What:** closes the feedback loop, learning from every other agent's outcomes to improve
  future plans.
- **Why last:** only meaningful once all other agents produce real results to learn from. The
  Manager already has the hook (its Phase-5 feedback step) and the Resource Agent already emits
  prediction-error signals it can consume.
- **Depends on:** all agents producing outcome data.

**Dependency chain:**
`Resource (in progress) -> Performance (in progress) -> Cost -> Assurance -> Learning`
</content>
