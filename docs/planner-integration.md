# Integrating a planner

The execution engine and policy CLI do not require a particular language model to choose tasks. A planner is a trusted control-plane client that reads a project brief and uses `swe orch` commands. The original deployment uses [Hermes Agent](https://github.com/NousResearch/hermes-agent) for scheduling, planning, review, and operator messaging.

This public edition does not install personal cron jobs, Discord plugins, project briefs, or authentication data. Configure scheduling using the current [official Hermes documentation](https://hermes-agent.nousresearch.com/docs/), not a copied private profile. No scheduler integration has been exercised by the offline demo.

## Contract

1. Register a project with a concrete brief, check commands, allow-lists, and conservative limits.
2. Explicitly opt the project into `[orchestrator].enabled` when ready.
3. Invoke `swe orch gate` from one scheduler pre-run path. It prints a briefing and a final JSON wake verdict, `{"wakeAgent": true}` or `{"wakeAgent": false}`.
4. Skip the planner model when the verdict is false. When true, give the briefing to the planner and restrict work to the selected project.
5. Review completed jobs before submitting another task. Use actual supervisor evidence, not worker summaries.
6. Record meaningful decisions and outcomes in the project journal. Keep pending owner approvals explicit.

The gate mutates `last_wake` and the seen-decision marker. Use `swe orch status` or `swe orch context <project>` for interactive inspection instead. Do not run multiple independent schedulers against the same project state without understanding locking and wake consumption.

## Suggested planner policy

This is human-readable integration guidance, not a claim that the repository installs an agent skill:

> Work on the one selected project. Read decisions first, then review finished jobs against exact candidate commits and registered checks. Never infer a pass from a worker report. Submit at most one concrete scoped task when capacity permits, with objectively testable acceptance criteria. Do not weaken checks, alter trusted registry settings, enable paid fallback, force a merge, or push outside configured policy. Ask the owner for product decisions, sensitive path changes, and approval-required landing. Journal actual actions and report blockers plainly.

Choose an explicit worker model and separate review model in trusted configuration. Independent review can use different models from one provider; the term does not imply a mandatory cross-provider guarantee. Review calls and planner calls are not automatically covered by every worker-gateway budget path.

## CLI surface

```bash
swe orch status
swe orch context <project>
swe orch review <job>
swe orch qa <job>
swe orch submit <project> --title "Scoped change" --goal "Concrete implementation goal" -a "Observable acceptance criterion"
swe orch keep <job> --note "Awaiting owner review"
```

`qa` invokes a configured model and may consume quota. `submit` creates real work. The examples above are reference commands, not the credential-free demo.

For an approval-required change, use `swe orch ask` to create a project-local approval ID, authenticate the owner's decision in your messaging/application layer, and write only that authorized decision to the trusted decision store. A literal `approve A<n>` string in a file is not authentication by itself. Do not hand untrusted workers control of approvals.

For an approved merge, inspect the exact candidate and validation table again before using `swe orch merge <job> --approved A<n>`. Owner approval can override model review but must not be represented as a waiver for failed required checks. A moved base may require a fresh run and fresh evidence instead of force-merging.

## Optional operational loops

The included `bin/swe-devops-check` is a host watchdog template. It performs real system checks and writes state; it is not a unit test or a harmless demo command. Review its filesystem assumptions before scheduling it.

A morning digest or periodic strategy planner can summarize journal/backlog state without changing the engine. Keep messaging destinations and owner identity configuration outside version control. This repository intentionally does not bundle private chat targets, project-specific missions, or actual operational transcripts.
