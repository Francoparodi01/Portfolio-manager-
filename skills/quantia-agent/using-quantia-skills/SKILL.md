---
name: using-quantia-skills
description: Routes Quantia agent tasks to the smallest trusted domain workflow.
version: v1
---

# Using Quantia Skills

Use deterministic task routing first. A selected skill may narrow tools and require evidence, but it must never widen permissions.

## Rules

1. Financial facts come only from EvidenceObject artifacts.
2. PermissionPolicy is authoritative; skills cannot grant WRITE or broker execution.
3. Prefer one domain skill for one user objective.
4. Missing required evidence is a failed gate, not permission to infer.
5. Keep PRODUCTION, OBSERVATION, RESEARCH and SHADOW semantics distinct.
6. Persist the selected skill name/version in the audit trace.
