# Quantia Agent Skills v1

Quantia now supports repository-owned workflow skills for the conversational agent.

\`\`\`text
message
  -> TaskParser
  -> SkillRouter
  -> ContextSelector
  -> PermissionPolicy
  -> bounded tools
  -> EvidenceObject
  -> grounded synthesis + selected SKILL.md
  -> HarnessVerifier
  -> skill verification gates
  -> answer
\`\`\`

Skills do not replace the economic engine. They cannot authorize actions or widen the tool surface selected by the harness.

## Safety contract

- A skill can only reduce the tools already allowed by ContextSelector.
- PermissionPolicy remains the final authority.
- Required skill evidence is checked deterministically after synthesis.
- Missing evidence fails closed to the existing safe fallback.
- Skill name/version/path are persisted in the run metadata.
- PRODUCTION, OBSERVATION, RESEARCH and SHADOW remain distinct.

## v1 routing

| Intent | Skill |
|---|---|
| portfolio_review | portfolio-risk-review |
| position_analysis | analyze-ticker |
| decision_explanation | explain-investment-decision |
| decision_consistency_audit | audit-decision-consistency |
| opportunities | find-opportunities |
| decision_lab | compare-against-hold |

The decision-consistency skill specifically compares signal/score with planner current/target/delta weights and action. It requires both \`signals\` and \`plan.decisions\` from structured decision evidence.

## Validation

\`\`\`bash
python -m compileall -q src/agentic/harness
pytest -q tests/test_quantia_agent_skills.py
pytest -q tests/test_conversational_harness.py
\`\`\`
