# Quantia Frontend Design

## Direction

Quantia is an audit surface for investment decisions. The interface should feel like a decision ledger: quiet, exact, and built for repeated inspection. It is not a fintech marketing dashboard.

## Tokens

Color:

- Ledger ink `#101820`: navigation, type, final-state marks.
- Ledger panel `#F7F9F4`: main surfaces, chosen as a cool workbench color rather than warm cream.
- Ledger mist `#DDE5DF`: page field and low-contrast hatching.
- Signal teal `#2D8C83`: positive outcomes, healthy services, active rails.
- Oxidized copper `#B66A3C`: warnings, human-review marks.
- Audit violet `#6D5FA8`: model/planner/shadow marks.
- Loss red `#BA4A45`: negative outcomes and failed services.

Typography:

- Display: `Space Grotesk`, used for view titles and large numbers.
- Text: `Inter`, used for dense operational labels.
- Utility: `JetBrains Mono`, used for tickers, timestamps, endpoint status, and table headers.

Layout:

Chosen concept: ledger rail.

```text
+-----------+-----------------------------------------------+
| nav       | title / period / refresh                      |
|           +-----------------------------------------------+
| endpoints | plan -> review -> execution -> result         |
|           +-----------------------------------------------+
|           | kpis + charts + audit tables                  |
+-----------+-----------------------------------------------+
```

Rejected concepts:

```text
Hero dashboard:     too promotional for daily audit.
Pure terminal UI:   precise, but too hard to scan visually.
Broadsheet ledger:  close to audit, but matches a common AI default.
```

## Signature

The unique move is the plan -> revision -> execution -> result rail. It is built from real monitor payloads and mirrors the core project question: what did the bot decide, what did the human do, and what happened after 5D?

## Defaults Avoided

- No warm cream / serif / terracotta editorial theme.
- No black dashboard with a single neon green accent.
- No broadsheet newspaper layout with hairlines and zero radius.

## Implementation Notes

- Frontend API base defaults to `auto`: the browser resolves `http://<current-host>:8010`, so it works from localhost and from a private Tailscale hostname when CORS allows the same host.
- Login is stateless against the existing monitor auth contract. The token is stored in `sessionStorage`; TOTP is optional and sent as `X-TOTP-Code` when present.
- `decision-ledger` is consumed, but the current endpoint can exceed 30s locally. The UI shows that as endpoint evidence instead of hiding it.
