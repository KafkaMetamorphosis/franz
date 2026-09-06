# 08 — Access-policy engine

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md)
Specs: `003-franz/003.5-access-policy`
Proto: `AccessPolicy` shapes (in `async_channel.proto`), `common.Permission`

## Goal

The pure-domain evaluator for "may this Client read / write this channel". No RPC
surface of its own — it is wired into `AsyncChannelService` in [10](./10-async-channel.md).
Build and unit-test it standalone now.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 08.1 | Domain types: `AccessPolicy`, `Statement`, `Effect` (`ALLOW` / `DENY`), `Principal` (`client_orn`, `labels` selector), `Permission` (`READ` / `WRITE`) | `003.5`, proto | ⬜ | |
| 08.2 | Validation: `effect != UNSPECIFIED`, `permissions` non-empty, `principal` has ≥1 of `client_orn` / `labels` | `003.5` | ⬜ | |
| 08.3 | Principal match — `client_orn` glob (02.4) **OR** label selector (02.3) over the client's labels; a `client_orn` that resolves to no Client is valid (matches nothing) | `003.5`, `003.1` | ⬜ | |
| 08.4 | Evaluation `(client, action)` → gather statements matching principal AND covering the action → any `DENY` ⇒ deny → else any `ALLOW` ⇒ allow → else deny (zero trust). Order-independent; `READ` / `WRITE` evaluated separately | `003.5` | ⬜ | |
| 08.5 | Resolved views: `effective(client, policy)` → permission set + `matched_by`; used by both `ListChannelClients` and `ListClientChannelAccess` | `003.5` | ⬜ | |
| 08.6 | Table-driven unit tests — includes the `003.5` worked wildcard example (`xpto-*` allow + `xpto-blah` deny), zero-trust default, `READ`/`WRITE` independence | `003.5` | ⬜ | |

## Done when

- The worked example from `003.5` and a broad matrix of principal/effect/permission
  combinations pass.
- The engine is a pure function of `(policy, client-list)` — no DB, no context.

## Notes

- Reuses the 02 selector matcher and glob matcher verbatim.
- `EFFECT_UNSPECIFIED` is rejected at write time (10.5), not silently skipped.
