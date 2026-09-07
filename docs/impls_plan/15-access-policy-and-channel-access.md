# 15 — Access-policy engine & channel-access views

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md) · [10](./10-async-channel.md) · [14](./14-client.md)
Specs: `003-franz/003.5-access-policy`, `003-franz/003.10-clients`
Proto: `AsyncChannelService.ListChannelClients`, `ClientService.ListClientChannelAccess`

## Goal

The data-plane authorization **evaluator** — "may this Client read / write this
channel" — and the two resolved views built on it. Deliberately sequenced here,
**after Client ([14](./14-client.md))**: both views iterate/reference Clients, so
the engine has no exercisable consumer before Client exists.

The access-policy **document** (types + write validation) already shipped with
Async Channel ([10](./10-async-channel.md), tasks 10.5–10.7); this deliverable
adds matching, evaluation, and the views.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 15.1 | Principal match — `client_frn` glob (02.4) **OR** label selector (02.3) over the client's labels; a `client_frn` that resolves to no Client is valid (matches nothing) | `003.5`, `003.1` | ⬜ | |
| 15.2 | Evaluation `(client, action)` → gather statements matching principal AND covering the action → any `DENY` ⇒ deny → else any `ALLOW` ⇒ allow → else deny (zero trust). Order-independent; `READ` / `WRITE` evaluated separately | `003.5` | ⬜ | |
| 15.3 | Resolved view: `effective(client, policy)` → permission set + `matched_by`; a pure function of `(policy, client-list)` — no DB, no context | `003.5` | ⬜ | |
| 15.4 | Table-driven unit tests — the `003.5` worked wildcard example (`xpto-*` allow + `xpto-blah` deny), zero-trust default, `READ` / `WRITE` independence, `client_frn`-matches-nothing, broad principal/effect/permission matrix | `003.5` | ⬜ | |
| 15.5 | `AsyncChannelService.ListChannelClients` — the **forward** view: evaluate the channel's policy against every Client in the realm via 15.3; paginate. Replaces the `UNIMPLEMENTED` stub from 10.8 | `003.5` | ⬜ | |
| 15.6 | `ClientService.ListClientChannelAccess` — the **reverse** view: every channel whose policy grants this client anything, via 15.3; paginate | `003.5`, `003.10` | ⬜ | |
| 15.7 | REST — `/v1/async-channels/{name}/clients` and `/v1/clients/{name}/channel-access` | proto | ⬜ | |
| 15.8 | Integration tests — forward and reverse views **agree** for the same `(client, channel)` pair; a policy change is reflected in both; pagination | — | ⬜ | |

## Done when

- The `003.5` worked example and a broad evaluation matrix pass as unit tests.
- `ListChannelClients` and `ListClientChannelAccess` return consistent grants for
  every `(client, channel)` pair in an integration fixture.
- The engine is a pure function of `(policy, client-list)`.

## Notes

- Reuses the 02 selector matcher and glob matcher verbatim.
- The engine never mutates and never calls an agent — it is read-only evaluation.
- `ListChannelClients` lives on `AsyncChannelService` (proto) but is implemented
  here, not in [10](./10-async-channel.md), because it needs Clients.
