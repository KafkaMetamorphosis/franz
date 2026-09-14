# 17 — Access-policy engine & channel-access views

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex out of quota (resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md) · [10](./10-async-channel.md) · [16](./16-client.md)
Specs: `003-franz/003.5-access-policy`, `003-franz/003.10-clients`
Proto: `AsyncChannelService.ListChannelClients`, `ClientService.ListClientChannelAccess`

## Goal

The data-plane authorization **evaluator** — "may this Client read / write this
channel" — and the two resolved views built on it. Deliberately sequenced here,
**after Client ([16](./16-client.md))**: both views iterate/reference Clients, so
the engine has no exercisable consumer before Client exists.

The access-policy **document** (types + write validation) already shipped with
Async Channel ([10](./10-async-channel.md), tasks 10.5–10.7); this deliverable
adds matching, evaluation, and the views.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 17.1 | Principal match — `client_frn` glob (02.4) **OR** label selector (02.3) over the client's labels; a `client_frn` that resolves to no Client is valid (matches nothing) | `003.5`, `003.1` | ✅ | 2026-09-13 |
| 17.2 | Evaluation `(client, action)` → gather statements matching principal AND covering the action → any `DENY` ⇒ deny → else any `ALLOW` ⇒ allow → else deny (zero trust). Order-independent; `READ` / `WRITE` evaluated separately | `003.5` | ✅ | 2026-09-13 |
| 17.3 | Resolved view: `effective(client, policy)` → permission set + `matched_by`; a pure function of `(policy, client-list)` — no DB, no context | `003.5` | ✅ | 2026-09-13 |
| 17.4 | Table-driven unit tests — the `003.5` worked wildcard example (`xpto-*` allow + `xpto-blah` deny), zero-trust default, `READ` / `WRITE` independence, `client_frn`-matches-nothing, broad principal/effect/permission matrix | `003.5` | ✅ | 2026-09-13 |
| 17.5 | `AsyncChannelService.ListChannelClients` — the **forward** view: evaluate the channel's policy against every Client in the realm via 17.3; paginate. Replaces the `UNIMPLEMENTED` stub from 10.8 | `003.5` | ✅ | 2026-09-13 |
| 17.6 | `ClientService.ListClientChannelAccess` — the **reverse** view: every channel whose policy grants this client anything, via 17.3; paginate | `003.5`, `003.10` | ✅ | 2026-09-13 |
| 17.7 | REST — `/v1/async-channels/{name}/clients` and `/v1/clients/{name}/channel-access` | proto | ✅ | 2026-09-13 |
| 17.8 | Integration tests — forward and reverse views **agree** for the same `(client, channel)` pair; a policy change is reflected in both; pagination | — | ✅ | 2026-09-13 |

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

### Decisions (asked, not assumed — see the tracker for the full Q&A)

- **`matched_by` (`003.5` OQ4)**: a DENY, once it wins, is never reported (the
  permission it decided is simply absent from `effective`); an ALLOW outcome
  lists *every* matching ALLOW statement, since none of them individually
  "won" over the others — e.g. `"READ: statement 0 (ALLOW labels=…), statement
  2 (ALLOW client_frn=…)"`.
- **`client_frn` glob matching**: against the client's prefix-less stored FRN
  (`FRN.Path()`, e.g. `acme:client:xpto-*`), never the API-rendered/prefixed
  form — consistent with every other FRN comparison in the schema
  (`resource_frn`, `indicator_sample`, `policy_action` are all stored and
  compared prefix-less).
- **Row inclusion**: both views return only rows with ≥1 effective
  permission. `003.10`'s own framing of the reverse view ("every channel whose
  policy matches this client") already implies this; a client/channel with no
  grant is simply absent, not shown with `effective: []`.
- **Selector-match cost (`003.5` OQ3)**: no special bound. Each view fetches
  one page of the underlying List (Clients for the forward view, Channels for
  the reverse), evaluates it in Go, and returns whatever matched — the same
  pattern `ClusterRepo.List` already uses for its 003.1 selector. A
  sparse-match page can come back with fewer rows than requested, or zero; the
  caller pages forward with `next_page_token` the same way it would past a
  selector that matched little.
- **Statement cap (`003.5` OQ2)**: still deferred, as it was in deliverable 10
  — nothing in 17's task list needs one to function correctly.
- **SDK enforcement point (`003.5` OQ1)**: confirmed out of scope for 17 — no
  task here builds a live enforcement path; it stays the client-auth ADR's to
  resolve, exactly as `003.5` already says.

### What landed

| Piece | Path |
|---|---|
| Evaluator | `core/domain/accesspolicy/evaluate.go` — `Evaluator` (compiles a `Policy`'s label selectors once, `NewEvaluator`), `Evaluate(clientFRN, labels) Evaluation` (`Effective []Permission`, `MatchedBy string`) |
| Ports | `core/ports/in/channel.go` (`ListChannelClientsInput`, `ChannelClientAccess` — `ClientFRN` typed `frn.FRN`, `ChannelClientAccessPage`), `core/ports/in/client.go` (`ListClientChannelAccessInput`, `ClientChannelAccess` — `AsyncChannel` a plain name string, `ClientChannelAccessPage`) |
| Usecases | `core/usecases/channels/service.go` — `ListChannelClients` (new `clients out.ClientRepository` dependency); `core/usecases/clients/service.go` — `ListClientChannelAccess` (new `channels out.AsyncChannelRepository` dependency). Both constructors' new params are wired for free by fx's DI graph (both repos were already provided) — no `main.go` call-site change needed, only the `fx.Annotate` function signatures |
| Handlers | `adapters/in/grpcgateway/asyncchannel.go` — `ListChannelClients` replaces the `UNIMPLEMENTED` stub; `adapters/in/grpcgateway/client.go` — `ListClientChannelAccess` added; shared `permissionsToProto` helper |
| Tests | `accesspolicy/evaluate_test.go` (17.4 — the worked example verbatim, zero-trust default, READ/WRITE independence, DENY-wins-regardless-of-order, both-criteria-is-OR, prefix-less-only glob matching, broad matrix); `grpcgateway/{asyncchannel,client}_test.go` (mapping + error-mapping); `adapters/out/postgres/accesspolicy_integration_test.go` (17.8 — forward/reverse agreement, a `SetAccessPolicy` change reflected in both views, pagination across a real keyset page) |
| Local seed | `local/seed/06-access-policy-demo.sql` — one Async Channel (`billing-events`) with a real policy against deliverable 16's seeded clients, deliberately asymmetric (one label-selector ALLOW + one client_frn ALLOW) so the views have something worth looking at |
| Doc follow-up | `docs/impl_plans/21-client-ui.md`'s "Channel access" panel moved from "deliberately absent" (blocked on this deliverable) into the real panel/task list, since the RPC now exists |
