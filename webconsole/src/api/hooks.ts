import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseQueryOptions,
} from "@tanstack/react-query";
import { api, toApiError } from "./client";
import type { components } from "./schema";

type Schemas = components["schemas"];
export type Agent = Schemas["v1Agent"];
export type KafkaCluster = Schemas["v1KafkaCluster"];
export type ClusterProviderEvent = Schemas["v1ClusterProviderEvent"];
export type ConnectionString = Schemas["v1ConnectionString"];
export type AgentType = Schemas["v1AgentType"];
export type AsyncChannel = Schemas["v1AsyncChannel"];
export type ChannelType = Schemas["v1ChannelType"];
export type ChannelState = Schemas["v1ChannelState"];
export type Indicator = Schemas["v1Indicator"];
export type IndicatorSampleView = Schemas["v1IndicatorSampleView"];
export type Policy = Schemas["v1Policy"];
export type PolicyAction = Schemas["v1PolicyAction"];
export type Action = Schemas["v1Action"];
export type Matcher = Schemas["v1Matcher"];
export type Limit = Schemas["v1Limit"];
export type Client = Schemas["v1Client"];
export type ClientChannelAccess = Schemas["v1ClientChannelAccess"];
export type ObservedConsumerGroup = Schemas["v1ObservedConsumerGroup"];
export type ShardMigration = Schemas["v1ShardMigration"];

// The gateway parses `update_mask` with protojson semantics: comma-separated
// lowerCamelCase paths (snake_case is rejected). Callers pass the body keys they
// changed.
export function updateMask(paths: string[]): string {
  return paths.join(",");
}

function unwrap<T>(result: { data?: T; error?: unknown; response: Response }): T {
  if (result.error !== undefined || !result.response.ok) {
    throw toApiError(result.response.status, result.error);
  }
  return result.data as T;
}

// --- Agents -----------------------------------------------------------------

export function useAgents(type?: AgentType) {
  return useQuery({
    queryKey: ["agents", type ?? "all"],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/kafka/agents", {
          params: { query: type ? { type } : {} },
        }),
      ),
  });
}

export function useAgent(name: string, options?: Partial<UseQueryOptions<Schemas["v1GetAgentResponse"]>>) {
  return useQuery({
    queryKey: ["agent", name],
    queryFn: async () => unwrap(await api.GET("/v1/kafka/agents/{name}", { params: { path: { name } } })),
    ...options,
  });
}

export function useCreateAgent() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["v1CreateAgentRequest"]) =>
      unwrap(await api.POST("/v1/kafka/agents", { body })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["agents"] }),
  });
}

// UpdateAgentBody is the PATCH payload; callers set only the changed fields plus
// updateMask (built with updateMask()).
export type UpdateAgentBody = Schemas["AgentServiceUpdateAgentBody"];

export function useUpdateAgent(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdateAgentBody) =>
      unwrap(await api.PATCH("/v1/kafka/agents/{name}", { params: { path: { name } }, body })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["agent", name] });
      qc.invalidateQueries({ queryKey: ["agents"] });
    },
  });
}

export function useRotateAgentToken(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async () =>
      unwrap(await api.POST("/v1/kafka/agents/{name}:rotateToken", { params: { path: { name } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["agent", name] }),
  });
}

export function useAgentLifecycle(name: string) {
  const qc = useQueryClient();
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["agent", name] });
    qc.invalidateQueries({ queryKey: ["agents"] });
  };
  return {
    pause: useMutation({
      mutationFn: async () =>
        unwrap(await api.POST("/v1/kafka/agents/{name}:pause", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
    resume: useMutation({
      mutationFn: async () =>
        unwrap(await api.POST("/v1/kafka/agents/{name}:resume", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
    remove: useMutation({
      mutationFn: async () =>
        unwrap(await api.DELETE("/v1/kafka/agents/{name}", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
  };
}

// --- Kafka Clusters -------------------------------------------------------

export function useClusters() {
  return useQuery({
    queryKey: ["clusters"],
    queryFn: async () => unwrap(await api.GET("/v1/kafka/clusters", { params: { query: {} } })),
  });
}

export function useCluster(name: string, opts?: { pollMs?: number }) {
  return useQuery({
    queryKey: ["cluster", name],
    queryFn: async () =>
      unwrap(await api.GET("/v1/kafka/clusters/{name}", { params: { path: { name } } })),
    refetchInterval: opts?.pollMs,
  });
}

export function useClusterProviderEvents(name: string, opts?: { pollMs?: number }) {
  return useQuery({
    queryKey: ["cluster-events", name],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/kafka/clusters/{name}/provider-events", {
          params: { path: { name }, query: {} },
        }),
      ),
    refetchInterval: opts?.pollMs,
  });
}

export function useCreateCluster() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["v1CreateKafkaClusterRequest"]) =>
      unwrap(await api.POST("/v1/kafka/clusters", { body })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clusters"] }),
  });
}

export type UpdateKafkaClusterBody = Schemas["KafkaClusterServiceUpdateKafkaClusterBody"];

export function useUpdateKafkaCluster(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdateKafkaClusterBody) =>
      unwrap(await api.PATCH("/v1/kafka/clusters/{name}", { params: { path: { name } }, body })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["cluster", name] });
      qc.invalidateQueries({ queryKey: ["clusters"] });
    },
  });
}

export function useClusterLifecycle(name: string) {
  const qc = useQueryClient();
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["cluster", name] });
    qc.invalidateQueries({ queryKey: ["clusters"] });
  };
  return {
    pause: useMutation({
      mutationFn: async () =>
        unwrap(await api.POST("/v1/kafka/clusters/{name}:pause", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
    resume: useMutation({
      mutationFn: async () =>
        unwrap(await api.POST("/v1/kafka/clusters/{name}:resume", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
    // force defaults to false: a plain Delete on a cluster with live shards
    // is rejected (FAILED_PRECONDITION, "...pass force=true"); the caller
    // re-invokes with { force: true } to auto-start a drain instead (003.13
    // OQ5) — the cluster is not deleted immediately in that case, only once
    // every shard has migrated off.
    remove: useMutation({
      mutationFn: async (vars?: { force?: boolean }) =>
        unwrap(
          await api.DELETE("/v1/kafka/clusters/{name}", {
            params: { path: { name }, query: vars?.force ? { force: true } : {} },
          }),
        ),
      onSuccess: invalidate,
    }),
  };
}

// --- Async Channels ---------------------------------------------------------

export function useChannels(selector?: string) {
  return useQuery({
    queryKey: ["channels", selector ?? "all"],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/async-channels", {
          params: { query: selector ? { selector } : {} },
        }),
      ),
  });
}

export function useChannel(name: string) {
  return useQuery({
    queryKey: ["channel", name],
    queryFn: async () =>
      unwrap(await api.GET("/v1/async-channels/{name}", { params: { path: { name } } })),
  });
}

// The async-channel shards placement has materialised for a channel — each is
// one real Kafka Topic. Empty until a cluster matches the channel's
// `franz.affinity/selector` (deliverable 13, ADR-API-009).
export function useChannelTopics(asyncChannel: string, opts?: { pollMs?: number }) {
  return useQuery({
    queryKey: ["channel-topics", asyncChannel],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/kafka/topics", {
          params: { query: { asyncChannel } },
        }),
      ),
    refetchInterval: opts?.pollMs,
  });
}

export function useCreateChannel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["v1CreateAsyncChannelRequest"]) =>
      unwrap(await api.POST("/v1/async-channels", { body })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["channels"] }),
  });
}

// UpdateAsyncChannelBody is the PATCH payload. Only `labels` is maskable —
// `type` is immutable, `channel_partitions` is a staged re-shard (003.11), and
// the access policy is set through SetAccessPolicy (deliverable 17).
export type UpdateAsyncChannelBody = Schemas["AsyncChannelServiceUpdateAsyncChannelBody"];

export function useUpdateChannel(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdateAsyncChannelBody) =>
      unwrap(await api.PATCH("/v1/async-channels/{name}", { params: { path: { name } }, body })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["channel", name] });
      qc.invalidateQueries({ queryKey: ["channels"] });
    },
  });
}

export function useChannelLifecycle(name: string) {
  const qc = useQueryClient();
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["channel", name] });
    qc.invalidateQueries({ queryKey: ["channels"] });
  };
  return {
    pause: useMutation({
      mutationFn: async () =>
        unwrap(await api.POST("/v1/async-channels/{name}:pause", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
    resume: useMutation({
      mutationFn: async () =>
        unwrap(await api.POST("/v1/async-channels/{name}:resume", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
    remove: useMutation({
      mutationFn: async () =>
        unwrap(await api.DELETE("/v1/async-channels/{name}", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
  };
}

// --- Governance: Indicators --------------------------------------------------

export function useIndicators() {
  return useQuery({
    queryKey: ["indicators"],
    queryFn: async () =>
      unwrap(await api.GET("/v1/governance/indicators", { params: { query: {} } })),
  });
}

export function useIndicator(name: string) {
  return useQuery({
    queryKey: ["indicator", name],
    queryFn: async () =>
      unwrap(await api.GET("/v1/governance/indicators/{name}", { params: { path: { name } } })),
  });
}

// No resourceFrn filter: the detail page's "recent samples" table shows the
// indicator's most recent activity across every resource it applies to.
// IndicatorSamplesQuery narrows the sample history a caller wants.
//
// `from` is what makes a chart possible: the series is 30-day-retained and the
// default page is 50 rows *across every resource the indicator covers*, so an
// unbounded request gives a few minutes of history for a multi-resource
// indicator. A visualisation has to say how far back it wants and raise the page
// size to match.
export type IndicatorSamplesQuery = {
  /** ISO-8601 lower bound. Omit for "as far back as one page reaches". */
  from?: string;
  /** Restrict to one resource — omit to get every resource, one series each. */
  resourceFrn?: string;
  pageSize?: number;
};

export function useIndicatorSamples(indicator: string, query: IndicatorSamplesQuery = {}) {
  const { from, resourceFrn, pageSize } = query;
  return useQuery({
    // from/resourceFrn/pageSize are part of the key: changing the range must
    // refetch rather than reuse a narrower window's cache.
    queryKey: ["indicator-samples", indicator, from ?? null, resourceFrn ?? null, pageSize ?? null],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/governance/indicators/{indicator}/samples", {
          params: {
            path: { indicator },
            query: {
              ...(from ? { from } : {}),
              ...(resourceFrn ? { resourceFrn } : {}),
              ...(pageSize ? { "page.pageSize": pageSize } : {}),
            },
          },
        }),
      ),
    enabled: !!indicator,
  });
}

export function useCreateIndicator() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["v1CreateIndicatorRequest"]) =>
      unwrap(await api.POST("/v1/governance/indicators", { body })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["indicators"] }),
  });
}

// UpdateIndicatorBody is the PATCH payload — unit / staleness_threshold /
// source_agents only. `applies_to` is immutable (003.14).
export type UpdateIndicatorBody = Schemas["GovernanceServiceUpdateIndicatorBody"];

export function useUpdateIndicator(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdateIndicatorBody) =>
      unwrap(await api.PATCH("/v1/governance/indicators/{name}", { params: { path: { name } }, body })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["indicator", name] });
      qc.invalidateQueries({ queryKey: ["indicators"] });
    },
  });
}

export function useDeleteIndicator() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (name: string) =>
      unwrap(await api.DELETE("/v1/governance/indicators/{name}", { params: { path: { name } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["indicators"] }),
  });
}

// --- Governance: Policies ----------------------------------------------------

export function usePolicies() {
  return useQuery({
    queryKey: ["policies"],
    queryFn: async () => unwrap(await api.GET("/v1/governance/policies", { params: { query: {} } })),
  });
}

export function usePolicy(name: string) {
  return useQuery({
    queryKey: ["policy", name],
    queryFn: async () =>
      unwrap(await api.GET("/v1/governance/policies/{name}", { params: { path: { name } } })),
  });
}

export function usePolicyActions(name: string) {
  return useQuery({
    queryKey: ["policy-actions", name],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/governance/policies/{name}/actions", {
          params: { path: { name }, query: {} },
        }),
      ),
    enabled: !!name,
  });
}

export function useCreatePolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["v1CreatePolicyRequest"]) =>
      unwrap(await api.POST("/v1/governance/policies", { body })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["policies"] }),
  });
}

// UpdatePolicyBody is the PATCH payload — matcher / limit / actions / weight /
// enabled. `indicator` is immutable: changing it would silently reinterpret an
// existing Limit's value against a different unit.
export type UpdatePolicyBody = Schemas["GovernanceServiceUpdatePolicyBody"];

export function useUpdatePolicy(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdatePolicyBody) =>
      unwrap(await api.PATCH("/v1/governance/policies/{name}", { params: { path: { name } }, body })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["policy", name] });
      qc.invalidateQueries({ queryKey: ["policies"] });
    },
  });
}

export function useDeletePolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (name: string) =>
      unwrap(await api.DELETE("/v1/governance/policies/{name}", { params: { path: { name } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["policies"] }),
  });
}

// DryRunPolicy evaluates a definition (matcher/limit/actions/indicator) against
// the latest real sample per matched resource — no resourceFrn/hypothetical
// value input on the wire, and no mutation. Not tied to a query key: it is a
// mutation-shaped read, invoked on demand from a button.
export function useDryRunPolicy() {
  return useMutation({
    mutationFn: async (body: Schemas["v1DryRunPolicyRequest"]) =>
      unwrap(await api.POST("/v1/governance/policies:dryRun", { body })),
  });
}

// --- Clients (003.10) --------------------------------------------------------

export function useClients(selector?: string) {
  return useQuery({
    queryKey: ["clients", selector ?? "all"],
    queryFn: async () =>
      unwrap(await api.GET("/v1/clients", { params: { query: selector ? { selector } : {} } })),
  });
}

export function useClient(name: string) {
  return useQuery({
    queryKey: ["client", name],
    queryFn: async () => unwrap(await api.GET("/v1/clients/{name}", { params: { path: { name } } })),
  });
}

export function useClientChannelAccess(name: string) {
  return useQuery({
    queryKey: ["client-channel-access", name],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/clients/{name}/channel-access", {
          params: { path: { name }, query: {} },
        }),
      ),
    enabled: !!name,
  });
}

export function useObservedConsumerGroups(name: string) {
  return useQuery({
    queryKey: ["observed-consumer-groups", name],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/clients/{name}/consumer-groups", {
          params: { path: { name }, query: {} },
        }),
      ),
    enabled: !!name,
  });
}

// No (group, topic) filter on the wire — ListConsumerGroupObservations returns
// every sighting for the client in the time range. The caller filters
// client-side to the one (group, topic) pair a "show history" row expands.
export function useConsumerGroupObservations(name: string, opts?: { enabled?: boolean }) {
  return useQuery({
    queryKey: ["consumer-group-observations", name],
    queryFn: async () =>
      unwrap(
        await api.GET("/v1/clients/{name}/consumer-group-observations", {
          params: { path: { name }, query: {} },
        }),
      ),
    enabled: !!name && (opts?.enabled ?? true),
  });
}

export function useCreateClient() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["v1CreateClientRequest"]) =>
      unwrap(await api.POST("/v1/clients", { body })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clients"] }),
  });
}

// UpdateClientBody is the PATCH payload — `labels` only. `name` is immutable
// (003.10, "the default consumer-group prefix").
export type UpdateClientBody = Schemas["ClientServiceUpdateClientBody"];

export function useUpdateClient(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdateClientBody) =>
      unwrap(await api.PATCH("/v1/clients/{name}", { params: { path: { name } }, body })),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["client", name] });
      qc.invalidateQueries({ queryKey: ["clients"] });
    },
  });
}

export function useDeleteClient() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (name: string) =>
      unwrap(await api.DELETE("/v1/clients/{name}", { params: { path: { name } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clients"] }),
  });
}

// --- Migration (003.13) ------------------------------------------------------

// asyncChannel is required in practice, not just in name: ListShardMigrations
// resolves it to a channel row and rejects an empty/unknown one with
// NotFound — there is no "list every migration" or "filter by cluster" query
// shape on this RPC (found live, not assumed; ClusterDetail has its own note
// on why it has no Migrations panel as a result).
export function useShardMigrations(asyncChannel: string, opts?: { pollMs?: number }) {
  return useQuery({
    queryKey: ["shard-migrations", asyncChannel],
    queryFn: async () =>
      unwrap(await api.GET("/v1/shard-migrations", { params: { query: { asyncChannel } } })),
    enabled: !!asyncChannel,
    refetchInterval: opts?.pollMs,
  });
}

export function useShardMigration(id: string) {
  return useQuery({
    queryKey: ["shard-migration", id],
    queryFn: async () =>
      unwrap(await api.GET("/v1/shard-migrations/{id}", { params: { path: { id } } })),
    enabled: !!id,
  });
}

// kafkaTopic is a mutate-time variable, not a hook argument: a shards table
// (ChannelDetail) triggers this per-row from one shared mutation instance,
// where a name-bound hook (mirroring useUpdateChannel(name)'s pattern) would
// mean calling a hook inside a loop.
export function useMigrateKafkaTopic() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({
      kafkaTopic,
      ...body
    }: { kafkaTopic: string } & Schemas["MigrationServiceMigrateKafkaTopicBody"]) =>
      unwrap(await api.POST("/v1/kafka-topics/{kafkaTopic}/migrate", { params: { path: { kafkaTopic } }, body })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shard-migrations"] }),
  });
}

export function useMigrateCluster(kafkaCluster: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["MigrationServiceMigrateClusterBody"]) =>
      unwrap(
        await api.POST("/v1/kafka-clusters/{kafkaCluster}/migrate", { params: { path: { kafkaCluster } }, body }),
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shard-migrations"] }),
  });
}
