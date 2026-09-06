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
export type ProvisioningLabelSpec = Schemas["v1ProvisioningLabelSpec"];

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
    remove: useMutation({
      mutationFn: async () =>
        unwrap(await api.DELETE("/v1/kafka/clusters/{name}", { params: { path: { name } } })),
      onSuccess: invalidate,
    }),
  };
}
