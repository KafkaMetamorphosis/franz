// Friendly labels + option lists for the proto enums the console exposes.
// The gateway speaks the proto3-JSON string form (e.g. "AGENT_TYPE_CUSTOM").

export const AGENT_TYPES = [
  { value: "AGENT_TYPE_CLUSTER_PROVIDER", label: "Cluster Provider" },
  { value: "AGENT_TYPE_RESOURCE_PROVIDER", label: "Resource Provider" },
  { value: "AGENT_TYPE_TELEMETRY_AGENT", label: "Telemetry Agent" },
  { value: "AGENT_TYPE_CUSTOM", label: "Custom" },
] as const;

export function agentTypeLabel(v?: string): string {
  return AGENT_TYPES.find((t) => t.value === v)?.label ?? "—";
}

export const CONNECTION_TYPES = [{ value: "CONNECTION_TYPE_PLAINTEXT", label: "PLAINTEXT" }] as const;

export function providerPhaseLabel(v?: string): string {
  return (v ?? "").replace(/^CLUSTER_PROVIDER_PHASE_/, "").replace(/_/g, " ") || "No report yet";
}
