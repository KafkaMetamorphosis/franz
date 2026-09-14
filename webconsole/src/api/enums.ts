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

// CHANNEL_TYPE_KAFKA_TOPIC is the only type Franz supports today (003.4), so the
// console renders it as the sole, fixed option rather than a real choice.
export const CHANNEL_TYPES = [
  { value: "CHANNEL_TYPE_KAFKA_TOPIC", label: "Kafka topic" },
] as const;

export function channelTypeLabel(v?: string): string {
  return CHANNEL_TYPES.find((t) => t.value === v)?.label ?? "—";
}

export function channelStateLabel(v?: string): string {
  const short = (v ?? "").replace(/^CHANNEL_STATE_/, "").replace(/_/g, " ");
  if (!short || short === "UNSPECIFIED") return "Unspecified";
  return short.charAt(0) + short.slice(1).toLowerCase();
}

// --- Governance (003.8 / 003.14) --------------------------------------------

export const ENTITY_TYPES = [
  { value: "ENTITY_ASYNC_CHANNEL", label: "Async Channel" },
  { value: "ENTITY_KAFKA_TOPIC", label: "Kafka Topic" },
  { value: "ENTITY_KAFKA_CLUSTER", label: "Kafka Cluster" },
] as const;

export function entityLabel(v?: string): string {
  return ENTITY_TYPES.find((t) => t.value === v)?.label ?? "—";
}

export function healthLabel(v?: string): string {
  const short = (v ?? "").replace(/^INDICATOR_HEALTH_/, "");
  if (!short || short === "UNSPECIFIED") return "No samples yet";
  return short.charAt(0) + short.slice(1).toLowerCase();
}

export const OPERATORS = [
  { value: "OPERATOR_LESS_THAN", label: "<" },
  { value: "OPERATOR_LESS_THAN_OR_EQUAL", label: "<=" },
  { value: "OPERATOR_EQUAL", label: "==" },
  { value: "OPERATOR_NOT_EQUAL", label: "!=" },
  { value: "OPERATOR_GREATER_THAN_OR_EQUAL", label: ">=" },
  { value: "OPERATOR_GREATER_THAN", label: ">" },
] as const;

export function operatorLabel(v?: string): string {
  return OPERATORS.find((o) => o.value === v)?.label ?? "?";
}

// GOVERNABLE_STATUSES is SET_STATUS's value set (003.8): plain state names, not
// the proto-prefixed enum form (e.g. "PAUSED", not "CHANNEL_STATE_PAUSED") —
// Channel and Cluster share the same three names (governance/whitelist.go's
// isGovernableStatus).
export const GOVERNABLE_STATUSES = ["ACTIVE", "PAUSED", "DELETED"] as const;

export const ACTION_KINDS = [
  { value: "ACTION_KIND_ADD_LABEL", label: "Add label", argCount: 2 },
  { value: "ACTION_KIND_REMOVE_LABEL", label: "Remove label", argCount: 1 },
  { value: "ACTION_KIND_SET_STATUS", label: "Set status", argCount: 1 },
  { value: "ACTION_KIND_UPDATE_FIELD", label: "Update field", argCount: 2 },
  { value: "ACTION_KIND_INCREASE_FIELD_BY", label: "Increase field by", argCount: 2 },
  { value: "ACTION_KIND_DECREASE_FIELD_BY", label: "Decrease field by", argCount: 2 },
] as const;

export function actionKindLabel(v?: string): string {
  return ACTION_KINDS.find((k) => k.value === v)?.label ?? "?";
}

// describeAction renders one Action as a single-line summary for a read-only
// list (PolicyList's action count, PolicyDetail's audit trail) — the args'
// meaning depends on kind (see v1Action's own doc comment).
export function describeAction(action?: { kind?: string; args?: string[] }): string {
  if (!action?.kind) return "—";
  const args = action.args ?? [];
  switch (action.kind) {
    case "ACTION_KIND_ADD_LABEL":
      return `add label ${args[0] ?? "?"}=${args[1] ?? "?"}`;
    case "ACTION_KIND_REMOVE_LABEL":
      return `remove label ${args[0] ?? "?"}`;
    case "ACTION_KIND_SET_STATUS":
      return `set status ${args[0] ?? "?"}`;
    case "ACTION_KIND_UPDATE_FIELD":
      return `set ${args[0] ?? "?"} = ${args[1] ?? "?"}`;
    case "ACTION_KIND_INCREASE_FIELD_BY":
      return `increase ${args[0] ?? "?"} by ${args[1] ?? "?"}${args[2] ? ` (${args[2]})` : ""}`;
    case "ACTION_KIND_DECREASE_FIELD_BY":
      return `decrease ${args[0] ?? "?"} by ${args[1] ?? "?"}${args[2] ? ` (${args[2]})` : ""}`;
    default:
      return actionKindLabel(action.kind);
  }
}
