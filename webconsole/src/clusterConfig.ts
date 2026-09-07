// Helpers for the cluster-configuration form section.
//
// A Kafka Cluster's config lives in its `cluster_configuration` map (ADR-API-010).
// A Cluster Provider agent may advertise defaults as `franz.default-kafka-config/*`
// labels on its own `Agent.labels`; the console reads those to pre-fill a new
// cluster form. Advisory only — Franz enforces nothing.

import type { components } from "./api/schema";

type Agent = components["schemas"]["v1Agent"];

export const AGENT_DEFAULT_PREFIX = "franz.default-kafka-config/";

// Meta-keys inside the franz.default-kafka-config/* namespace that are not
// themselves config values.
const VERSIONS_KEY = "available-versions";
export const KAFKA_VERSION_KEY = "kafka-version";

// Shown when the selected provider agent advertises no defaults — keeps the form
// usable and matches what the local-docker recipe understands.
export const FALLBACK_CONFIG: Record<string, string> = {
  partitions: "3",
  "replication-factor": "1",
};
export const FALLBACK_VERSIONS = ["3.7.0", "3.9.0", "4.0.0"];

export interface AgentDefaults {
  // Scalar cluster_configuration defaults (excludes kafka-version + available-versions).
  config: Record<string, string>;
  // Options for the version <select>.
  versions: string[];
  // Pre-selected version.
  defaultVersion: string;
}

// defaultsFromAgent extracts the cluster-config defaults an agent advertises,
// falling back to the built-in local-docker set when it advertises none.
export function defaultsFromAgent(agent: Agent | undefined): AgentDefaults {
  const labels = agent?.labels ?? {};
  const config: Record<string, string> = {};
  let versions: string[] = [];
  let defaultVersion = "";

  for (const [key, value] of Object.entries(labels)) {
    if (!key.startsWith(AGENT_DEFAULT_PREFIX)) continue;
    const short = key.slice(AGENT_DEFAULT_PREFIX.length);
    if (short === VERSIONS_KEY) {
      versions = value.split(",").map((s) => s.trim()).filter(Boolean);
    } else if (short === KAFKA_VERSION_KEY) {
      defaultVersion = value;
    } else {
      config[short] = value;
    }
  }

  if (Object.keys(config).length === 0 && versions.length === 0 && !defaultVersion) {
    return {
      config: { ...FALLBACK_CONFIG },
      versions: FALLBACK_VERSIONS,
      defaultVersion: FALLBACK_VERSIONS[FALLBACK_VERSIONS.length - 1],
    };
  }
  if (versions.length === 0) versions = FALLBACK_VERSIONS;
  if (!defaultVersion) defaultVersion = versions[versions.length - 1];
  return { config, versions, defaultVersion };
}
