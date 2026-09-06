import type { ProvisioningLabelSpec } from "./api/hooks";

// The reserved-label namespace an agent's recipe reads (003.1, ADR-006).
export const PROVISIONING_PREFIX = "franz.provisioning/";

// Shown when the selected provider agent advertises no schema of its own — keeps
// the cluster form usable and matches what the local-docker recipe understands.
export const FALLBACK_PROVISIONING_LABELS: ProvisioningLabelSpec[] = [
  {
    key: `${PROVISIONING_PREFIX}deployment-type`,
    description: "Selects the agent's recipe family.",
    allowedValues: [],
    defaultValue: "local-docker",
    required: false,
  },
  {
    key: `${PROVISIONING_PREFIX}kafka-version`,
    description: "Image tag when kafka-image is unset (apache/kafka:<version>).",
    allowedValues: [],
    defaultValue: "3.7.0",
    required: false,
  },
];

// shortKey drops the franz.provisioning/ prefix for display.
export function shortKey(key: string): string {
  return key.startsWith(PROVISIONING_PREFIX) ? key.slice(PROVISIONING_PREFIX.length) : key;
}

// specKeys returns the non-empty keys a spec list declares.
export function specKeys(specs: ProvisioningLabelSpec[]): string[] {
  return specs.map((s) => s.key ?? "").filter(Boolean);
}

// splitLabels partitions a label map into the keys a schema covers and the rest.
export function splitLabels(
  labels: Record<string, string>,
  specs: ProvisioningLabelSpec[],
): { schema: Record<string, string>; free: Record<string, string> } {
  const keys = new Set(specKeys(specs));
  const schema: Record<string, string> = {};
  const free: Record<string, string> = {};
  for (const [k, v] of Object.entries(labels)) {
    (keys.has(k) ? schema : free)[k] = v;
  }
  return { schema, free };
}

// prefilled returns the schema values seeded with each spec's default where the
// map has no value yet.
export function prefilled(
  current: Record<string, string>,
  specs: ProvisioningLabelSpec[],
): Record<string, string> {
  const out: Record<string, string> = { ...current };
  for (const s of specs) {
    const key = s.key ?? "";
    if (key && out[key] === undefined && s.defaultValue) out[key] = s.defaultValue;
  }
  return out;
}

// validateSchema mirrors the server's ValidateProvisioningLabels so a form can
// block a bad Save before the round-trip. Returns an error message or null.
export function validateSchema(specs: ProvisioningLabelSpec[]): string | null {
  const seen = new Set<string>();
  for (const s of specs) {
    const key = (s.key ?? "").trim();
    if (!key) return "A provisioning label has an empty key.";
    if (!key.startsWith("franz.")) return `Key "${key}" must be under the franz. namespace.`;
    if (seen.has(key)) return `Duplicate provisioning-label key "${key}".`;
    seen.add(key);
    const allowed = s.allowedValues ?? [];
    if (s.defaultValue && allowed.length > 0 && !allowed.includes(s.defaultValue)) {
      return `Default "${s.defaultValue}" for "${key}" is not one of its allowed values.`;
    }
  }
  return null;
}

// missingRequired lists the spec keys marked required that have no value.
export function missingRequired(
  values: Record<string, string>,
  specs: ProvisioningLabelSpec[],
): string[] {
  return specs
    .filter((s) => s.required && !!s.key && !(values[s.key] ?? "").trim())
    .map((s) => s.key as string);
}
