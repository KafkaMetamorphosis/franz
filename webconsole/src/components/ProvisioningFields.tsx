import type { ProvisioningLabelSpec } from "../api/hooks";
import { shortKey } from "../provisioning";

// ProvisioningFields renders one control per spec the selected provider agent
// advertises: a dropdown when allowed_values is set, otherwise free text.
// Controlled — the parent owns the value map (keyed by the full label key).
export function ProvisioningFields({
  specs,
  value,
  onChange,
}: {
  specs: ProvisioningLabelSpec[];
  value: Record<string, string>;
  onChange: (next: Record<string, string>) => void;
}) {
  if (specs.length === 0) return null;

  const set = (key: string, v: string) => {
    const next = { ...value };
    if (v === "") delete next[key];
    else next[key] = v;
    onChange(next);
  };

  return (
    <div className="provisioning-fields">
      {specs.map((spec) => {
        const key = spec.key ?? "";
        const id = `prov-${key}`;
        const current = value[key] ?? spec.defaultValue ?? "";
        const options = spec.allowedValues ?? [];
        return (
          <div className="field" key={key}>
            <label htmlFor={id}>
              <code>{shortKey(key)}</code>
              {spec.required ? <span className="req" aria-hidden="true"> *</span> : null}
              {spec.description ? <small>{spec.description}</small> : null}
            </label>
            <div>
              {options.length > 0 ? (
                <select
                  id={id}
                  value={current}
                  required={spec.required}
                  onChange={(e) => set(key, e.target.value)}
                >
                  {!spec.required && <option value="">— unset —</option>}
                  {options.map((o) => (
                    <option key={o} value={o}>
                      {o}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  id={id}
                  value={current}
                  required={spec.required}
                  aria-required={spec.required}
                  onChange={(e) => set(key, e.target.value)}
                />
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
