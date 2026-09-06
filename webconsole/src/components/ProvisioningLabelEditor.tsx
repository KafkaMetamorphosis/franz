import type { ProvisioningLabelSpec } from "../api/hooks";
import { PROVISIONING_PREFIX } from "../provisioning";

// ProvisioningLabelEditor edits an agent's advisory provisioning-label schema:
// repeatable rows of { key, description, allowed values, default, required }.
// Controlled — the parent owns the list.
export function ProvisioningLabelEditor({
  value,
  onChange,
}: {
  value: ProvisioningLabelSpec[];
  onChange: (next: ProvisioningLabelSpec[]) => void;
}) {
  const patch = (i: number, next: Partial<ProvisioningLabelSpec>) => {
    onChange(value.map((row, idx) => (idx === i ? { ...row, ...next } : row)));
  };
  const remove = (i: number) => onChange(value.filter((_, idx) => idx !== i));
  const add = () =>
    onChange([
      ...value,
      { key: PROVISIONING_PREFIX, description: "", allowedValues: [], defaultValue: "", required: false },
    ]);

  return (
    <div className="provisioning-schema-editor">
      {value.length === 0 ? (
        <p className="field-note">No provisioning labels advertised.</p>
      ) : (
        value.map((row, i) => (
          <fieldset className="provisioning-schema-row" key={i}>
            <div className="field">
              <label htmlFor={`ps-key-${i}`}>Key</label>
              <div>
                <input
                  id={`ps-key-${i}`}
                  value={row.key ?? ""}
                  placeholder={`${PROVISIONING_PREFIX}kafka-image`}
                  onChange={(e) => patch(i, { key: e.target.value })}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor={`ps-desc-${i}`}>Description</label>
              <div>
                <input
                  id={`ps-desc-${i}`}
                  value={row.description ?? ""}
                  onChange={(e) => patch(i, { description: e.target.value })}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor={`ps-allowed-${i}`}>
                Allowed values <small>Comma-separated. Empty = free text.</small>
              </label>
              <div>
                <input
                  id={`ps-allowed-${i}`}
                  value={(row.allowedValues ?? []).join(", ")}
                  onChange={(e) =>
                    patch(i, {
                      allowedValues: e.target.value
                        .split(",")
                        .map((s) => s.trim())
                        .filter(Boolean),
                    })
                  }
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor={`ps-default-${i}`}>Default value</label>
              <div>
                <input
                  id={`ps-default-${i}`}
                  value={row.defaultValue ?? ""}
                  onChange={(e) => patch(i, { defaultValue: e.target.value })}
                />
              </div>
            </div>
            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={row.required ?? false}
                onChange={(e) => patch(i, { required: e.target.checked })}
              />
              Required
            </label>
            <button type="button" className="button" onClick={() => remove(i)}>
              Remove label
            </button>
          </fieldset>
        ))
      )}
      <button type="button" className="button" onClick={add}>
        Add provisioning label
      </button>
    </div>
  );
}
