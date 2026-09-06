import { useState } from "react";

// A key/value label builder — ported from register-kafka-cluster.html's inline
// script. Controlled: the parent owns the label map.
export function LabelEditor({
  value,
  onChange,
  keyPlaceholder = "Label key",
  valuePlaceholder = "Label value",
}: {
  value: Record<string, string>;
  onChange: (next: Record<string, string>) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
}) {
  const [k, setK] = useState("");
  const [v, setV] = useState("");

  const add = () => {
    const key = k.trim();
    const val = v.trim();
    if (!key || !val) return;
    onChange({ ...value, [key]: val });
    setK("");
    setV("");
  };

  const remove = (key: string) => {
    const next = { ...value };
    delete next[key];
    onChange(next);
  };

  const entries = Object.entries(value);

  return (
    <div className="label-builder" aria-label="Labels">
      <div className="label-builder-controls">
        <input
          aria-label="Label key"
          placeholder={keyPlaceholder}
          value={k}
          onChange={(e) => setK(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && (e.preventDefault(), add())}
        />
        <input
          aria-label="Label value"
          placeholder={valuePlaceholder}
          value={v}
          onChange={(e) => setV(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && (e.preventDefault(), add())}
        />
        <button type="button" className="button" onClick={add}>
          Add label
        </button>
      </div>
      <div className="label-tags" aria-live="polite">
        {entries.length === 0 ? (
          <span className="field-note">No labels yet.</span>
        ) : (
          entries.map(([key, val]) => (
            <span className="tag editable-tag" key={key}>
              {key}={val}
              <button
                type="button"
                className="tag-remove"
                aria-label={`Remove ${key}=${val}`}
                onClick={() => remove(key)}
              >
                ×
              </button>
            </span>
          ))
        )}
      </div>
    </div>
  );
}
