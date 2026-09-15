import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, PageHeading, Panel } from "../../components/ui";
import { useCreateIndicator } from "../../api/hooks";
import { ENTITY_TYPES } from "../../api/enums";

// A repeatable text list for source_agents — simpler than LabelEditor's
// key/value shape since an agent name has no associated value.
function AgentListEditor({ value, onChange }: { value: string[]; onChange: (next: string[]) => void }) {
  const [draft, setDraft] = useState("");
  const add = () => {
    const name = draft.trim();
    if (!name || value.includes(name)) return;
    onChange([...value, name]);
    setDraft("");
  };
  return (
    <div>
      <div className="label-builder-controls">
        <input
          aria-label="Source agent"
          placeholder="agent name"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && (e.preventDefault(), add())}
        />
        <button type="button" className="button" onClick={add}>
          Add agent
        </button>
      </div>
      <div className="label-tags" aria-live="polite">
        {value.length === 0 ? (
          <span className="field-note">Any agent may publish (no restriction).</span>
        ) : (
          value.map((agent) => (
            <span className="tag editable-tag" key={agent}>
              {agent}
              <button
                type="button"
                className="tag-remove"
                aria-label={`Remove ${agent}`}
                onClick={() => onChange(value.filter((a) => a !== agent))}
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

export function IndicatorRegister() {
  const navigate = useNavigate();
  const createIndicator = useCreateIndicator();

  const [name, setName] = useState("");
  const [unit, setUnit] = useState("");
  const [appliesTo, setAppliesTo] = useState<string>(ENTITY_TYPES[0].value);
  const [stalenessThreshold, setStalenessThreshold] = useState("5m");
  const [sourceAgents, setSourceAgents] = useState<string[]>([]);

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Indicators", to: "/governance/indicators" },
          { label: "Register Indicator" },
        ]}
      />
      <PageHeading
        title="Register Indicator"
        lead="A sample or a Policy against an unregistered indicator is rejected — register it here first."
      />
      <ErrorBanner error={createIndicator.error} />
      <Panel title="Indicator details">
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            createIndicator.mutate(
              {
                name: name.trim(),
                unit: unit.trim(),
                appliesTo: appliesTo as never,
                stalenessThreshold: stalenessThreshold.trim(),
                sourceAgents,
              },
              {
                onSuccess: (result) =>
                  navigate(`/governance/indicators/${result.indicator?.name ?? name.trim()}`),
              },
            );
          }}
        >
          <div className="form-section">
            <h3>Identity</h3>
            <div className="field">
              <label htmlFor="indicator-name">
                Name <small>Unique. Immutable.</small>
              </label>
              <div>
                <input
                  id="indicator-name"
                  required
                  placeholder="for example, kafka.topic.disk_used"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="indicator-unit">
                Unit <small>bytes, count, duration, boolean, percent, or a string/enum family.</small>
              </label>
              <div>
                <input
                  id="indicator-unit"
                  required
                  placeholder="bytes"
                  value={unit}
                  onChange={(event) => setUnit(event.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="indicator-applies-to">
                Applies to <small>Immutable once registered (003.14).</small>
              </label>
              <div>
                <select
                  id="indicator-applies-to"
                  value={appliesTo}
                  onChange={(event) => setAppliesTo(event.target.value)}
                >
                  {ENTITY_TYPES.map((entity) => (
                    <option key={entity.value} value={entity.value}>
                      {entity.label}
                    </option>
                  ))}
                </select>
              </div>
            </div>
            <div className="field">
              <label htmlFor="indicator-staleness">
                Staleness threshold <small>Duration spec, e.g. "5m". Beyond this age policies do not act.</small>
              </label>
              <div>
                <input
                  id="indicator-staleness"
                  required
                  placeholder="5m"
                  value={stalenessThreshold}
                  onChange={(event) => setStalenessThreshold(event.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Source agents</h3>
            <p className="form-help">Optional. Restricts which agents may publish samples for this indicator.</p>
            <div className="field">
              <label>Source agents</label>
              <div>
                <AgentListEditor value={sourceAgents} onChange={setSourceAgents} />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to="/governance/indicators">
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={createIndicator.isPending}>
              {createIndicator.isPending ? "Registering…" : "Register Indicator"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
