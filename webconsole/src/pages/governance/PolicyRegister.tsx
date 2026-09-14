import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, PageHeading, Panel } from "../../components/ui";
import { ActionEditor } from "../../components/ActionEditor";
import { violationsByActionIndex } from "../../api/violations";
import { ApiError } from "../../api/client";
import { useCreatePolicy, useIndicators, type Action } from "../../api/hooks";
import { OPERATORS, entityLabel } from "../../api/enums";

export function PolicyRegister() {
  const navigate = useNavigate();
  const createPolicy = useCreatePolicy();
  const { data: indicatorsData } = useIndicators();
  const indicators = indicatorsData?.indicators ?? [];

  const [name, setName] = useState("");
  const [indicatorName, setIndicatorName] = useState("");
  const [selector, setSelector] = useState("");
  const [operator, setOperator] = useState<string>(OPERATORS[0].value);
  const [limitValue, setLimitValue] = useState("");
  const [actions, setActions] = useState<Action[]>([]);
  const [weight, setWeight] = useState("0");
  const [enabled, setEnabled] = useState(true);
  const [localError, setLocalError] = useState<string | null>(null);

  const selectedIndicator = indicators.find((i) => i.name === indicatorName);
  const rowViolations =
    createPolicy.error instanceof ApiError ? violationsByActionIndex(createPolicy.error.fieldViolations) : {};

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Policies", to: "/governance/policies" },
          { label: "Register Policy" },
        ]}
      />
      <PageHeading
        title="Register Policy"
        lead="Watches one Indicator and runs its actions on every resource the matcher selects when the limit is crossed."
      />
      <ErrorBanner error={localError ?? createPolicy.error} />
      <Panel title="Policy details">
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            setLocalError(null);
            if (!selectedIndicator) {
              setLocalError("Pick an indicator.");
              return;
            }
            if (actions.length === 0) {
              setLocalError("A policy needs at least one action.");
              return;
            }
            createPolicy.mutate(
              {
                name: name.trim(),
                indicator: indicatorName,
                matcher: { entity: selectedIndicator.appliesTo, selector: selector.trim() },
                limit: { operator: operator as never, value: limitValue.trim() },
                actions,
                weight: Number(weight) || 0,
                enabled,
              },
              { onSuccess: (result) => navigate(`/governance/policies/${result.policy?.name ?? name.trim()}`) },
            );
          }}
        >
          <div className="form-section">
            <h3>Identity</h3>
            <div className="field">
              <label htmlFor="policy-name">
                Name <small>Unique. Immutable.</small>
              </label>
              <div>
                <input
                  id="policy-name"
                  required
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="policy-indicator">
                Indicator{" "}
                <small>Immutable once created — changing it would reinterpret the limit's value against a different unit.</small>
              </label>
              <div>
                <select
                  id="policy-indicator"
                  required
                  disabled={!!indicatorName}
                  value={indicatorName}
                  onChange={(event) => setIndicatorName(event.target.value)}
                >
                  <option value="" disabled>
                    Choose an indicator…
                  </option>
                  {indicators.map((indicator) => (
                    <option key={indicator.name} value={indicator.name}>
                      {indicator.name} ({indicator.unit})
                    </option>
                  ))}
                </select>
                {indicatorName ? (
                  <button type="button" className="button" onClick={() => setIndicatorName("")}>
                    Change
                  </button>
                ) : null}
                {indicators.length === 0 ? (
                  <p className="field-note">
                    No indicators registered yet. <Link to="/governance/indicators/register">Register one</Link>{" "}
                    first.
                  </p>
                ) : null}
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Matcher</h3>
            <p className="form-help">
              Entity is fixed to the indicator's applies-to
              {selectedIndicator ? ` (${entityLabel(selectedIndicator.appliesTo)})` : ""}. An empty selector
              matches every resource of that kind.
            </p>
            <div className="field">
              <label htmlFor="policy-selector">Label selector</label>
              <div>
                <input
                  id="policy-selector"
                  placeholder="env=prod (empty = every resource)"
                  value={selector}
                  onChange={(event) => setSelector(event.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Limit</h3>
            <div className="field">
              <label htmlFor="policy-operator">Operator</label>
              <div>
                <select
                  id="policy-operator"
                  value={operator}
                  onChange={(event) => setOperator(event.target.value)}
                >
                  {OPERATORS.map((op) => (
                    <option key={op.value} value={op.value}>
                      {op.label}
                    </option>
                  ))}
                </select>
              </div>
            </div>
            <div className="field">
              <label htmlFor="policy-limit-value">
                Value <small>Encoded per the indicator's unit, e.g. "150Gi", "3", "90d".</small>
              </label>
              <div>
                <input
                  id="policy-limit-value"
                  required
                  value={limitValue}
                  onChange={(event) => setLimitValue(event.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Actions</h3>
            <p className="form-help">
              What runs on every matched resource when the limit is crossed. Rejected against the write
              whitelist (003.8) at save time — violations render inline below.
            </p>
            <ActionEditor value={actions} onChange={setActions} violations={rowViolations} />
          </div>

          <div className="form-section">
            <h3>Scheduling</h3>
            <div className="field">
              <label htmlFor="policy-weight">
                Weight <small>Breaks ties when several policies act on the same resource. Higher wins.</small>
              </label>
              <div>
                <input
                  id="policy-weight"
                  type="number"
                  step={1}
                  value={weight}
                  onChange={(event) => setWeight(event.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="policy-enabled">Enabled</label>
              <div>
                <input
                  id="policy-enabled"
                  type="checkbox"
                  checked={enabled}
                  onChange={(event) => setEnabled(event.target.checked)}
                />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to="/governance/policies">
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={createPolicy.isPending}>
              {createPolicy.isPending ? "Registering…" : "Register Policy"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
