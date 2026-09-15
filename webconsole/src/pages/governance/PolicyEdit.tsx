import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, Loading, PageHeading, Panel } from "../../components/ui";
import { ActionEditor } from "../../components/ActionEditor";
import { violationsByActionIndex } from "../../api/violations";
import { ApiError } from "../../api/client";
import { updateMask, usePolicy, useUpdatePolicy, type Action } from "../../api/hooks";
import { OPERATORS, entityLabel } from "../../api/enums";

// matcher / limit / actions / weight / enabled are maskable — `indicator` is
// immutable (changing it would reinterpret the limit's value against a
// different unit) and never appears in the mask or as an input.
export function PolicyEdit() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = usePolicy(name);
  const update = useUpdatePolicy(name);
  const policy = data?.policy;

  const baseSelector = policy?.matcher?.selector ?? "";
  const baseOperator = policy?.limit?.operator ?? OPERATORS[0].value;
  const baseLimitValue = policy?.limit?.value ?? "";
  const baseActions = useMemo(() => policy?.actions ?? [], [policy]);
  const baseWeight = policy?.weight ?? 0;
  const baseEnabled = policy?.enabled ?? true;

  const [selector, setSelector] = useState<string | null>(null);
  const [operator, setOperator] = useState<string | null>(null);
  const [limitValue, setLimitValue] = useState<string | null>(null);
  const [actions, setActions] = useState<Action[] | null>(null);
  const [weight, setWeight] = useState<string | null>(null);
  const [enabled, setEnabled] = useState<boolean | null>(null);

  if (isLoading) return <Loading what="policy" />;
  if (error || !policy) {
    return (
      <>
        <ErrorBanner error={error} />
        <p className="empty-note">
          Policy not found. <Link to="/governance/policies">Back to Policies</Link>
        </p>
      </>
    );
  }

  const currentSelector = selector ?? baseSelector;
  const currentOperator = operator ?? baseOperator;
  const currentLimitValue = limitValue ?? baseLimitValue;
  const currentActions = actions ?? baseActions;
  const currentWeight = weight ?? String(baseWeight);
  const currentEnabled = enabled ?? baseEnabled;

  const matcherChanged = currentSelector !== baseSelector;
  const limitChanged = currentOperator !== baseOperator || currentLimitValue !== baseLimitValue;
  const actionsChanged = JSON.stringify(currentActions) !== JSON.stringify(baseActions);
  const weightChanged = currentWeight !== String(baseWeight);
  const enabledChanged = currentEnabled !== baseEnabled;
  const changed = matcherChanged || limitChanged || actionsChanged || weightChanged || enabledChanged;

  const conflict = update.error instanceof ApiError && update.error.status === 409;
  const rowViolations =
    update.error instanceof ApiError ? violationsByActionIndex(update.error.fieldViolations) : {};

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Policies", to: "/governance/policies" },
          { label: name, to: `/governance/policies/${name}` },
          { label: "Edit" },
        ]}
      />
      <PageHeading title={`Edit ${name}`} lead={`Watches ${policy.indicator}`} />
      <ErrorBanner error={update.error} />
      {conflict ? (
        <div className="app-error" role="alert">
          The policy changed since you opened this form.{" "}
          <button
            type="button"
            className="button"
            onClick={() => {
              setSelector(null);
              setOperator(null);
              setLimitValue(null);
              setActions(null);
              setWeight(null);
              setEnabled(null);
              update.reset();
            }}
          >
            Reload and re-apply
          </button>
        </div>
      ) : null}

      <Panel title="Editable fields" note="Matcher, limit, actions, weight, enabled. The indicator is immutable.">
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            const paths: string[] = [];
            if (matcherChanged) paths.push("matcher");
            if (limitChanged) paths.push("limit");
            if (actionsChanged) paths.push("actions");
            if (weightChanged) paths.push("weight");
            if (enabledChanged) paths.push("enabled");
            update.mutate(
              {
                updateMask: updateMask(paths),
                matcher: { entity: policy.matcher?.entity, selector: currentSelector },
                limit: { operator: currentOperator as never, value: currentLimitValue },
                actions: currentActions,
                weight: Number(currentWeight) || 0,
                enabled: currentEnabled,
              },
              { onSuccess: () => navigate(`/governance/policies/${name}`) },
            );
          }}
        >
          <div className="form-section">
            <h3>Immutable</h3>
            <dl className="detail-grid">
              <dt>Indicator</dt>
              <dd>
                <code>{policy.indicator}</code>
              </dd>
              <dt>Entity</dt>
              <dd>{entityLabel(policy.matcher?.entity)}</dd>
            </dl>
          </div>

          <div className="form-section">
            <h3>Matcher</h3>
            <div className="field">
              <label htmlFor="policy-selector">Label selector</label>
              <div>
                <input
                  id="policy-selector"
                  placeholder="env=prod (empty = every resource)"
                  value={currentSelector}
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
                  value={currentOperator}
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
              <label htmlFor="policy-limit-value">Value</label>
              <div>
                <input
                  id="policy-limit-value"
                  required
                  value={currentLimitValue}
                  onChange={(event) => setLimitValue(event.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Actions</h3>
            <ActionEditor value={currentActions} onChange={setActions} violations={rowViolations} />
          </div>

          <div className="form-section">
            <h3>Scheduling</h3>
            <div className="field">
              <label htmlFor="policy-weight">Weight</label>
              <div>
                <input
                  id="policy-weight"
                  type="number"
                  step={1}
                  value={currentWeight}
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
                  checked={currentEnabled}
                  onChange={(event) => setEnabled(event.target.checked)}
                />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to={`/governance/policies/${name}`}>
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={!changed || update.isPending}>
              {update.isPending ? "Saving…" : "Save changes"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
