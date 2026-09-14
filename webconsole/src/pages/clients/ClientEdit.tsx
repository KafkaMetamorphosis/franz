import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, Loading, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { ApiError } from "../../api/client";
import { updateMask, useClient, useUpdateClient } from "../../api/hooks";

// Labels are the only editable field — `name` is immutable (003.10, also the
// default consumer-group prefix) and never appears in the mask.
export function ClientEdit() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useClient(name);
  const update = useUpdateClient(name);
  const client = data?.client;

  const baseLabels = useMemo(() => ({ ...(client?.labels ?? {}) }), [client]);
  const [draftLabels, setDraftLabels] = useState<Record<string, string> | null>(null);
  const labels = draftLabels ?? baseLabels;

  if (isLoading) return <Loading what="client" />;
  if (error || !client) {
    return (
      <>
        <ErrorBanner error={error} />
        <p className="empty-note">
          Client not found. <Link to="/clients">Back to Clients</Link>
        </p>
      </>
    );
  }

  const labelsChanged = JSON.stringify(labels) !== JSON.stringify(baseLabels);
  const conflict = update.error instanceof ApiError && update.error.status === 409;

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Clients", to: "/clients" },
          { label: name, to: `/clients/${name}` },
          { label: "Edit" },
        ]}
      />
      <PageHeading title={`Edit ${name}`} lead={client.frn} />
      <ErrorBanner error={update.error} />
      {conflict ? (
        <div className="app-error" role="alert">
          The client changed since you opened this form.{" "}
          <button
            type="button"
            className="button"
            onClick={() => {
              setDraftLabels(null);
              update.reset();
            }}
          >
            Reload and re-apply
          </button>
        </div>
      ) : null}

      <Panel title="Editable fields" note="Labels only. name and FRN are immutable — the name can never be reused.">
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            update.mutate(
              { updateMask: updateMask(["labels"]), labels },
              { onSuccess: () => navigate(`/clients/${name}`) },
            );
          }}
        >
          <div className="form-section">
            <h3>Immutable</h3>
            <dl className="detail-grid">
              <dt>Client name</dt>
              <dd>
                <code>{client.name}</code>
              </dd>
            </dl>
          </div>

          <div className="form-section">
            <h3>Labels</h3>
            <div className="field">
              <label>Labels</label>
              <div>
                <LabelEditor value={labels} onChange={setDraftLabels} />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to={`/clients/${name}`}>
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={!labelsChanged || update.isPending}>
              {update.isPending ? "Saving…" : "Save changes"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
