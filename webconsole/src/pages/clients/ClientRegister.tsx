import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { useCreateClient } from "../../api/hooks";

export function ClientRegister() {
  const navigate = useNavigate();
  const createClient = useCreateClient();

  const [name, setName] = useState("");
  const [labels, setLabels] = useState<Record<string, string>>({});

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Clients", to: "/clients" },
          { label: "Register Client" },
        ]}
      />
      <PageHeading
        title="Register Client"
        lead="Also the default consumer-group prefix — choose a name your SDK integration is stable under."
      />
      <ErrorBanner error={createClient.error} />
      <Panel title="Client details">
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            createClient.mutate(
              { name: name.trim(), labels },
              { onSuccess: (result) => navigate(`/clients/${result.client?.name ?? name.trim()}`) },
            );
          }}
        >
          <div className="form-section">
            <h3>Identity</h3>
            <div className="field">
              <label htmlFor="client-name">
                Client name{" "}
                <small>
                  Globally unique, provided at registration. Immutable — deleting a client does not free
                  the name for reuse (003.10).
                </small>
              </label>
              <div>
                <input
                  id="client-name"
                  required
                  placeholder="for example, billing"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Labels</h3>
            <p className="form-help">
              Channel access policies match against these labels — at least <code>org.com/owner</code> is
              recommended, though not enforced.
            </p>
            <div className="field">
              <label>Labels</label>
              <div>
                <LabelEditor value={labels} onChange={setLabels} />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to="/clients">
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={createClient.isPending}>
              {createClient.isPending ? "Registering…" : "Register Client"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
