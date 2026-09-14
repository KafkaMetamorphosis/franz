import { Link } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading } from "../../components/ui";
import { useClients } from "../../api/hooks";

const OWNER_LABEL = "org.com/owner";

export function ClientList() {
  const { data, isLoading, error } = useClients();
  const clients = data?.clients ?? [];

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console", to: "/" }, { label: "Clients" }]} />
      <PageHeading
        title="Clients"
        lead="The fleet-wide SDK identity. A Client has no Read/Write role of its own — every permission comes from the access policy of the channel it connects to."
        actions={
          <Link className="button primary" to="/clients/register">
            Register Client
          </Link>
        }
      />
      <ErrorBanner error={error} />
      <section className="panel">
        <div className="toolbar">
          <span className="panel-note">
            {clients.length} client{clients.length === 1 ? "" : "s"}
          </span>
        </div>
        {isLoading ? (
          <Loading what="clients" />
        ) : clients.length === 0 ? (
          <Empty>
            No Clients yet. <Link to="/clients/register">Register one</Link>.
          </Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Client name</th>
                  <th>Owner</th>
                  <th>Labels</th>
                </tr>
              </thead>
              <tbody>
                {clients.map((client) => (
                  <tr key={client.name}>
                    <td>
                      <Link to={`/clients/${client.name}`}>{client.name}</Link>
                      <small className="resource-id">{client.frn}</small>
                    </td>
                    <td>{client.labels?.[OWNER_LABEL] ?? <span className="panel-note">—</span>}</td>
                    <td>
                      {Object.entries(client.labels ?? {})
                        .filter(([key]) => key !== OWNER_LABEL)
                        .map(([key, value]) => (
                          <span className="tag" key={key}>
                            {key}={value}
                          </span>
                        ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </>
  );
}
