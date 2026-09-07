import { Link } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, StatusPill } from "../../components/ui";
import { useChannels } from "../../api/hooks";
import { channelTypeLabel } from "../../api/enums";

export function ChannelList() {
  const { data, isLoading, error } = useChannels();
  const channels = data?.asyncChannels ?? [];

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console", to: "/" }, { label: "Async Channels" }]} />
      <PageHeading
        title="Async Channels"
        lead="Customer-facing async boundaries managed by Franz. The channel declares the intent; Franz maintains its generated Kafka Topics."
        actions={
          <Link className="button primary" to="/async-channels/register">
            Create Async Channel
          </Link>
        }
      />
      <ErrorBanner error={error} />
      <section className="panel">
        <div className="toolbar">
          <span className="panel-note">
            {channels.length} channel{channels.length === 1 ? "" : "s"}
          </span>
        </div>
        {isLoading ? (
          <Loading what="channels" />
        ) : channels.length === 0 ? (
          <Empty>
            No Async Channels yet. <Link to="/async-channels/register">Create one</Link>.
          </Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Channel name</th>
                  <th>Type</th>
                  <th>Channel partitions</th>
                  <th>Labels</th>
                  <th>State</th>
                </tr>
              </thead>
              <tbody>
                {channels.map((channel) => (
                  <tr key={channel.name}>
                    <td>
                      <Link to={`/async-channels/${channel.name}`}>{channel.name}</Link>
                      <small className="resource-id">{channel.frn}</small>
                    </td>
                    <td>
                      <code>{channelTypeLabel(channel.type)}</code>
                    </td>
                    <td>{channel.channelPartitions ?? "—"}</td>
                    <td>
                      {Object.entries(channel.labels ?? {}).map(([key, value]) => (
                        <span className="tag" key={key}>
                          {key}={value}
                        </span>
                      ))}
                    </td>
                    <td>
                      <StatusPill value={channel.state} />
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
