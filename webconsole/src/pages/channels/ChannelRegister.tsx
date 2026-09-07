import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { useCreateChannel } from "../../api/hooks";
import { CHANNEL_TYPES } from "../../api/enums";

// Ported from 001-ux/demo/create-async-channel.html. The demo's placement-context
// section is not reproduced — placement coordinates are ordinary labels
// (deliverable 13) — and the access-policy editor ships with deliverable 17.
export function ChannelRegister() {
  const navigate = useNavigate();
  const createChannel = useCreateChannel();

  const [name, setName] = useState("");
  const [channelType, setChannelType] = useState<string>(CHANNEL_TYPES[0].value);
  const [channelPartitions, setChannelPartitions] = useState("1");
  const [labels, setLabels] = useState<Record<string, string>>({});
  const [localError, setLocalError] = useState<string | null>(null);

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Async Channels", to: "/async-channels" },
          { label: "Create Async Channel" },
        ]}
      />
      <PageHeading
        title="Create Async Channel"
        lead="Create a customer-facing asynchronous communication boundary managed by Franz."
      />
      <ErrorBanner error={localError ?? createChannel.error} />
      <Panel
        title="Channel details"
        note="A Kafka topic channel generates one or more linked Kafka Topic entities."
      >
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            setLocalError(null);
            const partitions = Number(channelPartitions);
            if (!Number.isInteger(partitions) || partitions < 1) {
              setLocalError("Channel partitions must be a whole number of 1 or more.");
              return;
            }
            createChannel.mutate(
              {
                name: name.trim(),
                type: channelType as never,
                channelPartitions: partitions,
                labels,
              },
              {
                onSuccess: (result) =>
                  navigate(`/async-channels/${result.asyncChannel?.name ?? name.trim()}`),
              },
            );
          }}
        >
          <div className="form-section">
            <h3>Channel details</h3>
            <p className="form-help">The Async Channel is the primary resource managed by your service team.</p>
            <div className="field">
              <label htmlFor="channel-name">
                Channel name <small>Unique within your authorized scope. Immutable.</small>
              </label>
              <div>
                <input
                  id="channel-name"
                  required
                  placeholder="for example, order-events"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                />
                <p className="field-note">
                  Franz uses channel information to generate the Kafka topic name and channel FRN.
                </p>
              </div>
            </div>
            <div className="field">
              <label htmlFor="channel-type">Channel type</label>
              <div>
                <select
                  id="channel-type"
                  aria-describedby="channel-type-note"
                  value={channelType}
                  onChange={(event) => setChannelType(event.target.value)}
                >
                  {CHANNEL_TYPES.map((type) => (
                    <option key={type.value} value={type.value}>
                      {type.label}
                    </option>
                  ))}
                </select>
                <p className="field-note" id="channel-type-note">
                  Kafka topic is the only supported type in the current experience.
                </p>
              </div>
            </div>
            <div className="field">
              <label htmlFor="channel-partitions">
                Channel partitions <small>Number of generated topics.</small>
              </label>
              <div>
                <input
                  id="channel-partitions"
                  type="number"
                  min={1}
                  step={1}
                  required
                  value={channelPartitions}
                  onChange={(event) => setChannelPartitions(event.target.value)}
                />
                <p className="field-note">
                  This controls how many Kafka topics Franz creates for the Async Channel. It is distinct
                  from Kafka topic partitions.
                </p>
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Labels</h3>
            <p className="form-help">
              Fleet-context metadata, including the reserved <code>franz.placement/*</code> coordinates
              placement uses to find eligible clusters.
            </p>
            <div className="field">
              <label>Labels</label>
              <div>
                <LabelEditor value={labels} onChange={setLabels} />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to="/async-channels">
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={createChannel.isPending}>
              {createChannel.isPending ? "Creating…" : "Create Async Channel"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
