import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, Loading, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { ApiError } from "../../api/client";
import { useChannel, useUpdateChannel, updateMask } from "../../api/hooks";
import { channelTypeLabel } from "../../api/enums";

// Labels are the only editable field: `type` is immutable, `channel_partitions`
// is a staged re-shard (003.11), and the access policy has its own RPC — none of
// the three may appear in the update mask.
export function ChannelEdit() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useChannel(name);
  const update = useUpdateChannel(name);
  const channel = data?.asyncChannel;

  const baseLabels = useMemo(() => ({ ...(channel?.labels ?? {}) }), [channel]);
  const [draftLabels, setDraftLabels] = useState<Record<string, string> | null>(null);
  const labels = draftLabels ?? baseLabels;

  if (isLoading) return <Loading what="channel" />;
  if (error || !channel) {
    return (
      <>
        <ErrorBanner error={error} />
        <p className="empty-note">
          Async Channel not found. <Link to="/async-channels">Back to Async Channels</Link>
        </p>
      </>
    );
  }

  const deleted = channel.state === "CHANNEL_STATE_DELETED";
  const labelsChanged = JSON.stringify(labels) !== JSON.stringify(baseLabels);
  const conflict = update.error instanceof ApiError && update.error.status === 409;

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Async Channels", to: "/async-channels" },
          { label: name, to: `/async-channels/${name}` },
          { label: "Edit" },
        ]}
      />
      <PageHeading title={`Edit ${name}`} lead={channel.frn} />
      <ErrorBanner error={update.error} />
      {conflict ? (
        <div className="app-error" role="alert">
          The channel changed since you opened this form.{" "}
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
      {deleted ? <p className="empty-note">This channel is deleted and cannot be edited.</p> : null}

      <Panel
        title="Editable fields"
        note="Labels only. name, FRN, type, channel partitions and state are immutable here — use the detail page for pause / resume / delete."
      >
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            update.mutate(
              { updateMask: updateMask(["labels"]), labels },
              { onSuccess: () => navigate(`/async-channels/${name}`) },
            );
          }}
        >
          <div className="form-section">
            <h3>Immutable</h3>
            <dl className="detail-grid">
              <dt>Channel type</dt>
              <dd>
                <code>{channelTypeLabel(channel.type)}</code>
              </dd>
              <dt>Channel partitions</dt>
              <dd>
                {channel.channelPartitions ?? 0}{" "}
                <span className="panel-note">changed through a staged re-shard, not this form</span>
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
            <Link className="button" to={`/async-channels/${name}`}>
              Cancel
            </Link>
            <button
              className="button primary"
              type="submit"
              disabled={deleted || !labelsChanged || update.isPending}
            >
              {update.isPending ? "Saving…" : "Save changes"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
