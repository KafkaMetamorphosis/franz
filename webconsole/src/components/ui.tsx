import type { ReactNode } from "react";
import { ApiError } from "../api/client";

export function PageHeading({ title, lead, actions }: { title: string; lead?: string; actions?: ReactNode }) {
  return (
    <div className="page-heading">
      <div>
        <h1>{title}</h1>
        {lead ? <p className="lead">{lead}</p> : null}
      </div>
      {actions ? <div className="inline-actions">{actions}</div> : null}
    </div>
  );
}

export function Panel({ title, note, actions, children }: { title?: string; note?: string; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="panel">
      {title ? (
        <div className="panel-header">
          <div>
            <h2>{title}</h2>
            {note ? <p className="panel-note">{note}</p> : null}
          </div>
          {actions}
        </div>
      ) : null}
      <div className="panel-body">{children}</div>
    </section>
  );
}

export function Breadcrumbs({ items }: { items: { label: string; to?: string }[] }) {
  return (
    <div className="breadcrumbs">
      {items.map((it, i) => (
        <span key={i}>
          {i > 0 ? <span>/</span> : null}
          {it.to ? <a href={it.to}>{it.label}</a> : it.label}
        </span>
      ))}
    </div>
  );
}

export function ErrorBanner({ error }: { error: unknown }) {
  if (!error) return null;
  if (error instanceof ApiError) {
    return (
      <div className="app-error" role="alert">
        <strong>{error.message}</strong>
        {error.fieldViolations.length > 0 ? (
          <ul>
            {error.fieldViolations.map((v, i) => (
              <li key={i}>
                <code>{v.field}</code>: {v.description}
              </li>
            ))}
          </ul>
        ) : null}
      </div>
    );
  }
  return (
    <div className="app-error" role="alert">
      {String((error as Error)?.message ?? error)}
    </div>
  );
}

export function Loading({ what }: { what: string }) {
  return <p className="loading-note">Loading {what}…</p>;
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="empty-note">{children}</p>;
}

const STATUS_CLASS: Record<string, string> = {
  ACTIVE: "",
  READY: "",
  PAUSED: "paused",
  DELETED: "error",
  ERROR: "error",
  DEGRADED: "paused",
  PROVISIONING: "pending",
  STOPPED: "pending",
  REMOVED: "pending",
  PENDING: "pending",
};

// StatusPill renders a proto enum short form (e.g. AGENT_STATUS_ACTIVE) as a
// coloured pill with a friendly label.
export function StatusPill({ value }: { value?: string }) {
  const short = (value ?? "UNKNOWN").replace(/^[A-Z_]+_(?=[A-Z])/, "").replace(/_/g, " ") || "UNKNOWN";
  const key = short.replace(/ /g, "_").toUpperCase();
  const cls = STATUS_CLASS[key] ?? "info";
  return (
    <span className={`status ${cls}`.trim()}>
      {short.charAt(0) + short.slice(1).toLowerCase()}
    </span>
  );
}

export function CopyButton({ text, label = "Copy" }: { text: string; label?: string }) {
  return (
    <button
      type="button"
      className="button copy-button"
      onClick={() => {
        void navigator.clipboard?.writeText(text);
      }}
    >
      {label}
    </button>
  );
}
