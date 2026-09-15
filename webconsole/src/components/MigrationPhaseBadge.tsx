import { migrationPhaseLabel } from "../api/enums";

// MigrationPhaseBadge renders a ShardMigration's phase (003.13): the four
// in-flight phases share a neutral "converging" look (the same visual
// language AgentStatus already uses), DONE is quiet history, and FAILED is an
// error the operator needs to look at — its failure_reason becomes the
// tooltip so the reason is one hover away, not a second click.
export function MigrationPhaseBadge({ phase, failureReason }: { phase?: string; failureReason?: string }) {
  const cls =
    phase === "MIGRATION_PHASE_DONE" ? "" : phase === "MIGRATION_PHASE_FAILED" ? "error" : "pending";
  return (
    <span className={`status ${cls}`.trim()} title={phase === "MIGRATION_PHASE_FAILED" ? failureReason : undefined}>
      {migrationPhaseLabel(phase)}
    </span>
  );
}
