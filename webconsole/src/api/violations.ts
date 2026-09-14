// violationsByActionIndex groups ApiError field violations by the leading
// `actions[N]` index a Policy's whitelist validation attributes them to
// (governance/whitelist.go's validateAction: field := "actions[<index>]"), so
// ActionEditor can render each violation inline on the offending action row
// instead of as a generic toast.
export function violationsByActionIndex(
  fieldViolations: { field: string; description: string }[],
): Record<number, string[]> {
  const out: Record<number, string[]> = {};
  for (const v of fieldViolations) {
    const match = /^actions\[(\d+)\]/.exec(v.field);
    if (!match) continue;
    const index = Number(match[1]);
    (out[index] ??= []).push(v.description);
  }
  return out;
}
