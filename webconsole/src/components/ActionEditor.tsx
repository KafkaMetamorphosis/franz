import { ACTION_KINDS, GOVERNABLE_STATUSES } from "../api/enums";
import type { Action } from "../api/hooks";

// ActionEditor edits a Policy's repeatable `actions` list. Each action's arg
// shape depends on its kind (see v1Action's doc comment); the two arithmetic
// kinds get an optional third "cap" arg (a "max=<n>" / "min=<n>" clamp, 003.8).
// Server-side whitelist-violation errors are field paths like "actions[0]" or
// "actions[0].args[1]" (governance/whitelist.go's validateAction) — violations
// passes them straight through, keyed by index, so the caller renders them
// inline on the offending row rather than as a generic toast.
export function ActionEditor({
  value,
  onChange,
  violations = {},
}: {
  value: Action[];
  onChange: (next: Action[]) => void;
  violations?: Record<number, string[]>;
}) {
  const update = (index: number, next: Action) => {
    const copy = value.slice();
    copy[index] = next;
    onChange(copy);
  };
  const remove = (index: number) => onChange(value.filter((_, i) => i !== index));
  const add = () => onChange([...value, { kind: "ACTION_KIND_UPDATE_FIELD", args: ["", ""] }]);

  return (
    <div className="action-editor">
      {value.map((action, index) => {
        const kindInfo = ACTION_KINDS.find((k) => k.value === action.kind) ?? ACTION_KINDS[0];
        const args = action.args ?? [];
        const isArithmetic =
          action.kind === "ACTION_KIND_INCREASE_FIELD_BY" || action.kind === "ACTION_KIND_DECREASE_FIELD_BY";
        const rowErrors = violations[index] ?? [];

        return (
          <div className="action-row" key={index}>
            <div className="action-row-fields">
              <select
                aria-label={`Action ${index + 1} kind`}
                value={action.kind}
                onChange={(e) => {
                  const kind = e.target.value;
                  const info = ACTION_KINDS.find((k) => k.value === kind)!;
                  update(index, { kind: kind as never, args: Array(info.argCount).fill("") });
                }}
              >
                {ACTION_KINDS.map((k) => (
                  <option key={k.value} value={k.value}>
                    {k.label}
                  </option>
                ))}
              </select>
              {Array.from({ length: kindInfo.argCount }).map((_, argIndex) =>
                argIndex === 0 && action.kind === "ACTION_KIND_SET_STATUS" ? (
                  <select
                    key={argIndex}
                    aria-label={`Action ${index + 1} arg ${argIndex + 1}`}
                    value={args[argIndex] ?? ""}
                    onChange={(e) => {
                      const nextArgs = args.slice();
                      nextArgs[argIndex] = e.target.value;
                      update(index, { ...action, args: nextArgs });
                    }}
                  >
                    <option value="" disabled>
                      status…
                    </option>
                    {GOVERNABLE_STATUSES.map((status) => (
                      <option key={status} value={status}>
                        {status}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    key={argIndex}
                    aria-label={`Action ${index + 1} arg ${argIndex + 1}`}
                    placeholder={
                      argIndex === 0
                        ? "field"
                        : action.kind === "ACTION_KIND_ADD_LABEL"
                          ? "value"
                          : "value / amount"
                    }
                    value={args[argIndex] ?? ""}
                    onChange={(e) => {
                      const nextArgs = args.slice();
                      nextArgs[argIndex] = e.target.value;
                      update(index, { ...action, args: nextArgs });
                    }}
                  />
                ),
              )}
              {isArithmetic ? (
                <input
                  aria-label={`Action ${index + 1} cap`}
                  placeholder="cap: max=64 or min=1 (optional)"
                  value={args[kindInfo.argCount] ?? ""}
                  onChange={(e) => {
                    const nextArgs = args.slice(0, kindInfo.argCount);
                    if (e.target.value) nextArgs[kindInfo.argCount] = e.target.value;
                    update(index, { ...action, args: nextArgs });
                  }}
                />
              ) : null}
              <button type="button" className="button danger" onClick={() => remove(index)}>
                Remove
              </button>
            </div>
            {rowErrors.length > 0 ? (
              <ul className="field-error" role="alert">
                {rowErrors.map((msg, i) => (
                  <li key={i}>{msg}</li>
                ))}
              </ul>
            ) : null}
          </div>
        );
      })}
      <button type="button" className="button" onClick={add}>
        Add action
      </button>
    </div>
  );
}
