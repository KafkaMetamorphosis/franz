import { useState } from "react";
import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ProvisioningFields } from "./ProvisioningFields";

// Stateful harness so a typed value actually round-trips through the controlled
// component (mirrors how the real forms hold the value map).
function Harness({ onChange }: { onChange?: (v: Record<string, string>) => void }) {
  const [value, setValue] = useState<Record<string, string>>({});
  return (
    <ProvisioningFields
      specs={specs}
      value={value}
      onChange={(v) => {
        setValue(v);
        onChange?.(v);
      }}
    />
  );
}

const specs = [
  {
    key: "franz.provisioning/deployment-type",
    allowedValues: ["local-docker"],
    defaultValue: "local-docker",
    required: true,
  },
  { key: "franz.provisioning/kafka-image", allowedValues: [], defaultValue: "", required: false },
];

test("renders a select for allowed_values and text for free fields, with defaults", () => {
  render(<ProvisioningFields specs={specs} value={{}} onChange={() => {}} />);

  const dep = screen.getByLabelText(/deployment-type/) as HTMLSelectElement;
  expect(dep.tagName).toBe("SELECT");
  expect(dep.value).toBe("local-docker");

  const img = screen.getByLabelText(/kafka-image/) as HTMLInputElement;
  expect(img.tagName).toBe("INPUT");
  expect(img.value).toBe("");
});

test("emits the full key on change and drops empty values", async () => {
  const user = userEvent.setup();
  const onChange = vi.fn();
  render(<Harness onChange={onChange} />);

  const img = screen.getByLabelText(/kafka-image/);
  await user.type(img, "redpanda");
  expect(onChange).toHaveBeenLastCalledWith({ "franz.provisioning/kafka-image": "redpanda" });

  await user.clear(img);
  expect(onChange).toHaveBeenLastCalledWith({});
});
