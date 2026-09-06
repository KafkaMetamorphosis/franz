import { useState } from "react";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderApp } from "../test/render";
import { LabelEditor } from "./LabelEditor";

function Harness() {
  const [labels, setLabels] = useState<Record<string, string>>({});
  return (
    <>
      <LabelEditor value={labels} onChange={setLabels} />
      <output data-testid="out">{JSON.stringify(labels)}</output>
    </>
  );
}

test("adds and removes labels", async () => {
  const user = userEvent.setup();
  renderApp(<Harness />);

  await user.type(screen.getByLabelText("Label key"), "env");
  await user.type(screen.getByLabelText("Label value"), "prod");
  await user.click(screen.getByRole("button", { name: "Add label" }));

  expect(screen.getByTestId("out")).toHaveTextContent('{"env":"prod"}');
  expect(screen.getByText("env=prod")).toBeInTheDocument();

  await user.click(screen.getByRole("button", { name: "Remove env=prod" }));
  expect(screen.getByTestId("out")).toHaveTextContent("{}");
});

test("ignores an incomplete pair", async () => {
  const user = userEvent.setup();
  renderApp(<Harness />);
  await user.type(screen.getByLabelText("Label key"), "env");
  await user.click(screen.getByRole("button", { name: "Add label" }));
  expect(screen.getByTestId("out")).toHaveTextContent("{}");
});
