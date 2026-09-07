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
      <button type="button">outside</button>
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

test("commits a typed-but-not-added pair when focus leaves the editor", async () => {
  const user = userEvent.setup();
  renderApp(<Harness />);

  await user.type(screen.getByLabelText("Label key"), "team");
  await user.type(screen.getByLabelText("Label value"), "platform");
  // No "Add label" click — the user moves straight to another control.
  await user.click(screen.getByRole("button", { name: "outside" }));

  expect(screen.getByTestId("out")).toHaveTextContent('{"team":"platform"}');
});

test("tabbing between the key and value inputs does not commit", async () => {
  const user = userEvent.setup();
  renderApp(<Harness />);

  await user.type(screen.getByLabelText("Label key"), "team");
  await user.tab(); // focus moves to the value input — still inside the editor
  expect(screen.getByTestId("out")).toHaveTextContent("{}");
});
