import { expect, test } from "vitest";
import {
  FALLBACK_PROVISIONING_LABELS,
  missingRequired,
  prefilled,
  shortKey,
  splitLabels,
  validateSchema,
} from "./provisioning";

test("shortKey strips the reserved prefix", () => {
  expect(shortKey("franz.provisioning/kafka-image")).toBe("kafka-image");
  expect(shortKey("team")).toBe("team");
});

test("splitLabels separates schema keys from the rest", () => {
  const specs = [{ key: "franz.provisioning/deployment-type" }];
  const { schema, free } = splitLabels(
    { "franz.provisioning/deployment-type": "local-docker", team: "infra" },
    specs,
  );
  expect(schema).toEqual({ "franz.provisioning/deployment-type": "local-docker" });
  expect(free).toEqual({ team: "infra" });
});

test("prefilled seeds defaults for unset keys only", () => {
  const specs = [
    { key: "franz.provisioning/kafka-version", defaultValue: "3.7.0" },
    { key: "franz.provisioning/kafka-image", defaultValue: "" },
  ];
  expect(prefilled({}, specs)).toEqual({ "franz.provisioning/kafka-version": "3.7.0" });
  expect(prefilled({ "franz.provisioning/kafka-version": "3.9.0" }, specs)).toEqual({
    "franz.provisioning/kafka-version": "3.9.0",
  });
});

test("missingRequired flags empty required keys", () => {
  const specs = [
    { key: "franz.provisioning/deployment-type", required: true },
    { key: "franz.provisioning/kafka-version", required: false },
  ];
  expect(missingRequired({}, specs)).toEqual(["franz.provisioning/deployment-type"]);
  expect(missingRequired({ "franz.provisioning/deployment-type": "local-docker" }, specs)).toEqual([]);
});

test("validateSchema mirrors the server rules", () => {
  expect(validateSchema([])).toBeNull();
  expect(validateSchema(FALLBACK_PROVISIONING_LABELS)).toBeNull();
  expect(validateSchema([{ key: "" }])).toMatch(/empty key/);
  expect(validateSchema([{ key: "provisioning/x" }])).toMatch(/franz\. namespace/);
  expect(validateSchema([{ key: "franz.a" }, { key: "franz.a" }])).toMatch(/Duplicate/);
  expect(
    validateSchema([{ key: "franz.a", allowedValues: ["x", "y"], defaultValue: "z" }]),
  ).toMatch(/not one of its allowed values/);
});
