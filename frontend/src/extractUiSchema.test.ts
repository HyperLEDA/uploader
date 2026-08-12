import { describe, expect, it } from "vitest";
import { extractUiSchema } from "./extractUiSchema";

describe("extractUiSchema", () => {
  it("copies ui keys from nested properties", () => {
    const schema = {
      type: "object",
      properties: {
        password: {
          type: "string",
          "ui:widget": "password",
        },
        expression: {
          type: "string",
          "ui:options": { tokens: [{ label: "col" }] },
        },
        advanced: {
          type: "object",
          properties: {
            secret: {
              type: "string",
              "ui:widget": "password",
            },
          },
        },
      },
    };

    expect(extractUiSchema(schema)).toEqual({
      password: { "ui:widget": "password" },
      expression: {
        "ui:options": { tokens: [{ label: "col" }] },
      },
      advanced: {
        secret: { "ui:widget": "password" },
      },
    });
  });
});
