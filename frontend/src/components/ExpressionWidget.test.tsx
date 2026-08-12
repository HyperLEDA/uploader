import { render, screen } from "@testing-library/react";
import type { WidgetProps } from "@rjsf/utils";
import { describe, expect, it, vi } from "vitest";
import { ExpressionWidget, findToken } from "./ExpressionWidget";

vi.mock("../api", async () => {
  const actual = await vi.importActual<typeof import("../api")>("../api");
  return {
    ...actual,
    validateExpression: vi.fn().mockResolvedValue([]),
  };
});

const tokens = [
  {
    label: "col",
    insert: 'col("$' + '{1:name}")',
    kind: "function" as const,
    detail: "Rawdata column",
  },
  {
    label: "deg",
    insert: "deg",
    kind: "constant" as const,
    detail: "Named constant",
  },
];

function renderWidget(onChange = vi.fn(), value = "") {
  const props = {
    id: "root_expression",
    name: "expression",
    label: "Designation expression",
    value,
    onChange,
    options: { tokens },
    schema: { type: "string" },
  } as unknown as WidgetProps;
  return render(<ExpressionWidget {...props} />);
}

describe("ExpressionWidget", () => {
  it("renders an editor", () => {
    renderWidget();
    expect(screen.getByLabelText("Designation expression")).toBeInTheDocument();
  });
});

describe("findToken", () => {
  it("returns the token matching a hovered word", () => {
    expect(findToken("col", tokens)?.detail).toBe("Rawdata column");
    expect(findToken("deg", tokens)?.detail).toBe("Named constant");
  });

  it("returns undefined for unknown words", () => {
    expect(findToken("unknown", tokens)).toBeUndefined();
  });
});
