import { render, screen } from "@testing-library/react";
import type { WidgetProps } from "@rjsf/utils";
import { describe, expect, it, vi } from "vitest";
import {
  ExpressionWidget,
  findToken,
  type ExpressionToken,
} from "./ExpressionWidget";

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

function renderWidget(
  onChange = vi.fn(),
  value = "",
  fieldTokens: ExpressionToken[] = tokens,
) {
  const props = {
    id: "root_expression",
    name: "expression",
    label: "Designation expression",
    value,
    onChange,
    options: { tokens: fieldTokens },
    schema: { type: "string" },
    registry: {
      formContext: {
        expressionErrors: {
          root_expression: [
            {
              path: ["expression"],
              message: "unknown name 'foo'",
              start_line: 1,
              start_column: 1,
              end_line: 1,
              end_column: 4,
            },
          ],
        },
      },
    },
  } as unknown as WidgetProps;
  return render(<ExpressionWidget {...props} />);
}

describe("ExpressionWidget", () => {
  it("renders an editor", () => {
    renderWidget();
    expect(screen.getByLabelText("Designation expression")).toBeInTheDocument();
  });

  it("renders an editor without tokens", () => {
    renderWidget(vi.fn(), "", []);
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
