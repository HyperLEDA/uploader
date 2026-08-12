import { fireEvent, render, screen } from "@testing-library/react";
import type { WidgetProps } from "@rjsf/utils";
import { describe, expect, it, vi } from "vitest";
import { ExpressionWidget } from "./ExpressionWidget";

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
  it("renders an editor and token list", () => {
    renderWidget();
    expect(screen.getByLabelText("Designation expression")).toBeInTheDocument();
    expect(screen.getByText("col")).toBeInTheDocument();
    expect(screen.getByText("deg")).toBeInTheDocument();
  });

  it("inserts a token when a chip is clicked", () => {
    const onChange = vi.fn();
    renderWidget(onChange, "1 * ");
    fireEvent.click(screen.getByText("deg"));
    expect(onChange).toHaveBeenCalledWith("1 * deg");
  });
});
