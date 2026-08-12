import { describe, expect, it } from "vitest";
import {
  validateExpression,
  validateKnownTokens,
  validateParentheses,
  validateStrings,
} from "./expressionValidate";

const tokens = ["col", "deg", "sin", "pi", "to_deg", "str"];

describe("validateStrings", () => {
  it("reports unclosed strings", () => {
    expect(validateStrings('col("a)')).toEqual([
      { message: "Unclosed string", startColumn: 5, endColumn: 8 },
    ]);
  });

  it("accepts closed strings", () => {
    expect(validateStrings('col("a")')).toEqual([]);
  });
});

describe("validateParentheses", () => {
  it("reports unclosed and unmatched parentheses", () => {
    expect(validateParentheses('col("a"')).toEqual([
      { message: "Unclosed '('", startColumn: 4, endColumn: 5 },
    ]);
    expect(validateParentheses("1)")).toEqual([
      { message: "Unmatched ')'", startColumn: 2, endColumn: 3 },
    ]);
  });
});

describe("validateKnownTokens", () => {
  it("reports unknown identifiers", () => {
    expect(validateKnownTokens('foo + col("a")', tokens)).toEqual([
      { message: "Unknown name 'foo'", startColumn: 1, endColumn: 4 },
    ]);
  });

  it("ignores identifiers inside strings", () => {
    expect(validateKnownTokens('col("foo_bar")', tokens)).toEqual([]);
  });
});

describe("validateExpression", () => {
  it("accepts a valid expression", () => {
    expect(validateExpression('to_deg(col("RAJ2000"))', tokens)).toEqual([]);
    expect(validateExpression("sin(pi) + 1.5 * deg", tokens)).toEqual([]);
  });

  it("combines diagnostics from each check", () => {
    expect(validateExpression('col("a)', tokens)).toEqual([
      { message: "Unclosed string", startColumn: 5, endColumn: 8 },
      { message: "Unclosed '('", startColumn: 4, endColumn: 5 },
    ]);
  });
});
