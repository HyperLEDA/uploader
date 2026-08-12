import { createElement, type ChangeEvent } from "react";
import { vi } from "vitest";
import "@testing-library/jest-dom";

vi.mock("@monaco-editor/react", () => {
  function Default(props: {
    value?: string;
    onChange?: (value: string | undefined) => void;
    options?: { ariaLabel?: string };
  }) {
    return createElement("textarea", {
      "aria-label": props.options?.ariaLabel,
      value: props.value ?? "",
      onChange: (event: ChangeEvent<HTMLTextAreaElement>) =>
        props.onChange?.(event.target.value),
    });
  }
  return {
    default: Default,
    loader: { config: () => undefined },
  };
});
