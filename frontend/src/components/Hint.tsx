import Tooltip from "@mui/material/Tooltip";
import type { ReactElement, ReactNode } from "react";

export type HintPosition =
  | "top"
  | "left"
  | "right"
  | "bottom";

interface HintProps {
  children: ReactElement;
  hintContent: ReactNode;
  position?: HintPosition;
}

export function Hint(props: HintProps): ReactElement {
  const placement = props.position ?? "right";

  return (
    <Tooltip
      title={props.hintContent}
      placement={placement}
      arrow={false}
      slotProps={{
        tooltip: {
          sx: {
            maxWidth: 576,
            bgcolor: "grey.700",
            fontSize: "0.8125rem",
          },
        },
      }}
    >
      {props.children}
    </Tooltip>
  );
}
