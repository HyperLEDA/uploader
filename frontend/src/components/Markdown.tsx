import Box from "@mui/material/Box";
import { useTheme } from "@mui/material/styles";
import ReactMarkdown from "react-markdown";

type MarkdownProps = {
  children: string;
};

export function Markdown({ children }: MarkdownProps) {
  const theme = useTheme();

  return (
    <Box
      sx={{
        typography: "body2",
        color: "text.secondary",
        "& h1, & h2, & h3, & h4": {
          color: "text.primary",
          fontWeight: 600,
          mt: 0,
          mb: 1,
        },
        "& h2": { fontSize: "1.1rem" },
        "& h3": { fontSize: "1rem" },
        "& p": { mt: 0, mb: 1 },
        "& ul, & ol": { mt: 0, mb: 1, pl: 2.5 },
        "& li": { mb: 0.5 },
        "& code": {
          fontFamily:
            "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
          fontSize: "0.85em",
          px: 0.5,
          py: 0.15,
          borderRadius: 0.5,
          backgroundColor:
            theme.palette.mode === "dark"
              ? "rgba(255, 255, 255, 0.08)"
              : "rgba(0, 0, 0, 0.06)",
        },
        "& pre": {
          p: 1.5,
          borderRadius: 1,
          overflow: "auto",
          backgroundColor:
            theme.palette.mode === "dark"
              ? "rgba(255, 255, 255, 0.06)"
              : "rgba(0, 0, 0, 0.04)",
          "& code": {
            p: 0,
            backgroundColor: "transparent",
          },
        },
        "& a": { color: "primary.main" },
      }}
    >
      <ReactMarkdown>{children}</ReactMarkdown>
    </Box>
  );
}
