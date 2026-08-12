import { useRef, useState } from "react";
import Editor, { type Monaco, type OnMount } from "@monaco-editor/react";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Stack from "@mui/material/Stack";
import { useTheme } from "@mui/material/styles";
import type { WidgetProps } from "@rjsf/utils";
import type { editor, Position } from "monaco-editor";

const LANGUAGE_ID = "hyperleda-expression";

export type ExpressionToken = {
  label: string;
  insert: string;
  kind: "function" | "constant";
  detail: string;
};

let languageRegistered = false;
let currentTokens: ExpressionToken[] = [];

function isExpressionToken(value: unknown): value is ExpressionToken {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const token = value as Record<string, unknown>;
  return (
    typeof token.label === "string" &&
    typeof token.insert === "string" &&
    (token.kind === "function" || token.kind === "constant") &&
    typeof token.detail === "string"
  );
}

function readTokens(options: WidgetProps["options"]): ExpressionToken[] {
  const raw = options.tokens;
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.filter(isExpressionToken);
}

function snippetToText(insert: string): string {
  return insert
    .replace(/\$\{\d+:([^}]+)\}/g, "$1")
    .replace(/\$\{\d+\}/g, "")
    .replace(/\$\d+/g, "");
}

function registerExpressionLanguage(
  monaco: Monaco,
  tokens: ExpressionToken[],
): void {
  currentTokens = tokens;
  if (languageRegistered) {
    return;
  }
  monaco.languages.register({ id: LANGUAGE_ID });
  monaco.languages.setMonarchTokensProvider(LANGUAGE_ID, {
    keywords: tokens.map((token) => token.label),
    tokenizer: {
      root: [
        [/"(?:\\.|[^"\\])*"/, "string"],
        [/'[^']*'/, "string"],
        [/\d+(?:\.\d+)?/, "number"],
        [/[+\-*/%=()]/, "delimiter"],
        [
          /[a-zA-Z_]\w*/,
          {
            cases: {
              "@keywords": "keyword",
              "@default": "identifier",
            },
          },
        ],
      ],
    },
  });
  monaco.languages.registerCompletionItemProvider(LANGUAGE_ID, {
    triggerCharacters: ["(", '"'],
    provideCompletionItems(model: editor.ITextModel, position: Position) {
      const word = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };
      return {
        suggestions: currentTokens.map((token) => ({
          label: token.label,
          kind:
            token.kind === "function"
              ? monaco.languages.CompletionItemKind.Function
              : monaco.languages.CompletionItemKind.Constant,
          insertText: token.insert,
          insertTextRules:
            monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
          detail: token.detail,
          range,
        })),
      };
    },
  });
  languageRegistered = true;
}

export function ExpressionWidget(props: WidgetProps) {
  const theme = useTheme();
  const [focused, setFocused] = useState(false);
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);
  const tokens = readTokens(props.options);
  const value = typeof props.value === "string" ? props.value : "";

  function handleBeforeMount(monaco: Monaco) {
    registerExpressionLanguage(monaco, tokens);
  }

  function handleMount(editor: Parameters<OnMount>[0]) {
    editorRef.current = editor;
    editor.onDidFocusEditorText(() => setFocused(true));
    editor.onDidBlurEditorText(() => setFocused(false));
  }

  function insertToken(token: ExpressionToken) {
    const text = snippetToText(token.insert);
    const instance = editorRef.current;
    if (instance) {
      const selection = instance.getSelection();
      if (selection) {
        instance.executeEdits("token-chip", [
          { range: selection, text, forceMoveMarkers: true },
        ]);
        instance.focus();
        props.onChange(instance.getValue());
        return;
      }
    }
    props.onChange(`${value}${text}`);
  }

  return (
    <Box>
      <Box
        sx={{
          border: 1,
          borderColor: focused ? "primary.main" : "divider",
          borderRadius: 1,
          overflow: "hidden",
          "&:hover": {
            borderColor: focused ? "primary.main" : "text.primary",
          },
        }}
      >
        <Editor
          height="72px"
          language={LANGUAGE_ID}
          theme={theme.palette.mode === "dark" ? "vs-dark" : "light"}
          value={value}
          onChange={(next) => props.onChange(next ?? "")}
          beforeMount={handleBeforeMount}
          onMount={handleMount}
          options={{
            ariaLabel: props.label,
            readOnly: props.disabled || props.readonly,
            minimap: { enabled: false },
            lineNumbers: "off",
            folding: false,
            glyphMargin: false,
            lineDecorationsWidth: 0,
            lineNumbersMinChars: 0,
            scrollBeyondLastLine: false,
            wordWrap: "on",
            fontSize: 14,
            lineHeight: 22,
            padding: { top: 8, bottom: 8 },
            overviewRulerLanes: 0,
            hideCursorInOverviewRuler: true,
            renderLineHighlight: "none",
            scrollbar: {
              vertical: "hidden",
              horizontal: "auto",
              alwaysConsumeMouseWheel: false,
            },
            quickSuggestions: { other: true, comments: false, strings: true },
            wordBasedSuggestions: "off",
            suggestOnTriggerCharacters: true,
            tabCompletion: "on",
            automaticLayout: true,
            contextmenu: false,
          }}
          loading={<Box sx={{ height: 72 }} />}
        />
      </Box>
      {tokens.length > 0 && (
        <Stack
          direction="row"
          spacing={0.5}
          useFlexGap
          flexWrap="wrap"
          sx={{ mt: 0.75 }}
        >
          {tokens.map((token) => (
            <Chip
              key={token.label}
              size="small"
              label={token.label}
              title={token.detail}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => insertToken(token)}
            />
          ))}
        </Stack>
      )}
    </Box>
  );
}
