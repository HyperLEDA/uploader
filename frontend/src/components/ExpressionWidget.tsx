import Editor, { type Monaco } from "@monaco-editor/react";
import Box from "@mui/material/Box";
import { useTheme } from "@mui/material/styles";
import type { WidgetProps } from "@rjsf/utils";
import type { editor, Position } from "monaco-editor";
import { useRef } from "react";
import {
  type ExpressionDiagnostic,
  validateExpression,
} from "./expressionValidate";

const LANGUAGE_ID = "hyperleda-expression";
const MARKER_OWNER = "hyperleda-expression";

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

export function findToken(
  word: string,
  tokens: ExpressionToken[],
): ExpressionToken | undefined {
  return tokens.find((token) => token.label === word);
}

function toMarkers(
  monaco: Monaco,
  diagnostics: ExpressionDiagnostic[],
): editor.IMarkerData[] {
  return diagnostics.map((diagnostic) => ({
    severity: monaco.MarkerSeverity.Error,
    message: diagnostic.message,
    startLineNumber: 1,
    startColumn: diagnostic.startColumn,
    endLineNumber: 1,
    endColumn: diagnostic.endColumn,
  }));
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
          documentation: token.detail,
          range,
        })),
      };
    },
  });
  monaco.languages.registerHoverProvider(LANGUAGE_ID, {
    provideHover(model: editor.ITextModel, position: Position) {
      const word = model.getWordAtPosition(position);
      if (!word) {
        return null;
      }
      const token = findToken(word.word, currentTokens);
      if (!token) {
        return null;
      }
      return {
        range: {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        },
        contents: [{ value: `**${token.label}**` }, { value: token.detail }],
      };
    },
  });
  languageRegistered = true;
}

export function ExpressionWidget(props: WidgetProps) {
  const theme = useTheme();
  const tokens = readTokens(props.options);
  const value = typeof props.value === "string" ? props.value : "";
  const isDark = theme.palette.mode === "dark";
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<Monaco | null>(null);
  const allowedLabels = tokens.map((token) => token.label);

  function applyMarkers(text: string) {
    const editorInstance = editorRef.current;
    const monaco = monacoRef.current;
    const model = editorInstance?.getModel();
    if (!editorInstance || !monaco || !model) {
      return;
    }
    monaco.editor.setModelMarkers(
      model,
      MARKER_OWNER,
      toMarkers(monaco, validateExpression(text, allowedLabels)),
    );
  }

  function handleBeforeMount(monaco: Monaco) {
    registerExpressionLanguage(monaco, tokens);
  }

  function handleMount(
    editorInstance: editor.IStandaloneCodeEditor,
    monaco: Monaco,
  ) {
    editorRef.current = editorInstance;
    monacoRef.current = monaco;
    editorInstance.onKeyDown((e) => {
      if (e.keyCode === monaco.KeyCode.Enter) {
        e.preventDefault();
        e.stopPropagation();
      }
    });
    applyMarkers(value.replace(/\r?\n/g, ""));
  }

  return (
    <Box
      sx={{
        border: 1,
        borderColor: "divider",
        borderRadius: 1,
        overflow: "hidden",
        bgcolor: isDark ? "#1e1e1e" : "#ffffff",
        "&:hover": {
          borderColor: "text.primary",
        },
        "&:focus-within": {
          borderColor: "primary.main",
        },
      }}
    >
      <Editor
        height="42px"
        language={LANGUAGE_ID}
        theme={isDark ? "vs-dark" : "light"}
        value={value.replace(/\r?\n/g, "")}
        onChange={(next) => {
          const singleLine = (next ?? "").replace(/\r?\n/g, "");
          props.onChange(singleLine);
          applyMarkers(singleLine);
        }}
        beforeMount={handleBeforeMount}
        onMount={handleMount}
        options={{
          ariaLabel: props.label,
          readOnly: props.disabled || props.readonly,
          minimap: { enabled: false },
          lineNumbers: "off",
          folding: false,
          glyphMargin: false,
          lineDecorationsWidth: 12,
          lineNumbersMinChars: 0,
          scrollBeyondLastLine: false,
          wordWrap: "off",
          fontSize: 14,
          lineHeight: 22,
          padding: { top: 10, bottom: 10 },
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
          acceptSuggestionOnEnter: "off",
          tabCompletion: "on",
          automaticLayout: true,
          contextmenu: false,
          fixedOverflowWidgets: true,
        }}
        loading={<Box sx={{ height: 42 }} />}
      />
    </Box>
  );
}
