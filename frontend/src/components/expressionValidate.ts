export type ExpressionDiagnostic = {
  message: string;
  startColumn: number;
  endColumn: number;
};

type ScanVisitor = {
  onString?: (start: number, end: number, closed: boolean) => void;
  onParenOpen?: (index: number) => void;
  onParenClose?: (index: number) => void;
  onIdent?: (start: number, end: number, word: string) => void;
};

function isIdentStart(char: string): boolean {
  return /[a-zA-Z_]/.test(char);
}

function isIdentPart(char: string): boolean {
  return /[a-zA-Z0-9_]/.test(char);
}

function scanExpression(source: string, visitor: ScanVisitor): void {
  let i = 0;

  while (i < source.length) {
    const char = source[i];

    if (char === '"') {
      const start = i;
      i += 1;
      let closed = false;
      while (i < source.length) {
        if (source[i] === "\\") {
          i += 2;
          continue;
        }
        if (source[i] === '"') {
          closed = true;
          i += 1;
          break;
        }
        i += 1;
      }
      visitor.onString?.(start, i, closed);
      continue;
    }

    if (char === "'") {
      const start = i;
      i += 1;
      let closed = false;
      while (i < source.length) {
        if (source[i] === "'") {
          closed = true;
          i += 1;
          break;
        }
        i += 1;
      }
      visitor.onString?.(start, i, closed);
      continue;
    }

    if (char === "(") {
      visitor.onParenOpen?.(i);
      i += 1;
      continue;
    }

    if (char === ")") {
      visitor.onParenClose?.(i);
      i += 1;
      continue;
    }

    if (isIdentStart(char)) {
      const start = i;
      i += 1;
      while (i < source.length && isIdentPart(source[i])) {
        i += 1;
      }
      visitor.onIdent?.(start, i, source.slice(start, i));
      continue;
    }

    if (/[0-9]/.test(char)) {
      i += 1;
      while (i < source.length && /[0-9]/.test(source[i])) {
        i += 1;
      }
      if (source[i] === "." && /[0-9]/.test(source[i + 1] ?? "")) {
        i += 1;
        while (i < source.length && /[0-9]/.test(source[i])) {
          i += 1;
        }
      }
      continue;
    }

    i += 1;
  }
}

export function validateStrings(source: string): ExpressionDiagnostic[] {
  const diagnostics: ExpressionDiagnostic[] = [];
  scanExpression(source, {
    onString(start, end, closed) {
      if (!closed) {
        diagnostics.push({
          message: "Unclosed string",
          startColumn: start + 1,
          endColumn: Math.max(end, source.length) + 1,
        });
      }
    },
  });
  return diagnostics;
}

export function validateParentheses(source: string): ExpressionDiagnostic[] {
  const diagnostics: ExpressionDiagnostic[] = [];
  const parenStack: number[] = [];
  scanExpression(source, {
    onParenOpen(index) {
      parenStack.push(index);
    },
    onParenClose(index) {
      if (parenStack.length === 0) {
        diagnostics.push({
          message: "Unmatched ')'",
          startColumn: index + 1,
          endColumn: index + 2,
        });
        return;
      }
      parenStack.pop();
    },
  });
  for (const start of parenStack) {
    diagnostics.push({
      message: "Unclosed '('",
      startColumn: start + 1,
      endColumn: start + 2,
    });
  }
  return diagnostics;
}

export function validateKnownTokens(
  source: string,
  allowedTokens: ReadonlySet<string> | readonly string[],
): ExpressionDiagnostic[] {
  const allowed =
    allowedTokens instanceof Set ? allowedTokens : new Set(allowedTokens);
  const diagnostics: ExpressionDiagnostic[] = [];
  scanExpression(source, {
    onIdent(start, end, word) {
      if (!allowed.has(word)) {
        diagnostics.push({
          message: `Unknown name '${word}'`,
          startColumn: start + 1,
          endColumn: end + 1,
        });
      }
    },
  });
  return diagnostics;
}

export function validateExpression(
  source: string,
  allowedTokens: ReadonlySet<string> | readonly string[],
): ExpressionDiagnostic[] {
  return [
    ...validateStrings(source),
    ...validateParentheses(source),
    ...validateKnownTokens(source, allowedTokens),
  ];
}
