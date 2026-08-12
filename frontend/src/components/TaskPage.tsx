import { useEffect, useRef, useState } from "react";
import { useLocation, useParams } from "react-router-dom";
import Form from "@rjsf/mui";
import type { RegistryWidgetsType } from "@rjsf/utils";
import validator from "@rjsf/validator-ajv8";
import InfoOutlined from "@mui/icons-material/InfoOutlined";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import IconButton from "@mui/material/IconButton";
import Popover from "@mui/material/Popover";
import Typography from "@mui/material/Typography";
import {
  type FormError,
  fetchTaskSchema,
  submitTask,
  validateTaskForm,
} from "../api";
import { extractUiSchema } from "../extractUiSchema";
import { ExpressionWidget } from "./ExpressionWidget";
import { FoldableObjectFieldTemplate } from "./FoldableObjectFieldTemplate";
import { Markdown } from "./Markdown";
import { ProgressView } from "./ProgressView";

const widgets: RegistryWidgetsType = {
  expression: ExpressionWidget,
};

function pathToFieldId(path: string[]): string {
  return ["root", ...path].join("_");
}

function toExpressionErrors(errors: FormError[]): Record<string, FormError[]> {
  const byId: Record<string, FormError[]> = {};
  for (const error of errors) {
    const fieldId = pathToFieldId(error.path);
    (byId[fieldId] ??= []).push(error);
  }
  return byId;
}

export function TaskPage() {
  const { taskId } = useParams<{ taskId: string }>();
  const location = useLocation();
  const [schema, setSchema] = useState<Record<string, unknown> | null>(null);
  const [taskTitle, setTaskTitle] = useState<string | null>(null);
  const [taskDescription, setTaskDescription] = useState<string | null>(null);
  const [additionalDescription, setAdditionalDescription] = useState<
    string | null
  >(null);
  const [infoAnchor, setInfoAnchor] = useState<HTMLElement | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [formData, setFormData] = useState<Record<string, unknown>>({});
  const [formErrors, setFormErrors] = useState<FormError[]>([]);
  const requestIdRef = useRef(0);
  const timeoutRef = useRef<number | null>(null);

  useEffect(() => {
    const state = location.state as {
      formData?: Record<string, unknown>;
    } | null;
    setFormData(state?.formData ?? {});
    setFormErrors([]);
  }, [location.state, taskId]);

  useEffect(() => {
    if (!taskId) {
      return () => {};
    }
    let alive = true;
    fetchTaskSchema(taskId)
      .then(
        ({
          title,
          description,
          additional_description: extraDescription,
          schema: s,
        }) => {
          if (!alive) return;
          setSchema(s);
          setTaskTitle(title);
          setTaskDescription(description);
          setAdditionalDescription(extraDescription ?? null);
          setLoadError(null);
        },
      )
      .catch((e) => {
        if (alive) setLoadError(String(e));
      });
    return () => {
      alive = false;
    };
  }, [taskId]);

  useEffect(() => {
    if (!taskId || !schema) {
      return () => {};
    }
    if (timeoutRef.current !== null) {
      window.clearTimeout(timeoutRef.current);
    }
    timeoutRef.current = window.setTimeout(() => {
      const requestId = ++requestIdRef.current;
      validateTaskForm(taskId, formData)
        .then((errors) => {
          if (requestId !== requestIdRef.current) {
            return;
          }
          setFormErrors(errors);
        })
        .catch(() => {
          if (requestId !== requestIdRef.current) {
            return;
          }
          setFormErrors([]);
        });
    }, 200);
    return () => {
      if (timeoutRef.current !== null) {
        window.clearTimeout(timeoutRef.current);
      }
      requestIdRef.current += 1;
    };
  }, [taskId, schema, formData]);

  if (!taskId) return null;

  if (runId) {
    return <ProgressView runId={runId} onReset={() => setRunId(null)} />;
  }

  if (loadError) {
    return (
      <Alert severity="error">
        Failed to load form: {loadError}. Is the API running? (
        <code>make serve</code>)
      </Alert>
    );
  }

  if (!schema) {
    return (
      <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
        <CircularProgress size={24} />
        <Typography>Loading form…</Typography>
      </Box>
    );
  }

  const uiSchema = {
    ...extractUiSchema(schema),
    "ui:globalOptions": {
      enableMarkdownInDescription: true,
    },
  };

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 0.5,
          mb: taskDescription ? 1 : 2,
        }}
      >
        <Typography variant="h6">{taskTitle ?? taskId}</Typography>
        {additionalDescription && (
          <>
            <IconButton
              size="small"
              aria-label="Additional information"
              onClick={(e) => setInfoAnchor(e.currentTarget)}
            >
              <InfoOutlined fontSize="small" />
            </IconButton>
            <Popover
              open={Boolean(infoAnchor)}
              anchorEl={infoAnchor}
              onClose={() => setInfoAnchor(null)}
              anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
              transformOrigin={{ vertical: "top", horizontal: "left" }}
              slotProps={{
                paper: {
                  sx: { maxWidth: 480, p: 2 },
                },
              }}
            >
              <Markdown>{additionalDescription}</Markdown>
            </Popover>
          </>
        )}
      </Box>
      {taskDescription && (
        <Typography
          variant="body2"
          color="text.secondary"
          sx={{ mb: 2, whiteSpace: "pre-wrap" }}
        >
          {taskDescription}
        </Typography>
      )}
      {submitError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {submitError}
        </Alert>
      )}
      <Form
        schema={schema}
        validator={validator}
        formData={formData}
        widgets={widgets}
        uiSchema={uiSchema}
        templates={{ ObjectFieldTemplate: FoldableObjectFieldTemplate }}
        formContext={{ expressionErrors: toExpressionErrors(formErrors) }}
        onChange={({ formData: next }) => {
          setFormData((next ?? {}) as Record<string, unknown>);
        }}
        onSubmit={async ({ formData: submittedData }) => {
          setSubmitError(null);
          try {
            const submitted = await submitTask(
              taskId,
              submittedData as Record<string, unknown>,
            );
            setRunId(submitted.run_id);
          } catch (e: unknown) {
            const err = e as { detail?: unknown };
            const d = err.detail;
            setSubmitError(
              typeof d === "string"
                ? d
                : d !== undefined && d !== null
                  ? JSON.stringify(d, null, 2)
                  : String(e),
            );
          }
        }}
      />
    </Box>
  );
}
