import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import Form from "@rjsf/mui";
import validator from "@rjsf/validator-ajv8";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import Typography from "@mui/material/Typography";
import { fetchTaskSchema, submitTask } from "../api";
import { ProgressView } from "./ProgressView";

export function TaskPage() {
  const { taskId } = useParams<{ taskId: string }>();
  const [schema, setSchema] = useState<Record<string, unknown> | null>(null);
  const [taskTitle, setTaskTitle] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [runId, setRunId] = useState<string | null>(null);

  useEffect(() => {
    if (!taskId) return;
    let alive = true;
    fetchTaskSchema(taskId)
      .then(({ title, schema: s }) => {
        if (!alive) return;
        setSchema(s);
        setTaskTitle(title);
        setLoadError(null);
      })
      .catch((e) => {
        if (alive) setLoadError(String(e));
      });
    return () => {
      alive = false;
    };
  }, [taskId]);

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

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 2 }}>
        {taskTitle ?? taskId}
      </Typography>
      {submitError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {submitError}
        </Alert>
      )}
      <Form
        schema={schema}
        validator={validator}
        onSubmit={async ({ formData }) => {
          setSubmitError(null);
          try {
            const { run_id } = await submitTask(
              taskId,
              formData as Record<string, unknown>,
            );
            setRunId(run_id);
          } catch (e: unknown) {
            const err = e as { detail?: unknown };
            const d = err.detail;
            setSubmitError(
              typeof d === "string"
                ? d
                : d != null
                  ? JSON.stringify(d, null, 2)
                  : String(e),
            );
          }
        }}
      />
    </Box>
  );
}
