export type TaskInfo = {
  id: string;
  title: string;
  description: string;
  additional_description?: string | null;
  group: string;
  rerunnable: boolean;
};

export async function fetchTasks(): Promise<TaskInfo[]> {
  const r = await fetch("/api/tasks");
  if (!r.ok) throw new Error(`tasks: ${r.status}`);
  return r.json();
}

export type TaskSchemaResponse = {
  title: string;
  description: string;
  additional_description?: string | null;
  schema: Record<string, unknown>;
};

export async function fetchTaskSchema(
  taskId: string,
): Promise<TaskSchemaResponse> {
  const r = await fetch(`/api/tasks/${taskId}/schema`);
  if (!r.ok) throw new Error(`schema: ${r.status}`);
  return r.json();
}

export async function submitTask(
  taskId: string,
  formData: Record<string, unknown>,
): Promise<{ run_id: string }> {
  const r = await fetch(`/api/tasks/${taskId}/submit`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(formData),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    const detail = (body as { detail?: unknown }).detail ?? body;
    throw Object.assign(new Error("submit failed"), {
      status: r.status,
      detail,
    });
  }
  return r.json();
}

export async function cancelRun(runId: string): Promise<void> {
  const r = await fetch(`/api/runs/${runId}/cancel`, {
    method: "POST",
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    const detail = (body as { detail?: unknown }).detail ?? body;
    throw Object.assign(new Error("cancel failed"), {
      status: r.status,
      detail,
    });
  }
}

export type HistoryEntry = {
  timestamp: string;
  task_id: string;
  task_title: string;
  inputs: Record<string, unknown>;
  status: "success" | "error" | "cancelled";
  message: string;
  details?: string | null;
};

export type FormError = {
  path: string[];
  message: string;
  start_line: number;
  start_column: number;
  end_line: number;
  end_column: number;
};

export async function validateTaskForm(
  taskId: string,
  formData: Record<string, unknown>,
): Promise<FormError[]> {
  const r = await fetch(`/api/tasks/${taskId}/validate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(formData),
  });
  if (!r.ok) throw new Error(`validate: ${r.status}`);
  return r.json();
}

export async function fetchHistory(): Promise<HistoryEntry[]> {
  const r = await fetch("/api/history");
  if (!r.ok) throw new Error(`history: ${r.status}`);
  return r.json();
}
