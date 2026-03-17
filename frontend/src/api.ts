export type TaskInfo = {
  id: string;
  title: string;
  description: string;
  group: string;
};

export async function fetchTasks(): Promise<TaskInfo[]> {
  const r = await fetch("/api/tasks");
  if (!r.ok) throw new Error(`tasks: ${r.status}`);
  return r.json();
}

export async function fetchTaskSchema(
  taskId: string,
): Promise<Record<string, unknown>> {
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
