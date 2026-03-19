import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { TaskPage } from "./TaskPage";

const fakeTaskSchema = {
  title: "Fake Test Task",
  schema: {
    type: "object",
    properties: {
      name: {
        type: "string",
        title: "Name",
        description: "Your name",
      },
      count: {
        type: "integer",
        title: "Count",
        default: 1,
      },
    },
    required: ["name"],
  },
};

vi.mock("../api", () => ({
  fetchTaskSchema: vi.fn(() => Promise.resolve(fakeTaskSchema)),
  submitTask: vi.fn(),
}));

function renderTaskPage(taskId: string, formData?: Record<string, unknown>) {
  return render(
    <MemoryRouter
      initialEntries={[
        {
          pathname: `/task/${taskId}`,
          state: formData ? { formData } : undefined,
        },
      ]}
      initialIndex={0}
    >
      <Routes>
        <Route path="/task/:taskId" element={<TaskPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("TaskPage", () => {
  it("shows loading state initially", () => {
    renderTaskPage("fake-task");
    expect(screen.getByText("Loading form…")).toBeInTheDocument();
  });

  it("renders form with task title and schema fields after schema loads", async () => {
    renderTaskPage("fake-task");

    await waitFor(() => {
      expect(screen.getByText("Fake Test Task")).toBeInTheDocument();
    });

    expect(screen.getByLabelText(/name/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/count/i)).toBeInTheDocument();
  });

  it("prefills form when formData is passed via location state", async () => {
    renderTaskPage("fake-task", { name: "Alice", count: 42 });

    await waitFor(() => {
      expect(screen.getByLabelText(/name/i)).toBeInTheDocument();
    });

    const nameInput = screen.getByLabelText(/name/i);
    const countInput = screen.getByLabelText(/count/i);
    expect(nameInput).toHaveValue("Alice");
    expect(countInput).toHaveValue(42);
  });
});
