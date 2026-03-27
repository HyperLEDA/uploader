import { BrowserRouter, Route, Routes, useParams } from "react-router-dom";
import Typography from "@mui/material/Typography";
import { HistoryPage } from "./components/HistoryPage";
import { Layout } from "./components/Layout";
import { TaskPage } from "./components/TaskPage";

function TaskPageRoute() {
  const { taskId } = useParams();
  return <TaskPage key={taskId ?? ""} />;
}

function SelectTaskPage() {
  return <Typography>Select task to continue.</Typography>;
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<SelectTaskPage />} />
          <Route path="history" element={<HistoryPage />} />
          <Route path="task/:taskId" element={<TaskPageRoute />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
