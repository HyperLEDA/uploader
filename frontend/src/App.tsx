import {
  BrowserRouter,
  Navigate,
  Route,
  Routes,
  useParams,
} from "react-router-dom";
import { HistoryPage } from "./components/HistoryPage";
import { Layout } from "./components/Layout";
import { TaskPage } from "./components/TaskPage";

function TaskPageRoute() {
  const { taskId } = useParams();
  return <TaskPage key={taskId ?? ""} />;
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/task/upload" replace />} />
          <Route path="history" element={<HistoryPage />} />
          <Route path="task/:taskId" element={<TaskPageRoute />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
