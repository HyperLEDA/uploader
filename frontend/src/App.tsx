import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { TaskPage } from './components/TaskPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/task/upload" replace />} />
          <Route path="task/:taskId" element={<TaskPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
