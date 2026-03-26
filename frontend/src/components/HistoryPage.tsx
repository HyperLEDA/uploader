import { Fragment, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import Collapse from "@mui/material/Collapse";
import CircularProgress from "@mui/material/CircularProgress";
import IconButton from "@mui/material/IconButton";
import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Typography from "@mui/material/Typography";
import {
  fetchHistory,
  fetchTasks,
  type HistoryEntry,
  type TaskInfo,
} from "../api";

function formatTimestamp(value: string): string {
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return value;
  }
  return dt.toLocaleString();
}

export function HistoryPage() {
  const navigate = useNavigate();
  const [history, setHistory] = useState<HistoryEntry[] | null>(null);
  const [tasks, setTasks] = useState<TaskInfo[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openRows, setOpenRows] = useState<Record<string, boolean>>({});

  useEffect(() => {
    let alive = true;
    Promise.all([fetchHistory(), fetchTasks()])
      .then(([historyItems, taskItems]) => {
        if (!alive) return;
        setHistory(historyItems);
        setTasks(taskItems);
        setError(null);
      })
      .catch((e) => {
        if (alive) setError(String(e));
      });
    return () => {
      alive = false;
    };
  }, []);

  const rerunnableTaskIds = useMemo(() => {
    const ids = new Set<string>();
    for (const task of tasks ?? []) {
      if (task.rerunnable) {
        ids.add(task.id);
      }
    }
    return ids;
  }, [tasks]);

  if (error) {
    return <Alert severity="error">Failed to load history: {error}</Alert>;
  }

  if (!history || !tasks) {
    return (
      <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
        <CircularProgress size={24} />
        <Typography>Loading history...</Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ maxWidth: 1100 }}>
      <Typography variant="h6" sx={{ mb: 2 }}>
        Run history
      </Typography>
      {history.length === 0 ? (
        <Alert severity="info">No runs recorded yet on this machine.</Alert>
      ) : (
        <TableContainer component={Paper} variant="outlined">
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell sx={{ width: 40 }} />
                <TableCell>Timestamp</TableCell>
                <TableCell>Task</TableCell>
                <TableCell>Status</TableCell>
                <TableCell align="right">Action</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {history.map((entry, idx) => {
                const rerunnable = rerunnableTaskIds.has(entry.task_id);
                const rowId = `${entry.timestamp}-${entry.task_id}-${idx}`;
                const isOpen = openRows[rowId] ?? false;
                return (
                  <Fragment key={rowId}>
                    <TableRow>
                      <TableCell
                        sx={{ whiteSpace: "nowrap", width: 40, py: 0.5 }}
                      >
                        <IconButton
                          size="small"
                          onClick={() =>
                            setOpenRows((prev) => ({
                              ...prev,
                              [rowId]: !isOpen,
                            }))
                          }
                          aria-label={
                            isOpen ? "Collapse message" : "Expand message"
                          }
                        >
                          {isOpen ? <ExpandLess /> : <ExpandMore />}
                        </IconButton>
                      </TableCell>
                      <TableCell sx={{ whiteSpace: "nowrap" }}>
                        {formatTimestamp(entry.timestamp)}
                      </TableCell>
                      <TableCell>{entry.task_title}</TableCell>
                      <TableCell>
                        <Chip
                          size="small"
                          color={
                            entry.status === "success"
                              ? "success"
                              : entry.status === "cancelled"
                                ? "warning"
                                : "error"
                          }
                          label={entry.status}
                        />
                      </TableCell>
                      <TableCell align="right">
                        <Box sx={{ display: "inline-flex", gap: 1 }}>
                          <Button
                            variant="outlined"
                            size="small"
                            disabled={!rerunnable}
                            onClick={() =>
                              navigate(`/task/${entry.task_id}`, {
                                state: { formData: entry.inputs },
                              })
                            }
                          >
                            Rerun
                          </Button>
                        </Box>
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell colSpan={5} sx={{ py: 0, borderBottom: 0 }}>
                        <Collapse in={isOpen} timeout="auto" unmountOnExit>
                          <Box
                            sx={{
                              py: 1.5,
                              whiteSpace: "pre-wrap",
                              fontFamily: "monospace",
                            }}
                          >
                            {entry.message}
                          </Box>
                        </Collapse>
                      </TableCell>
                    </TableRow>
                  </Fragment>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
}
