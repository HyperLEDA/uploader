import { Fragment, useEffect, useMemo, useState } from "react";
import { Link, Outlet } from "react-router-dom";
import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import Box from "@mui/material/Box";
import Collapse from "@mui/material/Collapse";
import Drawer from "@mui/material/Drawer";
import List from "@mui/material/List";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemText from "@mui/material/ListItemText";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import { fetchTasks, type TaskInfo } from "../api";
import { Hint } from "./Hint";

const drawerWidth = 260;

export function Layout() {
  const [tasks, setTasks] = useState<TaskInfo[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [sectionOpen, setSectionOpen] = useState<Record<string, boolean>>({});

  useEffect(() => {
    fetchTasks()
      .then(setTasks)
      .catch((e) => setErr(String(e)));
  }, []);

  const byGroup = useMemo(() => {
    const m = new Map<string, TaskInfo[]>();
    for (const t of tasks) {
      const g = t.group || "Other";
      if (!m.has(g)) m.set(g, []);
      m.get(g)!.push(t);
    }
    return m;
  }, [tasks]);

  return (
    <Box sx={{ display: "flex" }}>
      <Drawer
        variant="permanent"
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          "& .MuiDrawer-paper": { width: drawerWidth, boxSizing: "border-box" },
        }}
      >
        <Toolbar>
          <Typography variant="subtitle1" fontWeight={600}>
            HyperLEDA Uploader
          </Typography>
        </Toolbar>
        {err && (
          <Typography color="error" variant="caption" sx={{ px: 2, py: 1 }}>
            {err}
          </Typography>
        )}
        <List dense>
          {[...byGroup.entries()].map(([group, items]) => {
            const open = sectionOpen[group] !== false;
            return (
              <Box key={group}>
                <ListItemButton
                  dense
                  onClick={() =>
                    setSectionOpen((prev) => ({
                      ...prev,
                      [group]: !(prev[group] !== false),
                    }))
                  }
                  sx={{ py: 0.25 }}
                >
                  <ListItemText
                    primary={group}
                    primaryTypographyProps={{
                      variant: "subtitle2",
                      fontWeight: 600,
                    }}
                  />
                  {open ? (
                    <ExpandLess fontSize="small" />
                  ) : (
                    <ExpandMore fontSize="small" />
                  )}
                </ListItemButton>
                <Collapse in={open} timeout="auto">
                  <List dense disablePadding>
                    {items.map((t) => {
                      const row = (
                        <ListItemButton
                          component={Link}
                          to={`/task/${t.id}`}
                          sx={{ pl: 2 }}
                        >
                          <ListItemText primary={t.title} />
                        </ListItemButton>
                      );
                      return t.description ? (
                        <Hint
                          key={t.id}
                          hintContent={t.description}
                          position="right"
                        >
                          {row}
                        </Hint>
                      ) : (
                        <Fragment key={t.id}>{row}</Fragment>
                      );
                    })}
                  </List>
                </Collapse>
              </Box>
            );
          })}
        </List>
      </Drawer>
      <Box component="main" sx={{ flexGrow: 1, p: 3 }}>
        <Toolbar />
        <Outlet />
      </Box>
    </Box>
  );
}
