import { useEffect, useMemo, useState } from 'react'
import { Link, Outlet } from 'react-router-dom'
import Box from '@mui/material/Box'
import Drawer from '@mui/material/Drawer'
import List from '@mui/material/List'
import ListItemButton from '@mui/material/ListItemButton'
import ListItemText from '@mui/material/ListItemText'
import ListSubheader from '@mui/material/ListSubheader'
import Toolbar from '@mui/material/Toolbar'
import Typography from '@mui/material/Typography'
import { fetchTasks, type TaskInfo } from '../api'

const drawerWidth = 260

export function Layout() {
  const [tasks, setTasks] = useState<TaskInfo[]>([])
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    fetchTasks()
      .then(setTasks)
      .catch((e) => setErr(String(e)))
  }, [])

  const byGroup = useMemo(() => {
    const m = new Map<string, TaskInfo[]>()
    for (const t of tasks) {
      const g = t.group || 'Other'
      if (!m.has(g)) m.set(g, [])
      m.get(g)!.push(t)
    }
    return m
  }, [tasks])

  return (
    <Box sx={{ display: 'flex' }}>
      <Drawer
        variant="permanent"
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          '& .MuiDrawer-paper': { width: drawerWidth, boxSizing: 'border-box' },
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
          {[...byGroup.entries()].map(([group, items]) => (
            <Box key={group}>
              <ListSubheader>{group}</ListSubheader>
              {items.map((t) => (
                <ListItemButton key={t.id} component={Link} to={`/task/${t.id}`}>
                  <ListItemText primary={t.title} secondary={t.description} />
                </ListItemButton>
              ))}
            </Box>
          ))}
        </List>
      </Drawer>
      <Box component="main" sx={{ flexGrow: 1, p: 3 }}>
        <Toolbar />
        <Outlet />
      </Box>
    </Box>
  )
}
