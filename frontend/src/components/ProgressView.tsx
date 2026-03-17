import { useEffect, useRef, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import LinearProgress from '@mui/material/LinearProgress'
import Paper from '@mui/material/Paper'
import Typography from '@mui/material/Typography'

type StreamEvent =
  | { type: 'progress'; percent: number }
  | { type: 'log'; message: string }
  | { type: 'error'; message: string }
  | { type: 'done'; total_rows: number }

export function ProgressView({
  runId,
  onReset,
}: {
  runId: string
  onReset: () => void
}) {
  const [percent, setPercent] = useState(0)
  const [logs, setLogs] = useState<string[]>([])
  const [done, setDone] = useState<{ total_rows: number } | null>(null)
  const [error, setError] = useState<string | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const es = new EventSource(`/api/runs/${runId}/stream`)
    es.onmessage = (e) => {
      try {
        const ev = JSON.parse(e.data) as StreamEvent
        if (ev.type === 'progress') setPercent(ev.percent)
        else if (ev.type === 'log') setLogs((x) => [...x, ev.message])
        else if (ev.type === 'error') {
          setError(ev.message)
          es.close()
        } else if (ev.type === 'done') {
          setDone({ total_rows: ev.total_rows })
          es.close()
        }
      } catch {
        /* ignore */
      }
    }
    es.onerror = () => es.close()
    return () => es.close()
  }, [runId])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 2 }}>
        Run progress
      </Typography>
      <LinearProgress variant="determinate" value={percent} sx={{ mb: 2, height: 8 }} />
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        {percent}%
      </Typography>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      {done && (
        <Alert severity="success" sx={{ mb: 2 }}>
          Finished. Total rows: {done.total_rows}
        </Alert>
      )}
      <Paper
        variant="outlined"
        sx={{
          p: 2,
          maxHeight: 360,
          overflow: 'auto',
          fontFamily: 'monospace',
          fontSize: 13,
          whiteSpace: 'pre-wrap',
        }}
      >
        {logs.map((line, i) => (
          <div key={i}>{line}</div>
        ))}
        <div ref={bottomRef} />
      </Paper>
      <Button sx={{ mt: 2 }} variant="outlined" onClick={onReset}>
        Back to form
      </Button>
    </Box>
  )
}
