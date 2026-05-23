import { useEffect, useRef, useState } from "react";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import LinearProgress from "@mui/material/LinearProgress";
import Paper from "@mui/material/Paper";
import Slider from "@mui/material/Slider";
import Typography from "@mui/material/Typography";
import { cancelRun } from "../api";

type StreamEvent =
  | { type: "progress"; percent: number }
  | { type: "log"; message: string }
  | {
      type: "image";
      data_url: string;
      caption: string | null;
      timestamp: string;
    }
  | { type: "error"; message: string }
  | { type: "done"; message: string }
  | { type: "cancelled"; message: string };

type StreamImage = {
  dataUrl: string;
  caption: string | null;
  timestamp: string;
};

function formatStreamTime(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function isNearBottom(el: HTMLElement, threshold = 48) {
  return el.scrollHeight - el.scrollTop - el.clientHeight <= threshold;
}

export function ProgressView({
  runId,
  onReset,
}: {
  runId: string;
  onReset: () => void;
}) {
  const [percent, setPercent] = useState(0);
  const [logs, setLogs] = useState<string[]>([]);
  const [images, setImages] = useState<StreamImage[]>([]);
  const [imageIndex, setImageIndex] = useState(0);
  const [done, setDone] = useState<string | null>(null);
  const [cancelled, setCancelled] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [cancelPending, setCancelPending] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);
  const logContainerRef = useRef<HTMLDivElement>(null);
  const stickToBottomRef = useRef(true);
  const stickToLatestImageRef = useRef(true);

  function handleLogScroll() {
    const el = logContainerRef.current;
    if (!el) return;
    stickToBottomRef.current = isNearBottom(el);
  }

  function handleImageSliderChange(_: Event, value: number | number[]) {
    const index = value as number;
    setImageIndex(index);
    stickToLatestImageRef.current = index === images.length - 1;
  }

  useEffect(() => {
    stickToBottomRef.current = true;
    stickToLatestImageRef.current = true;
    const es = new EventSource(`/api/runs/${runId}/stream`);
    es.onmessage = (e) => {
      try {
        const ev = JSON.parse(e.data) as StreamEvent;
        if (ev.type === "progress")
          setPercent(Math.min(100, Math.max(0, Math.ceil(ev.percent))));
        else if (ev.type === "log") setLogs((x) => [...x, ev.message]);
        else if (ev.type === "image") {
          setImages((prev) => {
            const next = [
              ...prev,
              {
                dataUrl: ev.data_url,
                caption: ev.caption,
                timestamp: ev.timestamp,
              },
            ];
            if (stickToLatestImageRef.current) {
              setImageIndex(next.length - 1);
            }
            return next;
          });
        } else if (ev.type === "error") {
          setError(ev.message);
          es.close();
        } else if (ev.type === "cancelled") {
          setCancelled(ev.message);
          es.close();
        } else if (ev.type === "done") {
          setDone(ev.message);
          es.close();
        }
      } catch {
        /* ignore */
      }
    };
    es.onerror = () => es.close();
    return () => es.close();
  }, [runId]);

  useEffect(() => {
    const el = logContainerRef.current;
    if (!el || !stickToBottomRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [logs]);

  const canCancel = !done && !error && !cancelled && !cancelPending;
  const currentImage = images[imageIndex];

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 2 }}>
        Run progress
      </Typography>
      <LinearProgress
        variant="determinate"
        value={percent}
        sx={{ mb: 2, height: 8 }}
      />
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        {percent}%
      </Typography>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      {cancelled && (
        <Alert
          severity="warning"
          sx={{ mb: 2, whiteSpace: "pre-wrap", fontFamily: "monospace" }}
        >
          {cancelled}
        </Alert>
      )}
      {cancelError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {cancelError}
        </Alert>
      )}
      <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
        {currentImage ? (
          <>
            <Box
              component="img"
              src={currentImage.dataUrl}
              sx={{ maxWidth: "100%", display: "block" }}
            />
            {currentImage.caption && (
              <Typography variant="caption" display="block" sx={{ mt: 1 }}>
                {currentImage.caption}
              </Typography>
            )}
            {images.length > 1 && (
              <Slider
                sx={{ mt: 2 }}
                min={0}
                max={images.length - 1}
                step={1}
                value={imageIndex}
                onChange={handleImageSliderChange}
                valueLabelDisplay="auto"
                valueLabelFormat={(i) => formatStreamTime(images[i].timestamp)}
              />
            )}
          </>
        ) : (
          <Typography variant="body2" color="text.secondary">
            No charts yet
          </Typography>
        )}
      </Paper>
      {done && (
        <Alert
          severity="success"
          sx={{ mb: 2, whiteSpace: "pre-wrap", fontFamily: "monospace" }}
        >
          {done}
        </Alert>
      )}
      <Paper
        ref={logContainerRef}
        onScroll={handleLogScroll}
        variant="outlined"
        sx={{
          p: 2,
          maxHeight: 360,
          overflow: "auto",
          fontFamily: "monospace",
          fontSize: 13,
          whiteSpace: "pre-wrap",
        }}
      >
        {logs.map((line, i) => (
          <div key={i}>{line}</div>
        ))}
      </Paper>
      <Box sx={{ mt: 2, display: "flex", gap: 1 }}>
        <Button
          variant="outlined"
          color="warning"
          disabled={!canCancel}
          onClick={async () => {
            setCancelError(null);
            setCancelPending(true);
            try {
              await cancelRun(runId);
            } catch (e) {
              setCancelError(String(e));
            } finally {
              setCancelPending(false);
            }
          }}
        >
          Cancel
        </Button>
        <Button variant="outlined" onClick={onReset}>
          Back to form
        </Button>
      </Box>
    </Box>
  );
}
