import base64
import io
from dataclasses import dataclass

import matplotlib
import matplotlib.figure
import matplotlib.pyplot as plt

matplotlib.use("Agg")


@dataclass(frozen=True)
class LogEvent:
    message: str


@dataclass(frozen=True)
class ProgressEvent:
    percent: float


@dataclass(frozen=True)
class DoneEvent:
    message: str


@dataclass(frozen=True)
class ErrorEvent:
    message: str


@dataclass(frozen=True)
class ImageEvent:
    data_url: str
    caption: str | None = None


Event = LogEvent | ProgressEvent | DoneEvent | ErrorEvent | ImageEvent


def image_event_from_figure(
    fig: matplotlib.figure.Figure,
    caption: str | None = None,
    dpi: int = 120,
) -> ImageEvent:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return ImageEvent(
        data_url=f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode('ascii')}",
        caption=caption,
    )
