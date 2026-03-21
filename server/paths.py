from pathlib import Path

DATA_DIR = Path.home() / ".hyperleda-uploader"
DATA_DIR.mkdir(parents=True, exist_ok=True)
