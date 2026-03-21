import argparse
import threading
import webbrowser

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="HyperLEDA Uploader")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    if not args.no_browser:
        url = f"http://{args.host}:{args.port}/"
        threading.Timer(1.5, webbrowser.open, args=[url]).start()

    uvicorn.run("server.main:app", host=args.host, port=args.port)
