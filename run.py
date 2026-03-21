import argparse
import threading
import time
import webbrowser

import uvicorn

from server.main import app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    if args.host == "0.0.0.0":
        open_url = f"http://127.0.0.1:{args.port}/"
    else:
        open_url = f"http://{args.host}:{args.port}/"

    if not args.no_browser:

        def open_later() -> None:
            time.sleep(0.5)
            webbrowser.open(open_url)

        threading.Thread(target=open_later, daemon=True).start()

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
    )


if __name__ == "__main__":
    main()
