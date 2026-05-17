import logging
import sys
import uvicorn

from config import APP_HOST, APP_PORT, DEBUG

if __name__ == "__main__":
    if DEBUG:
        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s] %(levelname)-5s %(name)s:%(lineno)d  %(message)s",
            datefmt="%H:%M:%S",
            stream=sys.stdout,
        )
        print(f"[DEBUG] Starting Data Copilot on {APP_HOST}:{APP_PORT} (debug=True)")
    else:
        logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

    uvicorn.run("src.gateway.api:app", host=APP_HOST, port=APP_PORT, reload=DEBUG)
