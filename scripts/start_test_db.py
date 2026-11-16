"""Start an embedded PostgreSQL for local tests and print its DSN.

Writes the DSN to /tmp/pg_dsn.txt (used by the pytest fixtures) and keeps the
server running until this process is terminated.  Only needed outside Docker;
the compose stack uses the real postgres image.
"""
from __future__ import annotations

import os
import tempfile
import time

import pgserver


def main() -> None:
    data_dir = os.environ.get("PG_DATA_DIR",
                              tempfile.mkdtemp(prefix="fabpg_"))
    pg = pgserver.get_server(data_dir, cleanup_mode=None)
    dsn = pg.get_uri()
    with open("/tmp/pg_dsn.txt", "w") as fh:
        fh.write(dsn)
    print(f"DATABASE_DSN={dsn}")
    print(f"data dir: {data_dir}")
    print("server is up; Ctrl-C to stop")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
