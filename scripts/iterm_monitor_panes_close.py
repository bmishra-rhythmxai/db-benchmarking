#!/usr/bin/env python3
"""
Close only the panes (sessions) that were opened by iterm_monitor_panes.py.
Uses the user.db_bench_monitor session variable set by that script.
Closes sessions with force=True even if a command is running.

Install: pip install -r scripts/requirements-iterm.txt   # or: pip install iterm2
Run: python3 scripts/iterm_monitor_panes_close.py
"""

from __future__ import annotations

import iterm2

MARKER_VAR = "user.db_bench_monitor"


async def main(connection: iterm2.connection.Connection) -> None:
    app = await iterm2.async_get_app(connection)

    to_close: list[iterm2.session.Session] = []
    for window in app.windows:
        for tab in window.tabs:
            for session in tab.sessions:
                try:
                    val = await session.async_get_variable(MARKER_VAR)
                    if val:
                        to_close.append(session)
                except Exception:
                    continue

    for session in to_close:
        try:
            await session.async_close(force=True)
        except Exception:
            pass

    if to_close:
        print(f"Closed {len(to_close)} monitor pane(s).")
    else:
        print("No db-bench monitor panes found (no sessions with marker).")


if __name__ == "__main__":
    iterm2.run_until_complete(main)
