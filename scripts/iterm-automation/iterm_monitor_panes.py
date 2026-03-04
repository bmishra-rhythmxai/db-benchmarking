#!/usr/bin/env python3
"""
Launch iTerm2 with 6 tabs, 3 panes per tab, each running a system_monitor
command in a ClickHouse/Keeper pod. Requires iTerm2 and the iterm2 Python package.

Install: pip install -r scripts/requirements-iterm.txt   # or: pip install iterm2
Run: python3 scripts/iterm_monitor_panes.py
  (iTerm2 must be running; run from any terminal or from iTerm2's Scripts menu)
"""

from __future__ import annotations

import asyncio
import iterm2

# iTerm2 escape sequence: clear screen + scrollback (not just visible screen)
CLEAR_SCROLLBACK = b"\x1b]1337;ClearScrollback\x07"

# 18 commands: 6 tabs × 3 panes. Order: tab1(pane1,pane2,pane3), tab2(...), ...
COMMANDS = [
    'k exec -it chi-clickhouse-dev-cluster-0-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chi-clickhouse-dev-cluster-1-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chi-clickhouse-dev-cluster-2-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chi-clickhouse-dev-cluster-0-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chi-clickhouse-dev-cluster-1-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chi-clickhouse-dev-cluster-2-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chi-clickhouse-dev-cluster-0-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chi-clickhouse-dev-cluster-1-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chi-clickhouse-dev-cluster-2-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chi-clickhouse-dev-cluster-0-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chi-clickhouse-dev-cluster-1-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chi-clickhouse-dev-cluster-2-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chk-clickhouse-keeper-chk-0-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chk-clickhouse-keeper-chk-0-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chk-clickhouse-keeper-chk-0-2-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net --no-block sda"',
    'k exec -it chk-clickhouse-keeper-chk-0-0-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chk-clickhouse-keeper-chk-0-1-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
    'k exec -it chk-clickhouse-keeper-chk-0-2-0 -- /bin/bash -c "python3 /scripts/system_monitor.py --no-disk"',
]


async def main(connection: iterm2.connection.Connection) -> None:
    app = await iterm2.async_get_app(connection)
    await app.async_activate()

    window = app.current_terminal_window
    if window is None:
        window = await iterm2.Window.async_create(connection)
    if window is None:
        raise SystemExit("Could not create or get an iTerm2 window")

    # Create 6 new tabs (do not use the current tab or any existing one)
    for _ in range(6):
        await window.async_create_tab()

    tabs = window.tabs
    if len(tabs) < 6:
        raise SystemExit(f"Expected 6 tabs, got {len(tabs)}")
    tabs = tabs[-6:]  # use only the 6 we just created

    for tab_index in range(6):
        tab = tabs[tab_index]
        await tab.async_activate()

        # Tab starts with one session; split into 3 panes
        session0 = tab.current_session
        if session0 is None:
            continue
        session1 = await session0.async_split_pane(vertical=False)
        session2 = await session0.async_split_pane(vertical=False)

        # After two horizontal splits: top=session0, middle=session2, bottom=session1
        sessions_ordered = [session0, session2, session1]
        base = tab_index * 3
        for i, session in enumerate(sessions_ordered):
            await session.async_set_variable("user.db_bench_monitor", "1")
            await session.async_activate(select_tab=False)
            await asyncio.sleep(0.05)  # let pane be ready so ClearScrollback is applied
            await session.async_inject(CLEAR_SCROLLBACK)
            cmd = COMMANDS[base + i]
            await session.async_send_text(cmd + "\n")

    # Select first tab
    await tabs[0].async_activate()


if __name__ == "__main__":
    # Second arg True = keep trying to connect (e.g. if iTerm2 is starting)
    iterm2.run_until_complete(main, True)
