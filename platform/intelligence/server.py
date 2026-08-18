"""C5 entrypoints for the MCP server.

Two transports over the same three tools:

* ``stdio`` — for Claude Desktop and other local MCP hosts. This is the default
  and the one a desktop ``mcpServers`` config points at::

      {
        "mcpServers": {
          "ecommerce-intelligence": {
            "command": "uv",
            "args": ["run", "python", "-m", "platform.intelligence.server"]
          }
        }
      }

* ``streamable-http`` — run the standalone FastMCP HTTP app (equivalent to the
  ``/mcp`` mount in :mod:`platform.intelligence.app`, served on its own port).

Usage::

    python -m platform.intelligence.server               # stdio
    python -m platform.intelligence.server --http        # streamable-http :8000
"""

from __future__ import annotations

import argparse
from platform.intelligence.tools import build_mcp


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="C5 intelligence MCP server")
    parser.add_argument(
        "--http",
        action="store_true",
        help="Serve over streamable-http instead of stdio.",
    )
    args = parser.parse_args(argv)

    mcp = build_mcp()
    if args.http:
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
