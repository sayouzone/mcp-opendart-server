import asyncio
import logging
import os
from typing import List, Dict, Any

from fastmcp import FastMCP

from opendarts import mcp

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(levelname)s]: %(message)s", level=logging.INFO)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    logger.info(f"🚀 Stocks MCP server started on port {port}")
    #mcp.run()
    """"""
    asyncio.run(
        mcp.run_async(
            transport="http",
            host="0.0.0.0",
            port=port,
        )
    )
    """"""