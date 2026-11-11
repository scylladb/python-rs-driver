import asyncio
import sys

from common import (
    SIMPLE_INSERT_QUERY,
    get_simple_data,
)
from python_rs_helpers import init_simple_table, init_with_inserts


async def main():
    num_of_rows = int(sys.argv[1])
    await init_with_inserts(num_of_rows, SIMPLE_INSERT_QUERY, init_simple_table, get_simple_data)


if __name__ == "__main__":
    asyncio.run(main())
