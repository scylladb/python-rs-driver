import asyncio
import sys

from common import (
    COMPLEX_INSERT_QUERY,
    get_complex_data,
)
from python_rs_helpers import init_complex_table, init_with_inserts


async def main():
    num_of_rows = int(sys.argv[1])
    await init_with_inserts(num_of_rows, COMPLEX_INSERT_QUERY, init_complex_table, get_complex_data)


if __name__ == "__main__":
    asyncio.run(main())
