import asyncio

from python_rs_helpers import init_simple_table


async def main():
    await init_simple_table()


if __name__ == "__main__":
    asyncio.run(main())
