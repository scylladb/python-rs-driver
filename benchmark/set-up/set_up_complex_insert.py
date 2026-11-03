import asyncio

from python_rs_helpers import init_complex_table


async def main():
    await init_complex_table()


if __name__ == "__main__":
    asyncio.run(main())
