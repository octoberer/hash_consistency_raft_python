import asyncio

from index import index
from util.settings import new_server_address

if __name__ == '__main__':
    asyncio.run(index(new_server_address,True))