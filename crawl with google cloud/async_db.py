import asyncio
import aiomysql

async def main(*args):
    loop = asyncio.get_event_loop()
    conn = await aiomysql.connect(host=args[0], port=3306, user=args[1], password=args[2], db=args[3], loop=loop)
    cur = await conn.cursor()
    await cur.execute("SELECT href FROM links LIMIT 10")
    print(cur.description)
    r = await cur.fetchall()
    print(r)    
    await cur.close()
    conn.close()


if __name__ == "__main__":
    asyncio.run(main('localhost', 'wikidb', 'wikidb', 'wikidb'))

