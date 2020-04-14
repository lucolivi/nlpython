#https://docs.python.org/3/library/asyncio-queue.html
#https://aiomysql.readthedocs.io
#https://aiohttp.readthedocs.io/en/stable/
#https://realpython.com/python-concurrency/

import sys
import random
import time
import re

import asyncio
import aiohttp
import aiomysql

from async_http import download_page_data


async def connect_db(host, user, password, db):
    loop = asyncio.get_event_loop()
    conn = await aiomysql.connect(host=host, port=3306, user=user, password=password, db=db, loop=loop)
    cur = await conn.cursor()
    return cur, conn

def extract_html_wiki_links(html):
    return re.findall('href="/wiki/([^"]+)"', html, re.IGNORECASE)

#Coroutine to download data
async def downloader(name, hrefs_queue, page_queue):
    global done_downloads
    async with aiohttp.ClientSession() as session:
        while True:
            # Get a "work item" out of the queue.
            href = await hrefs_queue.get()
            try:
                page_data = await download_page_data(session, href, "tet", 60)

                done_downloads += 1
            
                await page_queue.put(page_data)

            finally:
                hrefs_queue.task_done() # Notify the queue that the "work item" has been processed.
    
async def writer(page_queue, cur, conn):
    global done_insertions
    cur, conn = await connect_db('localhost', 'wikidb', 'wikidb', 'wikidb')
    while True:
        href, page_title, page_id, page_html = await page_queue.get()

        try:
            if page_id != -1:
                #Insert Data in articles db
                sql = """INSERT INTO articles (title, html, pageid) VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE title=VALUES(title), html=VALUES(html), pageid=VALUES(pageid)"""

                await cur.executemany(sql, [(page_title, page_html, page_id)])
                await conn.commit()

                new_hrefs = extract_html_wiki_links(page_html)

                sql = "INSERT INTO links (href) VALUES (%s) ON DUPLICATE KEY UPDATE href=href"
                val = [(new_href,) for new_href in new_hrefs]
                await cur.executemany(sql, val)
                await conn.commit()

            #Update hrefs
            sql = "INSERT INTO links (href, pageid) VALUES (%s, %s) ON DUPLICATE KEY UPDATE pageid=VALUES(pageid)"
            await cur.executemany(sql, [(href, page_id)])
            await conn.commit()

            done_insertions += 1
        finally:
            page_queue.task_done()

async def printer():
    while True:
        await asyncio.sleep(1) #Print every second
        print(f"Done downloads: {done_downloads}/{total_itens}\tDone insertions: {done_insertions}/{total_itens}")
        

async def main(host, user, password, db, n_hrefs):
    global total_itens

    #HREFS_QUEUE_SIZE = 10
    #PAGE_QUEUE_SIZE = 50
    N_DOWNLOADERS = 10
    N_WRITERS = 5

    N_HREFS = n_hrefs

    #Create queues for resources to be used
    hrefs_queue = asyncio.Queue()
    page_queue = asyncio.Queue()

    #Connect to db
    cur, conn = await connect_db(host, user, password, db)

    #Get empty hrefs and put in to the queue
    await cur.execute(f"SELECT href FROM links WHERE pageid IS NULL LIMIT {N_HREFS}")
    r = await cur.fetchall()
    for href, in r:
        hrefs_queue.put_nowait(href)

    total_itens = len(r)

    conn.close()

    print(f"Working on {total_itens} itens.")
    
    tasks = [] # Tasks to process the queue concurrently.

    tasks.append(asyncio.create_task(printer()))

    for i in range(N_DOWNLOADERS):
        task = asyncio.create_task(downloader(f'downloader-{i}', hrefs_queue, page_queue))
        tasks.append(task)

    for i in range(N_WRITERS):
        task = asyncio.create_task(writer(page_queue, cur, conn))
        tasks.append(task)

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await hrefs_queue.join()
    await page_queue.join()
    elapsed_time = time.monotonic() - started_at

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

    print("DONE", f"Time elapsed: {elapsed_time}")

    # print('====')
    # print(f'3 workers slept in parallel for {total_slept_for:.2f} seconds')
    # print(f'total expected sleep time: {total_sleep_time:.2f} seconds')

if __name__ == "__main__":
    done_downloads = 0
    done_insertions = 0
    total_itens = 0

    asyncio.run(main('localhost', 'wikidb', 'wikidb', 'wikidb', sys.argv[1]))