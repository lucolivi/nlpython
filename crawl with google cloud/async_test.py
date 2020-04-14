#https://realpython.com/async-io-python/
#https://realpython.com/python-concurrency/
#https://aiohttp.readthedocs.io/en/stable/


import asyncio, requests, re

from WikiDatabase import WikiDatabase

def download_page_data(page, lang, timeout):
    """Function to retrieve a wikipedia page in html form, with its sections"""

    req_params = [
        'action=parse',
        'redirects',
        'format=json',
        'prop=text|displaytitle',
        'page=' + page
    ]

    wikipedia_api_url = "https://" + lang + ".wikipedia.org/w/api.php?" + "&".join(req_params)

    try:
        page_data = requests.get(wikipedia_api_url, timeout=timeout).json()
    except requests.exceptions.ConnectTimeout:
        print("[WARNING]\tPage {} timed out.".format(page))

    #If the object parse is not in the json object, page does not exists
    if not 'parse' in page_data:
        print("[WARNING]\tPage {} does not exists.".format(page))
        page_title = ""
        page_id = -1
        page_html = ""
    else:
        page_title = page_data['parse']['title']
        page_id = page_data['parse']['pageid']
        page_html = page_data['parse']['text']['*']

    return page, page_title, page_id, page_html

def extract_html_wiki_links(html):
    return re.findall('href="/wiki/([^"]+)"', html, re.IGNORECASE)


async def main():
    print('Hello ...')
    await asyncio.sleep(1)
    print('... World!')

async def download_page(href):
    p_href, p_title, p_id, p_html = await download_page_data(href, 'tet', 60)
    print(p_title, p_id)

def download_pages(null_hrefs):
    for href in null_hrefs:
        p_href, p_title, p_id, p_html = download_page_data(href, 'tet', 60)
        print(p_title, p_id)

async def download_pages_async(null_hrefs):
    #queue = asyncio.Queue()
    tasks = []
    for href in null_hrefs:
        tasks.append(download_pages_async(href))
    await asyncio.gather(*tasks)
        #p_href, p_title, p_id, p_html = download_page_data(href, 'tet', 60)
        #print(p_title, p_id)

async def print_value(v):
    print(v)
    await asyncio.sleep(1)

if __name__ == "__main__":
    #wikidb = WikiDatabase("localhost", "wikidb", "wikidb", "wikidb")

    #null_hrefs = wikidb.get_null_hrefs(10)

    #download_pages(null_hrefs)
    #asyncio.run(download_pages_async(null_hrefs))
    #for v in range(3):
        #asyncio.run(print_value(v))
    asyncio.run(asyncio.gather(print_value(2)))





# Python 3.7+
#asyncio.run(main())