import aiohttp
import asyncio

# async def fetch(session, url):
#     async with session.get(url) as response:
#         return await response.text()

async def download_page_data(session, page, lang, timeout):
    """Function to retrieve a wikipedia page in html form, with its sections"""

    req_params = [
        'action=parse',
        'redirects',
        'format=json',
        'prop=text|displaytitle',
        'page=' + page
    ]

    wikipedia_api_url = "https://" + lang + ".wikipedia.org/w/api.php?" + "&".join(req_params)

    async with session.get(wikipedia_api_url, timeout=timeout) as response:
        page_data = await response.json()

    # try:
    #     page_data = requests.get(wikipedia_api_url, timeout=timeout).json()
    # except requests.exceptions.ConnectTimeout:
    #     print("[WARNING]\tPage {} timed out.".format(page))

    #If the object parse is not in the json object, page does not exists
    if not 'parse' in page_data:
        page_title = ""
        page_id = -1
        page_html = ""
    else:
        page_title = page_data['parse']['title']
        page_id = page_data['parse']['pageid']
        page_html = page_data['parse']['text']['*']

    return page, page_title, page_id, page_html

async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get('http://python.org') as response:
            html = await response.text()
            print(html)

if __name__ == '__main__':
    asyncio.run(main())