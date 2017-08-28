from base64 import urlsafe_b64encode
from pathlib import Path
from urllib.parse import urlsplit

import ujson
import aiofiles


STORAGE_PATH = "pages"


async def save_page(run_timestamp, target, html):
    url_parts = urlsplit(target.url)

    platform = url_parts.netloc.split('.')[1]
    concise_url = "{0.scheme}://{0.netloc}{0.path}".format(url_parts)

    dirpath = Path(STORAGE_PATH) / platform / str(run_timestamp)
    dirpath.mkdir(parents=True, exist_ok=True)

    filename = urlsafe_b64encode(concise_url.encode()).decode() + ".html"
    filepath = dirpath / filename

    async with aiofiles.open(filepath, 'w') as f:
        await f.write('\n'.join([html, ujson.dumps(target.data)]))
