import io
import trafilatura
from warcio.archiveiterator import WARCIterator
from commoncrawl import BASE_URL, CCDownloader, Downloader

class Scraper:
    def scrape(self, filename, offset, length):
        downloader = CCDownloader(BASE_URL)
        data = downloader.download_and_unzip(filename, offset, length)
        iterator = WARCIterator(io.BytesIO(data))
        for record in iterator:
            if record.rec_type == "response":
                content = record.content_stream().read()
                yield trafilatura.extract(content)
