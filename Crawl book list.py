"""Spider to extract URL's of books from a Listopia list on Goodreads"""

import scrapy
from scrapy import signals
import re
import datetime
from scrapy import Field
from scrapy.loader import ItemLoader

from itemloaders.processors import Identity, Compose, MapCompose, TakeFirst, Join

from dateutil.parser import parse as dateutil_parse
from w3lib.html import remove_tags

def num_page_extractor(num_pages):
    if num_pages:
        return num_pages.split()[0]
    return None


def safe_parse_date(date):
    try:
        date = dateutil_parse(date, fuzzy=True, default=datetime.datetime.min)
        date = date.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        date = None

    return date


def extract_publish_dates(maybe_dates):
    maybe_dates = [s for s in maybe_dates if "published" in s.lower()]
    return [safe_parse_date(date) for date in maybe_dates]


def extract_year(s):
    s = s.lower().strip()
    match = re.match(".*first published.*(\d{4})", s)
    if match:
        return match.group(1)


def extract_ratings(txt):
    """Extract the rating histogram from embedded Javascript code
        The embedded code looks like this:
        |----------------------------------------------------------|
        | renderRatingGraph([6, 3, 2, 2, 1]);                      |
        | if ($('rating_details')) {                               |
        |   $('rating_details').insert({top: $('rating_graph')})   |
        |  }                                                       |
        |----------------------------------------------------------|
    """
    codelines = "".join(txt).split(";")
    rating_code = [
        line.strip() for line in codelines if "renderRatingGraph" in line
    ]
    if not rating_code:
        return None
    rating_code = rating_code[0]
    rating_array = rating_code[rating_code.index("[") +
                               1:rating_code.index("]")]
    ratings = {5 - i: int(x) for i, x in enumerate(rating_array.split(","))}
    return ratings


def filter_asin(asin):
    if asin and len(str(asin)) == 10:
        return asin
    return None


def isbn_filter(isbn):
    if isbn and len(str(isbn)) == 10 and isbn.isdigit():
        return isbn


def isbn13_filter(isbn):
    if isbn and len(str(isbn)) == 13 and isbn.isdigit():
        return isbn


def filter_empty(vals):
    return [v.strip() for v in vals if v.strip()]


def split_by_newline(txt):
    return txt.split("\n")


class BookItem(scrapy.Item):
    # Scalars
    url = Field()

    title = Field(input_processor=MapCompose(str.strip))
    author = Field(input_processor=MapCompose(str.strip))

    num_ratings = Field(input_processor=MapCompose(str.strip, int))
    num_reviews = Field(input_processor=MapCompose(str.strip, int))
    avg_rating = Field(input_processor=MapCompose(str.strip, float))
    num_pages = Field(
        input_processor=MapCompose(str.strip, num_page_extractor, int))

    language = Field(input_processor=MapCompose(str.strip))
    publish_date = Field(input_processor=extract_publish_dates)

    original_publish_year = Field(
        input_processor=MapCompose(extract_year, int))

    isbn = Field(input_processor=MapCompose(str.strip, isbn_filter))
    isbn13 = Field(input_processor=MapCompose(str.strip, isbn13_filter))
    asin = Field(input_processor=MapCompose(filter_asin))

    series = Field()

    # Lists
    awards = Field(output_processor=Identity())
    places = Field(output_processor=Identity())
    characters = Field(output_processor=Identity())
    genres = Field(output_processor=Compose(set, list))

    # Dicts
    rating_histogram = Field(input_processor=MapCompose(extract_ratings))


class BookLoader(ItemLoader):
    default_output_processor = TakeFirst()


class AuthorItem(scrapy.Item):
    # Scalars
    url = Field()

    name = Field()
    birth_date = Field(input_processor=MapCompose(safe_parse_date))
    death_date = Field(input_processor=MapCompose(safe_parse_date))

    avg_rating = Field(serializer=float)
    num_ratings = Field(serializer=int)
    num_reviews = Field(serializer=int)

    # Lists
    genres = Field(output_processor=Compose(set, list))
    influences = Field(output_processor=Compose(set, list))

    # Blobs
    about = Field(
        # Take the first match, remove HTML tags, convert to list of lines, remove empty lines, remove the "edit data" prefix
        input_processor=Compose(TakeFirst(), remove_tags, split_by_newline,
                                filter_empty, lambda s: s[1:]),
        output_processor=Join())


class AuthorLoader(ItemLoader):
    default_output_processor = TakeFirst()
class AuthorSpider(scrapy.Spider):
    name = "author"

    def _set_crawler(self, crawler):
        super()._set_crawler(crawler)
        crawler.signals.connect(self.item_scraped_callback, signal=signals.item_scraped)

    def __init__(self, author_crawl="False", item_scraped_callback=None):
        # The default arg for author_crawl is intentionally a string
        # since command line arguments to scrapy are strings
        super().__init__()

        self.item_scraped_callback = item_scraped_callback

        # Convert author_crawl to str
        # just in case a boolean was passed in programmatically
        self.author_crawl = str(author_crawl).lower() in {"true", "yes", "y"}
        if self.author_crawl:
            self.start_urls = ["https://www.goodreads.com/", "https://www.goodreads.com/author/on_goodreads"]

    def parse(self, response):
        url = response.request.url

        # Don't follow blog pages
        if "/blog?page=" in url:
            return

        if url.startswith("https://www.goodreads.com/author/show/"):
            yield self.parse_author(response)

        # Exit early if an author crawl is not enabled
        if not self.author_crawl:
            return

        # If an author crawl is enabled, we crawl similar authors for this author,
        # authors that influenced this author,
        # as well as any URL that looks like an author bio page
        influence_author_urls = response.css('div.dataItem>span>a[href*="/author/show"]::attr(href)').extract()

        for author_url in influence_author_urls:
            yield response.follow(author_url, callback=self.parse)

        similar_authors = response.css('a[href*="/author/similar"]::attr(href)').extract_first()
        if similar_authors:
            yield response.follow(similar_authors, callback=self.parse)

        all_authors_on_this_page = response.css('a[href*="/author/show"]::attr(href)').extract()
        for author_url in all_authors_on_this_page:
            yield response.follow(author_url, callback=self.parse)

    def parse_author(self, response):
        loader = AuthorLoader(AuthorItem(), response=response)
        loader.add_value('url', response.request.url)
        loader.add_css("name", 'h1.authorName>span[itemprop="name"]::text')

        loader.add_css("birth_date", 'div.dataItem[itemprop="birthDate"]::text')
        loader.add_css("death_date", 'div.dataItem[itemprop="deathDate"]::text')

        loader.add_css("genres", 'div.dataItem>a[href*="/genres/"]::text')
        loader.add_css("influences", 'div.dataItem>span>a[href*="/author/show"]::text')

        loader.add_css("avg_rating", 'span.average[itemprop="ratingValue"]::text')
        loader.add_css("num_reviews", 'span[itemprop="reviewCount"]::attr(content)')
        loader.add_css("num_ratings", 'span[itemprop="ratingCount"]::attr(content)')

        loader.add_css("about", 'div.aboutAuthorInfo')

        return loader.load_item()

class BookSpider(scrapy.Spider):
    """Extract information from a /book/show type page on Goodreads
        Technically, this is not a Spider in the sense that
        it is never initialized by scrapy. Consequently,
         - its from_crawler method is never invoked
         - its `crawler` attribute is not set
         - it does not have a list of start_urls or start_requests
         - running this spider with scrapy crawl will do nothing
    """
    name = "book"

    def __init__(self):
        super().__init__()
        self.author_spider = AuthorSpider()

    def parse(self, response):
        loader = BookLoader(BookItem(), response=response)

        loader.add_value('url', response.request.url)

        loader.add_css("title", "#bookTitle::text")
        loader.add_css("author", "a.authorName>span::text")

        loader.add_css("num_ratings", "[itemprop=ratingCount]::attr(content)")
        loader.add_css("num_reviews", "[itemprop=reviewCount]::attr(content)")
        loader.add_css("avg_rating", "span[itemprop=ratingValue]::text")
        loader.add_css("num_pages", "span[itemprop=numberOfPages]::text")

        loader.add_css("language", "div[itemprop=inLanguage]::text")
        loader.add_css('publish_date', 'div.row::text')
        loader.add_css('publish_date', 'nobr.greyText::text')

        loader.add_css('original_publish_year', 'nobr.greyText::text')

        loader.add_css("genres", 'div.left>a.bookPageGenreLink[href*="/genres/"]::text')
        loader.add_css("awards", "a.award::text")
        loader.add_css('characters', 'a[href*="/characters/"]::text')
        loader.add_css('places', 'div.infoBoxRowItem>a[href*=places]::text')
        loader.add_css('series', 'div.infoBoxRowItem>a[href*="/series/"]::text')

        loader.add_css('asin', 'div.infoBoxRowItem[itemprop=isbn]::text')
        loader.add_css('isbn', 'div.infoBoxRowItem[itemprop=isbn]::text')
        loader.add_css('isbn', 'span[itemprop=isbn]::text')
        loader.add_css('isbn', 'div.infoBoxRowItem::text')
        loader.add_css('isbn13', 'div.infoBoxRowItem[itemprop=isbn]::text')
        loader.add_css('isbn13', 'span[itemprop=isbn]::text')
        loader.add_css('isbn13', 'div.infoBoxRowItem::text')

        loader.add_css('rating_histogram', 'script[type*="protovis"]::text')

        yield loader.load_item()

        author_url = response.css('a.authorName::attr(href)').extract_first()
        yield response.follow(author_url, callback=self.author_spider.parse)


class ListSpider(scrapy.Spider):
    """Extract and crawl URLs of books from a Listopia list on Goodreads
        This subsequently passes on the URLs to BookSpider.
        Consequently, this spider also yields BookItem's and AuthorItem's.
    """
    name = "list"

    goodreads_list_url = "https://www.goodreads.com/list/show/{}?page={}"

    def _set_crawler(self, crawler):
        super()._set_crawler(crawler)
        crawler.signals.connect(self.item_scraped_callback, signal=signals.item_scraped)

    def __init__(self, list_name, start_page_no, end_page_no, url=None, item_scraped_callback=None):
        super().__init__()
        self.book_spider = BookSpider()
        self.item_scraped_callback = item_scraped_callback

        self.start_urls = []
        for page_no in range(int(start_page_no), int(end_page_no) + 1):
            list_url = self.goodreads_list_url.format(list_name, page_no)
            self.start_urls.append(list_url)

    def parse(self, response):
        book_urls = response.css("a.bookTitle::attr(href)").extract()

        for book_url in book_urls:
            yield response.follow(book_url, callback=self.book_spider.parse)