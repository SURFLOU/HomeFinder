import re
import time
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as bs
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)

class Scraper:
    def __init__(self):
        self.BASE_URL = "https://www.otodom.pl"
        self.SEARCH_PATH = (
                "/pl/wyniki/sprzedaz/mieszkanie/wiele-lokalizacji"
                "?limit=72"
                "&ownerTypeSingleSelect=ALL"
                "&priceMax=750000"
                "&areaMin=40"
                "&locations=%5Bmazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%2Fochota%2C"
                "mazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%2Fwlochy%5D"
                "&extras=%5BGARAGE%5D"
                "&by=DEFAULT"
                "&direction=DESC"
                "&page={page}"
    )
        self.total_listings = 0
        self.number_of_pages = 0

    def fetch_listings(self, page=1):
        url = f"{self.BASE_URL}{self.SEARCH_PATH.format(page=page)}"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.otodom.pl/",
            "Connection": "keep-alive",
        }

        req = Request(url, headers=headers)
        html = urlopen(req).read()
        soup = bs(html, "html.parser")
        return soup

    def get_number_of_listings(self, soup):
        """ Get the total number of listings from the page using regex. """
        html_text = str(soup)

        pattern = r'"pageDescription"\s*:\s*"Zobacz\s*([\d\s]+)\s*ogłoszeń'
        match = re.search(pattern, html_text)

        if not match:
            return None
        
        
        self.total_listings = int(match.group(1).replace(" ", ""))

        time.sleep(3)
        return self.total_listings
    
    def _calculate_number_of_pages(self):
        if self.total_listings == 0:
            return 0
        self.number_of_pages = (self.total_listings + 71) // 72
        return self.number_of_pages
    
    def parse_listing(self, article):
        try:
            def clean_text(el):
                if el is None:
                    return None
                return el.get_text(" ", strip=True).replace("\xa0", " ").strip()

            def extract_int(text):
                if not text:
                    return None
                digits = re.sub(r"[^\d]", "", text)
                return int(digits) if digits else None

            def extract_float(text):
                if not text:
                    return None
                text = text.replace("\xa0", " ").strip()
                cleaned = re.sub(r"[^\d,\.]", "", text)
                cleaned = cleaned.replace(",", ".")
                return float(cleaned) if cleaned else None

            url_el = article.select_one("a[data-cy='listing-item-link']")
            full_url = self.BASE_URL + url_el["href"] if url_el else None

            main_price = extract_int(clean_text(article.select_one("span[data-sentry-element='MainPrice']")))
            price_per_m2 = extract_int(clean_text(article.select_one("span[data-sentry-element='MainPrice'] + span")))

            short_description = clean_text(article.select_one("p[data-cy='listing-item-title']"))
            description = clean_text(article.select_one("div[data-sentry-element='DescriptionText']"))

            address = clean_text(article.select_one("p[data-sentry-element='StyledParagraph']"))
            street, subdistrict, district = (None, None, None)
            if address:
                parts = [p.strip() for p in address.split(",")]
                street = parts[0] if len(parts) > 0 else None
                subdistrict = parts[1] if len(parts) > 1 else None
                district = parts[2] if len(parts) > 2 else None

            number_of_rooms = None
            area_m2 = None
            floor_number = None

            dl = article.select_one("dl[data-sentry-element='StyledDescriptionList']")
            if dl:
                for dt in dl.select("dt"):
                    label = clean_text(dt)
                    dd = dt.find_next_sibling("dd")
                    value = clean_text(dd)
                    if "inwestycji" in label or "-" in value:
                        continue

                    if label and "pokoje" in value:
                        number_of_rooms = extract_int(value)

                    if label and "Cena za metr" in label and value and "m²" in value:
                        area_m2 = extract_float(value)

                    if label and "Piętro" in label:
                        if value and "parter" in value.lower():
                            floor_number = 0
                        else:
                            floor_number = extract_int(value)
            if main_price is None or main_price < 500000 or full_url is None:
                return None 

            return {
                "url": full_url,
                "main_price": main_price,
                "price_per_m2": price_per_m2,
                "short_description": short_description,
                "description": description,
                "street": street,
                "subdistrict": subdistrict,
                "district": district,
                "number_of_rooms": number_of_rooms,
                "area_m2": area_m2,
                "floor_number": floor_number
            }

        except Exception as e:
            logger.exception("Error parsing listing: %s", e)
            return None


    def extract_listing(self):
        results = []

        for page in range(self._calculate_number_of_pages()):
            soup = self.fetch_listings(page=page + 1)

            articles = soup.find_all("article", {"data-sentry-element": "Container"})

            for article in articles:
                parsed = self.parse_listing(article)
                if parsed:
                    results.append(parsed)

            time.sleep(5)

        return results



if __name__ == "__main__":
    scraper = Scraper()


    total_listings = scraper.get_number_of_listings(scraper.fetch_listings(page=1))
    listings = scraper.extract_listing()

    print(f"Total Listings: {total_listings}")
    logger.info(f"Total Listings: {total_listings}")
    logger.info(f"Found {len(listings)} listings:\n")

    for listing in listings:
        logger.info("%s", listing)