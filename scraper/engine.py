# ==============================================================================
# File: link-scraper-bot/scraper/engine.py
# Description: The core web scraping and parsing logic. (FINAL SCRAPER FIX)
# ==============================================================================

import httpx
import hashlib
from bs4 import BeautifulSoup, Tag
from loguru import logger
from core.config import settings

class ScraperEngine:
    """ Handles fetching and parsing website content. """

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    async def _fetch_page(self, url: str) -> str | None:
        """ Fetches HTML content from a URL using the configured proxy. """
        try:
            transport = httpx.AsyncHTTPTransport(proxy=settings.PROXY_URL, retries=1)
            async with httpx.AsyncClient(transport=transport, headers=self.headers, timeout=30.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text
        except httpx.RequestError as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during fetch for {url}: {e}", exc_info=True)
            return None

    def _parse_links(self, html_content: str) -> tuple[list, str, list]:
        """
        Parses the HTML to find download links, creates a content hash,
        and identifies quality tags based on keywords in the filenames.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        content_wrap = soup.find('div', class_='cPost_contentWrap')
        
        if not content_wrap:
            logger.warning("Could not find 'cPost_contentWrap' div. Scraper might need an update.")
            return [], "", []

        content_hash = hashlib.md5(str(content_wrap).encode('utf-8')).hexdigest()
        
        post_content = content_wrap.find('div', class_='ipsType_richText')
        if not post_content:
            logger.warning("Could not find 'ipsType_richText' div inside the content wrap.")
            return [], content_hash, []

        links = []
        quality_tags = set()

        QUALITY_KEYWORDS = {
            '#PreDVD': ['predvd', 'pre-dvd'],
            '#CamRip': ['hdcam', 'camrip', 'cam'],
            '#TC': ['tc', 'telecine'],
            '#HDRip': ['hdrip', 'hd-rip'],
        }
        
        torrent_anchors = post_content.find_all('a', attrs={'data-fileext': 'torrent'})
        for anchor in torrent_anchors:
            file_name = anchor.text.strip()
            torrent_url = anchor.get('href')
            
            if file_name and torrent_url:
                links.append({'title': file_name, 'url': torrent_url})
                lower_file_name = file_name.lower()
                for tag, keywords in QUALITY_KEYWORDS.items():
                    if any(keyword in lower_file_name for keyword in keywords):
                        quality_tags.add(tag)

        logger.info(f"Successfully parsed {len(links)} .torrent links from post.")
        if quality_tags:
            logger.info(f"Found quality tags: {list(quality_tags)}")

        return links, content_hash, list(quality_tags)

    async def scrape_post(self, url: str) -> tuple[list, str, list] | None:
        """ Public method to scrape a single post. """
        logger.info(f"Scraping post: {url}")
        html = await self._fetch_page(url)
        if html:
            return self._parse_links(html)
        return None

    async def find_latest_posts(self) -> list[str]:
        """ Scrapes the main page to find the latest post URLs. """
        logger.info(f"Checking for latest posts on {settings.TARGET_WEBSITE_URL}")
        html = await self._fetch_page(settings.TARGET_WEBSITE_URL)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # --- NEW, MORE ROBUST SELECTOR ---
        # This selector looks for a common structure on Invision Community forums:
        # an <article> tag with a class 'c-card' containing the title link.
        post_links = soup.select('article.c-card h4.ipsDataItem_title a')
        
        urls = [link['href'] for link in post_links if link.has_attr('href')]
        
        # Fallback to the old selector if the new one finds nothing
        if not urls:
            logger.warning("New selector found 0 posts. Trying fallback selector.")
            post_links = soup.select('div[data-row-id] h4.ipsDataItem_title > a')
            urls = [link['href'] for link in post_links if link.has_attr('href')]

        logger.info(f"Found {len(urls)} potential post links on the main page.")
        return urls[:15] # Increased limit to 15 to be safe
