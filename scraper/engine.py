# ==============================================================================
# File: link-scraper-bot/scraper/engine.py
# Description: The core web scraping and parsing logic. (ADVANCED ENGINE)
# ==============================================================================

import httpx
import hashlib
import re
from bs4 import BeautifulSoup, Tag
from loguru import logger
from core.config import settings

class ScraperEngine:
    """ Handles fetching and parsing website content with advanced, resilient logic. """

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    async def _fetch_page(self, url: str) -> str | None:
        """ Fetches HTML content from a URL using the configured proxy. """
        try:
            transport = httpx.AsyncHTTPTransport(proxy=settings.PROXY_URL, retries=2)
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

    def _parse_links(self, html_content: str) -> tuple[list, str, list, dict]:
        """
        Advanced parser to find download links, create a content hash,
        and extract rich metadata like quality, languages, and file sizes.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        content_wrap = soup.find('div', class_='cPost_contentWrap')
        
        if not content_wrap:
            logger.warning("Could not find 'cPost_contentWrap' div. Scraper might need an update.")
            return [], "", [], {}

        content_hash = hashlib.md5(str(content_wrap).encode('utf-8')).hexdigest()
        
        post_content = content_wrap.find('div', class_='ipsType_richText')
        if not post_content:
            logger.warning("Could not find 'ipsType_richText' div inside the content wrap.")
            return [], content_hash, [], {}

        links = []
        quality_tags = set()
        metadata = {'language_tags': set(), 'file_sizes': set()}

        QUALITY_KEYWORDS = {
            '#PreDVD': ['predvd', 'pre-dvd'],
            '#CamRip': ['hdcam', 'camrip', 'cam'],
            '#TC': ['tc', 'telecine'],
            '#HDRip': ['hdrip', 'hd-rip'],
            '#WEBDL': ['web-dl', 'webdl'],
        }
        
        torrent_anchors = post_content.find_all('a', attrs={'data-fileext': 'torrent'})
        for anchor in torrent_anchors:
            file_name = anchor.text.strip()
            torrent_url = anchor.get('href')
            
            if file_name and torrent_url:
                links.append({'title': file_name, 'url': torrent_url})
                lower_file_name = file_name.lower()

                # --- Metadata and Quality Analysis ---
                for tag, keywords in QUALITY_KEYWORDS.items():
                    if any(keyword in lower_file_name for keyword in keywords):
                        quality_tags.add(tag)
                
                # Extract languages (e.g., [Tam + Tel + Hin])
                lang_match = re.search(r'\[([^\]]*)\]', file_name)
                if lang_match:
                    langs = [lang.strip() for lang in lang_match.group(1).split('+')]
                    metadata['language_tags'].update(langs)

                # Extract file sizes (e.g., 3.7GB or 450MB)
                size_match = re.search(r'(\d+(\.\d+)?)(gb|mb)', lower_file_name)
                if size_match:
                    metadata['file_sizes'].add(size_match.group(0).upper())

        logger.info(f"Successfully parsed {len(links)} .torrent links from post.")
        if quality_tags:
            logger.info(f"Found quality tags: {list(quality_tags)}")
        if metadata['language_tags'] or metadata['file_sizes']:
            logger.info(f"Extracted metadata: {metadata}")

        # Convert sets to lists for consistent return type
        final_metadata = {k: list(v) for k, v in metadata.items()}
        return links, content_hash, list(quality_tags), final_metadata

    async def scrape_post(self, url: str) -> tuple[list, str, list, dict] | None:
        """ Public method to scrape a single post. """
        logger.info(f"Scraping post: {url}")
        html = await self._fetch_page(url)
        if html:
            return self._parse_links(html)
        return None

    async def find_latest_posts(self) -> list[str]:
        """ 
        Scrapes the main page to find the latest post URLs using multiple,
        resilient methods.
        """
        logger.info(f"Checking for latest posts on {settings.TARGET_WEBSITE_URL}")
        html = await self._fetch_page(settings.TARGET_WEBSITE_URL)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        found_urls = set()

        # --- Method 1: Primary Selector (Specific) ---
        selector1_links = soup.select('article.c-card h4.ipsDataItem_title a')
        if selector1_links:
            logger.info(f"Found {len(selector1_links)} links with primary selector.")
            for link in selector1_links:
                if link.has_attr('href'):
                    found_urls.add(link['href'])
        
        # --- Method 2: Fallback Selector (Slightly less specific) ---
        selector2_links = soup.select('div[data-row-id] h4.ipsDataItem_title > a')
        if selector2_links:
            logger.info(f"Found {len(selector2_links)} links with fallback selector.")
            for link in selector2_links:
                if link.has_attr('href'):
                    found_urls.add(link['href'])

        # --- Method 3: Generic Pattern Matching (Most reliable fallback) ---
        if not found_urls:
            logger.warning("Selectors found 0 posts. Trying generic pattern matching.")
            # This looks for any link containing the typical forum topic structure.
            pattern_links = soup.find_all('a', href=re.compile(r'/forums/topic/\d+'))
            if pattern_links:
                logger.info(f"Found {len(pattern_links)} links with generic pattern matching.")
                for link in pattern_links:
                    found_urls.add(link['href'])

        final_urls = list(found_urls)
        logger.success(f"Found a total of {len(final_urls)} unique potential post links on the main page.")
        return final_urls[:20] # Increased limit to 20 to be safe
