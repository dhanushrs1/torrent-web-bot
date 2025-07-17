# ==============================================================================
# File: link-scraper-bot/scraper/engine.py
# Description: The core web scraping and parsing logic. (ADVANCED PARSING)
# ==============================================================================

import httpx
import hashlib
import re
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

    def _parse_links(self, html_content: str, filter_quality: list = None, filter_language: list = None, min_size_mb: int = None, max_size_mb: int = None) -> tuple[list, str, list, dict]:
        """
        Parses the HTML to find download links, creates a content hash,
        and identifies quality tags and other metadata. Supports advanced filtering.
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
        language_tags = set()
        file_sizes = []
        # --- Define keywords to look for and their corresponding tags ---
        QUALITY_KEYWORDS = {
            '#PreDVD': ['predvd', 'pre-dvd'],
            '#CamRip': ['hdcam', 'camrip', 'cam'],
            '#TC': ['tc', 'telecine'],
            '#HDRip': ['hdrip', 'hd-rip'],
            '#1080p': ['1080p'],
            '#720p': ['720p'],
            '#4K': ['2160p', '4k'],
        }
        LANGUAGE_KEYWORDS = {
            'Tamil': ['tam', 'tamil'],
            'Telugu': ['tel', 'telugu'],
            'Hindi': ['hin', 'hindi'],
            'English': ['eng', 'english'],
            'Malayalam': ['mal', 'malayalam'],
            'Kannada': ['kan', 'kannada'],
            'Bengali': ['ben', 'bengali'],
        }
        # --- Advanced Torrent Link Scraping ---
        # Find all torrent links directly first.
        torrent_anchors = post_content.find_all('a', attrs={'data-fileext': 'torrent'})
        for anchor in torrent_anchors:
            file_name = anchor.text.strip()
            torrent_url = anchor.get('href')
            # Try to extract file size from file name (e.g., "1.2GB", "700MB")
            size_match = None
            if file_name:
                size_match = re.search(r'(\d+(?:\.\d+)?)(GB|MB)', file_name, re.IGNORECASE)
            file_size_mb = None
            if size_match:
                size_val = float(size_match.group(1))
                size_unit = size_match.group(2).upper()
                file_size_mb = size_val * 1024 if size_unit == 'GB' else size_val
                file_sizes.append(file_size_mb)
            # --- Analyze filename for quality keywords ---
            lower_file_name = file_name.lower()
            for tag, keywords in QUALITY_KEYWORDS.items():
                if any(keyword in lower_file_name for keyword in keywords):
                    quality_tags.add(tag)
            for lang, keywords in LANGUAGE_KEYWORDS.items():
                if any(keyword in lower_file_name for keyword in keywords):
                    language_tags.add(lang)
            # Filtering logic
            if filter_quality and not any(q in quality_tags for q in filter_quality):
                continue
            if filter_language and not any(l in language_tags for l in filter_language):
                continue
            if min_size_mb and (file_size_mb is None or file_size_mb < min_size_mb):
                continue
            if max_size_mb and (file_size_mb is None or file_size_mb > max_size_mb):
                continue
            if file_name and torrent_url:
                links.append({'title': file_name, 'url': torrent_url, 'size_mb': file_size_mb})
        logger.info(f"Successfully parsed {len(links)} .torrent links from post.")
        meta = {
            'quality_tags': list(quality_tags),
            'language_tags': list(language_tags),
            'file_sizes': file_sizes,
        }
        if quality_tags:
            logger.info(f"Found quality tags: {meta['quality_tags']}")
        if language_tags:
            logger.info(f"Found language tags: {meta['language_tags']}")
        return links, content_hash, list(quality_tags), meta

    async def scrape_post(self, url: str, filter_quality: list = None, filter_language: list = None, min_size_mb: int = None, max_size_mb: int = None) -> tuple[list, str, list, dict] | None:
        """ Public method to scrape a single post with advanced filtering. """
        logger.info(f"Scraping post: {url}")
        html = await self._fetch_page(url)
        if html:
            return self._parse_links(html, filter_quality, filter_language, min_size_mb, max_size_mb)
        return None

    async def find_latest_posts(self) -> list[str]:
        """ Scrapes the main page to find the latest post URLs. """
        logger.info(f"Checking for latest posts on {settings.TARGET_WEBSITE_URL}")
        html = await self._fetch_page(settings.TARGET_WEBSITE_URL)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        post_links = soup.select('div[data-row-id] h4.ipsDataItem_title > a')
        
        urls = [link['href'] for link in post_links if link.has_attr('href')]
        logger.info(f"Found {len(urls)} potential post links on the main page.")
        return urls[:10]
