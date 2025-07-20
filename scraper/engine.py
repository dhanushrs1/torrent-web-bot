# ==============================================================================
# File: link-scraper-bot/scraper/engine.py
# Description: The core web scraping and parsing logic. (DATE-AWARE ENGINE)
# ==============================================================================

import httpx
import hashlib
import re
from bs4 import BeautifulSoup, Tag
from loguru import logger
from core.config import settings
from datetime import datetime, timedelta, timezone

class ScraperEngine:
    """ 
    Handles fetching and parsing website content with an advanced, date-aware,
    and resilient logic designed to minimize failures from website changes.
    """

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
        }

    async def _fetch_page(self, url: str) -> str | None:
        """ 
        Fetches HTML content from a URL, automatically following redirects and
        retrying on transient network errors.
        """
        try:
            transport = httpx.AsyncHTTPTransport(proxy=settings.PROXY_URL, retries=2)
            async with httpx.AsyncClient(transport=transport, headers=self.headers, timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Error {e.response.status_code} while fetching {url}. The page may be protected or removed.")
            return None
        except httpx.RequestError as e:
            logger.error(f"Network error while fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during fetch for {url}: {e}", exc_info=True)
            return None

    def _parse_relative_time(self, time_tag: Tag) -> datetime | None:
        """ Parses the datetime from a <time> tag. """
        if not time_tag or not time_tag.has_attr('datetime'):
            return None
        
        try:
            # The 'datetime' attribute is usually in ISO 8601 format (e.g., "2025-07-21T10:00:00Z")
            iso_format_str = time_tag['datetime']
            # Parse the string and make it timezone-aware (UTC)
            return datetime.fromisoformat(iso_format_str.replace('Z', '+00:00'))
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not parse datetime from tag: {time_tag}. Error: {e}")
            return None

    def _parse_links(self, html_content: str) -> tuple[list, str, list, dict]:
        """
        Advanced parser to find download links, create a content hash,
        and extract rich metadata like quality, languages, and file sizes.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        content_wrap = soup.find('div', class_='cPost_contentWrap')
        
        if not content_wrap:
            logger.warning("Could not find 'cPost_contentWrap' div. The main post container may have changed.")
            return [], "", [], {}

        content_hash = hashlib.md5(str(content_wrap).encode('utf-8')).hexdigest()
        
        post_content = content_wrap.find('div', class_='ipsType_richText')
        if not post_content:
            logger.warning("Could not find 'ipsType_richText' div. The rich text container may have changed.")
            return [], content_hash, [], {}

        links = []
        quality_tags = set()
        metadata = {'language_tags': set(), 'file_sizes': set()}

        QUALITY_KEYWORDS = {
            '#PreDVD': ['predvd', 'pre-dvd'],
            '#CamRip': ['hdcam', 'camrip', 'cam'],
            '#TC': ['tc', 'telecine'],
            '#HDRip': ['hdrip', 'hd-rip'],
            '#WEBDL': ['web-dl', 'webdl', 'web'],
        }
        
        torrent_anchors = post_content.find_all('a', attrs={'data-fileext': 'torrent'})
        for anchor in torrent_anchors:
            try:
                file_name = anchor.text.strip()
                torrent_url = anchor.get('href')
                
                if not (file_name and torrent_url):
                    logger.warning(f"Found a torrent link with missing name or URL. Skipping.")
                    continue

                links.append({'title': file_name, 'url': torrent_url})
                lower_file_name = file_name.lower()

                for tag, keywords in QUALITY_KEYWORDS.items():
                    if any(keyword in lower_file_name for keyword in keywords):
                        quality_tags.add(tag)
                
                lang_match = re.search(r'[\[\(]([a-zA-Z\s\+]+)[\]\)]', file_name)
                if lang_match:
                    langs = [lang.strip() for lang in lang_match.group(1).split('+')]
                    metadata['language_tags'].update(langs)

                size_matches = re.findall(r'(\d+(\.\d+)?\s?(gb|mb))', lower_file_name)
                for match in size_matches:
                    metadata['file_sizes'].add(match[0].replace(" ", "").upper())

            except Exception as e:
                logger.error(f"An error occurred while parsing a single link. Skipping it. Error: {e}")
                continue

        logger.info(f"Successfully parsed {len(links)} .torrent links from post.")
        if quality_tags:
            logger.info(f"Found quality tags: {list(quality_tags)}")
        if metadata['language_tags'] or metadata['file_sizes']:
            logger.info(f"Extracted metadata: {metadata}")

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
        Scrapes the main page to find the latest post URLs by parsing their
        publication time and filtering for recent posts.
        """
        logger.info(f"Checking for latest posts on {settings.TARGET_WEBSITE_URL}")
        html = await self._fetch_page(settings.TARGET_WEBSITE_URL)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        recent_post_urls = []
        
        # Define how recent a post needs to be to be considered "new"
        time_threshold = datetime.now(timezone.utc) - timedelta(hours=48)

        # Find all post containers. This selector should target the entire list item for a post.
        post_containers = soup.select('article.c-card, div[data-row-id]')
        logger.info(f"Found {len(post_containers)} potential post containers on the main page.")

        for container in post_containers:
            try:
                link_tag = container.select_one('h4.ipsDataItem_title a')
                time_tag = container.select_one('time')

                if not (link_tag and time_tag and link_tag.has_attr('href')):
                    continue

                post_url = link_tag['href']
                post_time = self._parse_relative_time(time_tag)

                if post_time and post_time > time_threshold:
                    logger.success(f"Found recent post: '{link_tag.text.strip()}' published at {post_time}")
                    recent_post_urls.append(post_url)
                else:
                    if post_time:
                        logger.info(f"Skipping old post: '{link_tag.text.strip()}' published at {post_time}")

            except Exception as e:
                logger.error(f"Error parsing a post container: {e}")
                continue

        logger.success(f"Found a total of {len(recent_post_urls)} genuinely new post links.")
        return recent_post_urls
