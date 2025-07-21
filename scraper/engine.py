# ==============================================================================
# File: link-scraper-bot/scraper/engine.py
# Description: The core web scraping and parsing logic with date/time filtering
# ==============================================================================

import httpx
import hashlib
import re
from bs4 import BeautifulSoup, Tag
from loguru import logger
from core.config import settings
from datetime import datetime, timedelta
import pytz
from dateutil import parser
from urllib.parse import urljoin

class ScraperEngine:
    """
    Handles fetching and parsing website content with advanced date/time filtering
    to focus on the latest posts only.
    """

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }

    async def _fetch_page(self, url: str) -> str | None:
        """
        Fetches HTML content from a URL with improved error handling and proxy support.
        """
        try:
            # Configure transport with retries
            transport = httpx.AsyncHTTPTransport(retries=3)

            # Configure client with proxy support
            proxies = None
            if settings.PROXY_URL:
                proxies = {"all://": settings.PROXY_URL}

            async with httpx.AsyncClient(
                transport=transport,
                proxies=proxies,
                headers=self.headers,
                timeout=45.0,
                follow_redirects=True
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
                logger.info(f"Successfully fetched {url}")
                return response.text
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Error {e.response.status_code} while fetching {url}")
            return None
        except httpx.RequestError as e:
            logger.error(f"Network error while fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}", exc_info=True)
            return None

    def _extract_post_datetime(self, soup: BeautifulSoup, post_container: Tag) -> datetime | None:
        """
        Extracts post date/time from various possible locations in the HTML,
        including absolute timestamps and relative 'time ago' strings.
        """
        try:
            # Common datetime selectors for various forum types
            datetime_selectors = [
                'time[datetime]', '.ipsType_light time', '.ipsType_medium time',
                '[data-timestamp]', '.post-date', '.topic-date', '.ipsDataItem_meta time',
                'abbr[title*="20"]', 'span[title*="20"]', '.date'
            ]
            
            for selector in datetime_selectors:
                elements = post_container.select(selector) if hasattr(post_container, 'select') else soup.select(selector)
                for element in elements:
                    # Try different datetime attributes first
                    for attr in ['datetime', 'data-timestamp', 'title']:
                        datetime_str = element.get(attr)
                        if datetime_str:
                            try:
                                # Handle numeric Unix timestamps
                                if datetime_str.isdigit():
                                    return datetime.fromtimestamp(int(datetime_str), tz=pytz.UTC)
                                # Handle various string formats
                                return parser.parse(datetime_str)
                            except (ValueError, parser.ParserError):
                                continue
                    
                    # If attributes fail, try the element's text content
                    text_content = element.get_text(strip=True)
                    if text_content:
                        try:
                            return parser.parse(text_content)
                        except (ValueError, parser.ParserError):
                            continue
            
            # Fallback to relative time indicators if no absolute time is found
            relative_time_patterns = [
                (r'(\d+)\s*minutes?\s*ago', lambda m: datetime.now(pytz.UTC) - timedelta(minutes=int(m.group(1)))),
                (r'(\d+)\s*hours?\s*ago', lambda m: datetime.now(pytz.UTC) - timedelta(hours=int(m.group(1)))),
                (r'(\d+)\s*days?\s*ago', lambda m: datetime.now(pytz.UTC) - timedelta(days=int(m.group(1)))),
                (r'yesterday', lambda m: datetime.now(pytz.UTC) - timedelta(days=1)),
                (r'today', lambda m: datetime.now(pytz.UTC)),
            ]
            
            page_text = str(post_container) if post_container else str(soup)
            for pattern, time_func in relative_time_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    return time_func(match)
                    
        except Exception as e:
            logger.debug(f"Error extracting datetime: {e}")
        
        return None

    def _is_recent_post(self, post_datetime: datetime, hours_threshold: int = 48) -> bool:
        """
        Checks if a post is recent based on a given threshold. Handles timezone awareness.
        """
        if not post_datetime:
            logger.warning("No date found for post, assuming it's recent to be safe.")
            return True # If we can't determine the date, process it just in case
            
        now = datetime.now(pytz.UTC)
        
        # Ensure the post datetime is timezone-aware for accurate comparison
        if post_datetime.tzinfo is None:
            post_datetime = pytz.UTC.localize(post_datetime)
        else:
            post_datetime = post_datetime.astimezone(pytz.UTC)
            
        time_diff = now - post_datetime
        is_recent = time_diff <= timedelta(hours=hours_threshold)
        
        logger.debug(f"Post datetime: {post_datetime}, Current Time: {now}, Difference: {time_diff}, Recent: {is_recent}")
        return is_recent

    def _parse_links(self, html_content: str) -> tuple[list, str, list, dict]:
        """
        Advanced parser to find download links and extract metadata like quality, language, and file size.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the main content container to narrow the search area
        content_wrap = (soup.find('div', class_='cPost_contentWrap') or
                        soup.find('div', class_='post-content') or
                        soup.find('div', class_='message-content') or
                        soup.find('article') or
                        soup.find('div', class_='content'))
        
        if not content_wrap:
            logger.warning("Could not find a primary post content container.")
            return [], "", [], {}

        # Create a hash of the content to detect future edits
        content_hash = hashlib.md5(str(content_wrap).encode('utf-8')).hexdigest()
        
        # Find the rich text area within the container
        post_content = (content_wrap.find('div', class_='ipsType_richText') or
                        content_wrap.find('div', class_='post-body') or
                        content_wrap)
        
        links = []
        quality_tags = set()
        metadata = {'language_tags': set(), 'file_sizes': set()}

        QUALITY_KEYWORDS = {
            '#PreDVD': ['predvd', 'pre-dvd'], '#CamRip': ['hdcam', 'camrip', 'cam'],
            '#TC': ['tc', 'telecine'], '#HDRip': ['hdrip', 'hd-rip'],
            '#WEBDL': ['web-dl', 'webdl', 'web'], '#BluRay': ['bluray', 'blu-ray', 'bdrip'],
            '#DVDRip': ['dvdrip', 'dvd-rip'], '#WEBRip': ['webrip', 'web-rip'],
        }
        
        # Use multiple selectors to find all potential torrent/magnet links
        torrent_selectors = [
            'a[data-fileext="torrent"]', 'a[href*=".torrent"]',
            'a[href*="torrent"]', 'a[href*="magnet:"]'
        ]
        
        torrent_anchors = []
        for selector in torrent_selectors:
            found_anchors = post_content.select(selector)
            if found_anchors:
                torrent_anchors.extend(found_anchors)
                logger.debug(f"Found {len(found_anchors)} links with selector: {selector}")
        
        # Process unique links to avoid duplicates
        seen_urls = set()
        for anchor in torrent_anchors:
            url = anchor.get('href')
            if url and url not in seen_urls:
                seen_urls.add(url)
                try:
                    file_name = anchor.text.strip() or anchor.get('title', '').strip()
                    if not file_name: continue

                    links.append({'title': file_name, 'url': url})
                    lower_file_name = file_name.lower()

                    # Extract quality, language, and file size metadata from the link text
                    for tag, keywords in QUALITY_KEYWORDS.items():
                        if any(keyword in lower_file_name for keyword in keywords):
                            quality_tags.add(tag)

                    lang_patterns = [
                        r'[\[\(]([a-zA-Z\s\+]+)[\]\)]',
                        r'(tamil|hindi|telugu|malayalam|kannada|english|multi)',
                        r'(tam|hin|tel|mal|kan|eng)'
                    ]
                    for pattern in lang_patterns:
                        for match in re.findall(pattern, file_name, re.IGNORECASE):
                            langs = [lang.strip() for lang in match.split('+')] if '+' in match else [match.strip()]
                            metadata['language_tags'].update(langs)

                    for match in re.findall(r'(\d+(?:\.\d+)?\s?(?:gb|mb|tb))', lower_file_name):
                        metadata['file_sizes'].add(match.replace(" ", "").upper())

                except Exception as e:
                    logger.error(f"Error parsing link: {e}", exc_info=True)
                    continue

        logger.info(f"Parsed {len(links)} download links.")
        if quality_tags: logger.info(f"Quality tags: {list(quality_tags)}")
        if metadata['language_tags'] or metadata['file_sizes']: logger.info(f"Metadata: {metadata}")

        return links, content_hash, list(quality_tags), {k: list(v) for k, v in metadata.items()}

    async def scrape_post(self, url: str) -> tuple[list, str, list, dict] | None:
        """ Public method to scrape a single post. """
        logger.info(f"Scraping post: {url}")
        html = await self._fetch_page(url)
        return self._parse_links(html) if html else None

    async def find_latest_posts(self, max_posts: int = 25, hours_filter: int = 48) -> list[str]:
        """
        Finds the latest posts from the main page using date/time filtering and robust selectors.
        """
        logger.info(f"Checking for posts from the last {hours_filter} hours on {settings.TARGET_WEBSITE_URL}")
        html = await self._fetch_page(settings.TARGET_WEBSITE_URL)
        if not html:
            logger.error("Failed to fetch main page, cannot find latest posts.")
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Comprehensive list of selectors for different forum software
        post_selectors = [
            'div[data-rowid] .ipsDataItem_title a', 'article.ipsStreamItem .ipsDataItem_title a',
            '.cTopicList .ipsDataItem_title a', '.topic-title a', '.topictitle',
            '.topic-list .main-link a', 'h3 a', 'h4 a', '.title a',
            '[class*="topic"] a[href*="/topic/"]', '[class*="post"] a[href*="/topic/"]'
        ]
        
        found_posts = []
        for selector in post_selectors:
            elements = soup.select(selector)
            if not elements: continue
                
            logger.info(f"Found {len(elements)} potential posts with selector: '{selector}'")
            for element in elements:
                href = element.get('href')
                if not href: continue

                full_url = urljoin(settings.TARGET_WEBSITE_URL, href)

                # Find the most relevant parent container for date extraction
                post_container = element
                for _ in range(5): # Go up a max of 5 levels
                    if not post_container.parent: break
                    post_container = post_container.parent
                    class_str = str(post_container.get('class', []))
                    if any(cls in class_str for cls in ['topic', 'post', 'item', 'row', 'stream']):
                        break
                
                post_datetime = self._extract_post_datetime(soup, post_container)
                
                if self._is_recent_post(post_datetime, hours_filter):
                    found_posts.append({
                        'url': full_url,
                        'datetime': post_datetime,
                        'title': element.get_text(strip=True)
                    })
            if found_posts:
                break # Stop after the first successful selector finds recent posts

        # Sort posts by datetime (newest first), with a fallback for posts without a date
        found_posts.sort(
            key=lambda x: x['datetime'] or datetime.min.replace(tzinfo=pytz.UTC),
            reverse=True
        )

        # Remove duplicate URLs, preserving the order (newest first)
        unique_urls = []
        seen = set()
        for post in found_posts:
            if post['url'] not in seen:
                unique_urls.append(post['url'])
                seen.add(post['url'])
        
        result_urls = unique_urls[:max_posts]
        
        logger.success(f"Found {len(result_urls)} unique recent posts to process.")
        if result_urls:
            logger.info("Top recent posts:")
            for i, post in enumerate(found_posts[:5], 1):
                date_str = post['datetime'].strftime('%Y-%m-%d %H:%M') if post['datetime'] else 'Unknown date'
                logger.info(f"  {i}. [{date_str}] {post['title'][:60]}...")
        
        return result_urls

}
