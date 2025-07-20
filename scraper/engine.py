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
        Fetches HTML content from a URL with improved error handling.
        """
        try:
            transport = httpx.AsyncHTTPTransport(proxy=settings.PROXY_URL, retries=3)
            
            async with httpx.AsyncClient(transport=transport, headers=self.headers, timeout=45.0, follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()
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

    def _extract_post_datetime(self, soup: BeautifulSoup, post_container) -> datetime | None:
        """
        Extract post date/time from various possible locations in the HTML.
        """
        try:
            # Common datetime selectors for forum posts
            datetime_selectors = [
                'time[datetime]',
                '.ipsType_light time',
                '.ipsType_medium time', 
                '[data-timestamp]',
                '.post-date',
                '.topic-date',
                '.ipsDataItem_meta time',
                'abbr[title*="20"]',  # Look for year in title
                'span[title*="20"]',
                '.date'
            ]
            
            for selector in datetime_selectors:
                elements = post_container.select(selector) if hasattr(post_container, 'select') else soup.select(selector)
                for element in elements:
                    # Try different datetime attributes
                    for attr in ['datetime', 'data-timestamp', 'title']:
                        datetime_str = element.get(attr)
                        if datetime_str:
                            try:
                                # Handle timestamp format
                                if datetime_str.isdigit():
                                    return datetime.fromtimestamp(int(datetime_str), tz=pytz.UTC)
                                else:
                                    # Parse ISO format or other formats
                                    return parser.parse(datetime_str)
                            except:
                                continue
                    
                    # Try text content
                    text_content = element.get_text().strip()
                    if text_content:
                        try:
                            return parser.parse(text_content)
                        except:
                            continue
            
            # Look for relative time indicators
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
        Check if a post is recent (within the specified hours threshold).
        """
        if not post_datetime:
            return True  # If we can't determine date, assume it might be recent
            
        now = datetime.now(pytz.UTC)
        if post_datetime.tzinfo is None:
            post_datetime = pytz.UTC.localize(post_datetime)
        elif post_datetime.tzinfo != pytz.UTC:
            post_datetime = post_datetime.astimezone(pytz.UTC)
            
        time_diff = now - post_datetime
        is_recent = time_diff <= timedelta(hours=hours_threshold)
        
        logger.debug(f"Post datetime: {post_datetime}, Current: {now}, Diff: {time_diff}, Recent: {is_recent}")
        return is_recent

    def _parse_links(self, html_content: str) -> tuple[list, str, list, dict]:
        """
        Advanced parser to find download links and extract metadata.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        content_wrap = soup.find('div', class_='cPost_contentWrap')
        
        if not content_wrap:
            # Try alternative containers
            content_wrap = soup.find('div', class_='post-content') or \
                          soup.find('div', class_='message-content') or \
                          soup.find('article') or \
                          soup.find('div', class_='content')
            
        if not content_wrap:
            logger.warning("Could not find post content container")
            return [], "", [], {}

        content_hash = hashlib.md5(str(content_wrap).encode('utf-8')).hexdigest()
        
        # Look for rich text content
        post_content = content_wrap.find('div', class_='ipsType_richText') or \
                      content_wrap.find('div', class_='post-body') or \
                      content_wrap
        
        if not post_content:
            logger.warning("Could not find post content")
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
            '#BluRay': ['bluray', 'blu-ray', 'bdrip'],
            '#DVDRip': ['dvdrip', 'dvd-rip'],
            '#WEBRip': ['webrip', 'web-rip'],
        }
        
        # Find torrent links using multiple methods
        torrent_selectors = [
            'a[data-fileext="torrent"]',
            'a[href*=".torrent"]',
            'a[href*="torrent"]',
            'a[href*="magnet:"]'
        ]
        
        torrent_anchors = []
        for selector in torrent_selectors:
            found_anchors = post_content.select(selector)
            torrent_anchors.extend(found_anchors)
            if found_anchors:
                logger.debug(f"Found {len(found_anchors)} links with selector: {selector}")
        
        # Remove duplicates
        seen_urls = set()
        unique_anchors = []
        for anchor in torrent_anchors:
            url = anchor.get('href')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_anchors.append(anchor)
        
        for anchor in unique_anchors:
            try:
                file_name = anchor.text.strip() or anchor.get('title', '').strip()
                torrent_url = anchor.get('href')
                
                if not (file_name and torrent_url):
                    logger.debug("Skipping link with missing name or URL")
                    continue

                links.append({'title': file_name, 'url': torrent_url})
                lower_file_name = file_name.lower()

                # Extract quality tags
                for tag, keywords in QUALITY_KEYWORDS.items():
                    if any(keyword in lower_file_name for keyword in keywords):
                        quality_tags.add(tag)
                
                # Extract languages
                lang_patterns = [
                    r'[\[\(]([a-zA-Z\s\+]+)[\]\)]',
                    r'(tamil|hindi|telugu|malayalam|kannada|english|multi)',
                    r'(tam|hin|tel|mal|kan|eng)'
                ]
                
                for pattern in lang_patterns:
                    lang_matches = re.findall(pattern, file_name, re.IGNORECASE)
                    for match in lang_matches:
                        if '+' in match:
                            langs = [lang.strip() for lang in match.split('+')]
                            metadata['language_tags'].update(langs)
                        else:
                            metadata['language_tags'].add(match.strip())

                # Extract file sizes
                size_matches = re.findall(r'(\d+(?:\.\d+)?\s?(?:gb|mb|tb))', lower_file_name)
                for match in size_matches:
                    metadata['file_sizes'].add(match.replace(" ", "").upper())

            except Exception as e:
                logger.error(f"Error parsing link: {e}")
                continue

        logger.info(f"Parsed {len(links)} download links")
        if quality_tags:
            logger.info(f"Quality tags: {list(quality_tags)}")
        if metadata['language_tags'] or metadata['file_sizes']:
            logger.info(f"Metadata: {metadata}")

        final_metadata = {k: list(v) for k, v in metadata.items()}
        return links, content_hash, list(quality_tags), final_metadata

    async def scrape_post(self, url: str) -> tuple[list, str, list, dict] | None:
        """ Public method to scrape a single post. """
        logger.info(f"Scraping post: {url}")
        html = await self._fetch_page(url)
        if html:
            return self._parse_links(html)
        return None

    async def find_latest_posts(self, max_posts: int = 10, hours_filter: int = 48) -> list[str]:
        """ 
        Find the latest posts with date/time filtering for efficiency.
        """
        logger.info(f"Checking for posts from last {hours_filter} hours on {settings.TARGET_WEBSITE_URL}")
        html = await self._fetch_page(settings.TARGET_WEBSITE_URL)
        if not html:
            logger.error("Failed to fetch main page")
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        recent_posts = []
        
        # Enhanced selectors for different forum types
        post_selectors = [
            # Invision Community (IPS)
            'div[data-rowid] .ipsDataItem_title a',
            'article.ipsStreamItem .ipsDataItem_title a',
            '.cTopicList .ipsDataItem_title a',
            
            # phpBB
            '.topic-title a',
            '.topictitle',
            
            # Discourse
            '.topic-list .main-link a',
            '.topic-title a',
            
            # Generic
            'h3 a', 'h4 a', '.title a',
            '[class*="topic"] a[href*="/topic/"]',
            '[class*="post"] a[href*="/topic/"]'
        ]
        
        found_posts = []
        
        for selector in post_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    logger.info(f"Found {len(elements)} posts with selector: {selector}")
                    
                    for element in elements:
                        href = element.get('href')
                        if not href:
                            continue
                            
                        # Normalize URL
                        if href.startswith('/'):
                            base_url = settings.TARGET_WEBSITE_URL.rstrip('/')
                            href = base_url + href
                        
                        # Find the post container for datetime extraction
                        post_container = element
                        for _ in range(5):  # Go up max 5 levels to find post container
                            post_container = post_container.parent
                            if not post_container:
                                break
                            if any(cls in str(post_container.get('class', [])) for cls in 
                                  ['topic', 'post', 'item', 'row', 'stream']):
                                break
                        
                        # Extract post datetime
                        post_datetime = self._extract_post_datetime(soup, post_container)
                        
                        # Check if post is recent
                        if self._is_recent_post(post_datetime, hours_filter):
                            found_posts.append({
                                'url': href,
                                'datetime': post_datetime,
                                'title': element.get_text().strip()
                            })
                            logger.debug(f"Added recent post: {element.get_text().strip()[:50]}...")
                        else:
                            logger.debug(f"Skipped old post: {element.get_text().strip()[:50]}...")
                    
                    if found_posts:
                        break  # Stop at first successful selector
                        
            except Exception as e:
                logger.warning(f"Error with selector '{selector}': {e}")
                continue
        
        # If no posts found with date filtering, fall back to recent posts by URL pattern
        if not found_posts:
            logger.warning("No recent posts found with date filtering, using fallback method")
            fallback_links = soup.find_all('a', href=re.compile(r'/topic/\d+'))
            for link in fallback_links[:max_posts]:
                href = link.get('href')
                if href.startswith('/'):
                    base_url = settings.TARGET_WEBSITE_URL.rstrip('/')
                    href = base_url + href
                found_posts.append({
                    'url': href,
                    'datetime': None,
                    'title': link.get_text().strip()
                })
        
        # Sort by datetime (newest first) or by URL ID if no datetime
        try:
            found_posts.sort(key=lambda x: x['datetime'] or datetime.min.replace(tzinfo=pytz.UTC), reverse=True)
        except:
            # Fallback sort by URL ID
            found_posts.sort(key=lambda x: int(re.search(r'/(\d+)', x['url']).group(1)) if re.search(r'/(\d+)', x['url']) else 0, reverse=True)
        
        # Extract URLs and limit results
        result_urls = [post['url'] for post in found_posts[:max_posts]]
        
        logger.success(f"Found {len(result_urls)} recent posts")
        if result_urls:
            logger.info("Top recent posts:")
            for i, post in enumerate(found_posts[:5], 1):
                date_str = post['datetime'].strftime('%Y-%m-%d %H:%M') if post['datetime'] else 'Unknown date'
                logger.info(f"  {i}. [{date_str}] {post['title'][:60]}...")
        
        return result_urls
