import requests
from bs4 import BeautifulSoup, Comment
import asyncio
import aiohttp
import json
import csv
import re
import hashlib
import os
import sys
from urllib.parse import urljoin, urlparse, urlunparse
from typing import Set, Dict, List, Any, Optional
from dataclasses import dataclass, asdict, field
from datetime import datetime
from fake_useragent import UserAgent
from rich.console import Console
from rich.progress import Progress
import logging
import sqlite3
import pandas
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import plotly.express as px
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class WebsiteData:
    url: str
    metadata: Dict = field(default_factory=dict)
    content: Dict = field(default_factory=dict)
    assets: Dict = field(default_factory=dict)
    links: Dict = field(default_factory=dict)
    technical: Dict = field(default_factory=dict)
    analytics: Dict = field(default_factory=dict)
    security: Dict = field(default_factory=dict)
    social: Dict = field(default_factory=dict)
    seo: Dict = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

class ComprehensiveScraper:

    def __init__(self, base_url: str, max_depth: int = 3, max_pages: int = 100):
        self.setup_output_dirs()
        self.setup_database()
        self.base_url = self.normalize_url(base_url)
        self.domain = urlparse(self.base_url).netloc
        self.console = Console()
        self.visited_urls: Set[str] = set()
        self.data = WebsiteData(url=base_url)
        self.ua = UserAgent()
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.headers = self.generate_headers()
        
    # Initialize directories before anything else that depends on them


    def setup_output_dirs(self):
        try:
            self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.output_dir = Path(f'scraped_data_{self.timestamp}')
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create output directory: {str(e)}")
            sys.exit(1)

    def normalize_url(self, url: str) -> str:
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
        parsed = urlparse(url)
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path if parsed.path else '/',
            parsed.params,
            parsed.query,
            ''
        ))

    def setup_database(self):
        if not hasattr(self, 'output_dir'):
            self.setup_output_dirs()  # Ensure output_dir is set before use
        self.db = sqlite3.connect(self.output_dir / 'site_data.db')
        self.create_tables()

    def create_tables(self):
        tables = {
            'pages': '''
                CREATE TABLE IF NOT EXISTS pages (
                    url TEXT PRIMARY KEY,
                    title TEXT,
                    content_type TEXT,
                    status_code INTEGER,
                    load_time FLOAT,
                    word_count INTEGER,
                    hash TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'links': '''
                CREATE TABLE IF NOT EXISTS links (
                    source_url TEXT,
                    target_url TEXT,
                    text TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source_url, target_url)
                )
            ''',
            'assets': '''
                CREATE TABLE IF NOT EXISTS assets (
                    url TEXT,
                    type TEXT,
                    source_page TEXT,
                    size INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (url, source_page)
                )
            ''',
            'business_info': '''
                CREATE TABLE IF NOT EXISTS business_info (
                    url TEXT PRIMARY KEY,
                    address TEXT,
                    phone TEXT,
                    email TEXT,
                    social_media TEXT,
                    business_hours TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            '''
        }
        
        for query in tables.values():
            self.db.execute(query)
        self.db.commit()

    def generate_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    async def scrape_site(self):
        try:
            timeout = aiohttp.ClientTimeout(total=300)
            async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
                with Progress() as progress:
                    task = progress.add_task("[cyan]Scraping...", total=self.max_pages)
                    await self.recursive_scrape(self.base_url, session, progress, task, depth=0)
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
        finally:
            self.save_data()
            await self.cleanup()

    async def recursive_scrape(self, url: str, session: aiohttp.ClientSession, 
                             progress: Progress, task: int, depth: int = 0):
        if (url in self.visited_urls or 
            depth > self.max_depth or 
            len(self.visited_urls) >= self.max_pages):
            return

        self.visited_urls.add(url)
        progress.update(task, advance=1)

        try:
            page_data = await self.scrape_page(url, session)
            if page_data:
                await self.store_page_data(page_data)
                links = self.extract_links(page_data['html'])
                tasks = []
                for link in links:
                    if self.should_follow_link(link):
                        tasks.append(self.recursive_scrape(link, session, progress, task, depth + 1))
                await asyncio.gather(*tasks)
        except Exception as e:
            logging.error(f"Error scraping {url}: {str(e)}")

    def should_follow_link(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            return (parsed.netloc == self.domain and 
                   url not in self.visited_urls and 
                   not any(ext in parsed.path.lower() 
                          for ext in ['.pdf', '.jpg', '.png', '.gif']))
        except:
            return False

    async def scrape_page(self, url: str, session: aiohttp.ClientSession) -> Optional[Dict]:
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return None

                content_type = response.headers.get('content-type', '')
                if 'text/html' not in content_type.lower():
                    return None

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                page_data = {
                    'url': url,
                    'html': html,
                    'title': self.extract_title(soup),
                    'meta': self.extract_meta(soup),
                    'headers': dict(response.headers),
                    'status': response.status,
                    'content_type': content_type,
                    'links': self.extract_links(html),
                    'images': self.extract_images(soup),
                    'business_info': self.extract_business_info(soup),
                    'timestamp': datetime.now().isoformat()
                }

                return page_data
        except Exception as e:
            logging.error(f"Error scraping {url}: {str(e)}")
            return None

    def extract_title(self, soup: BeautifulSoup) -> str:
        if soup.title:
            return soup.title.string.strip()
        h1 = soup.find('h1')
        return h1.get_text(strip=True) if h1 else ''

    def extract_business_info(self, soup: BeautifulSoup) -> Dict:
        info = {}
        
        # Phone numbers
        phone_pattern = re.compile(r'(\+?1[-.]?)?\s*\(?([0-9]{3})\)?[-.]?\s*([0-9]{3})[-.]?\s*([0-9]{4})')
        phones = soup.find_all(string=phone_pattern)
        info['phones'] = [phone_pattern.search(phone).group() for phone in phones if phone_pattern.search(phone)]

        # Emails
        email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        emails = soup.find_all(string=email_pattern)
        info['emails'] = [email_pattern.search(email).group() for email in emails if email_pattern.search(email)]

        # Social media links
        social_patterns = {
            'facebook': r'facebook\.com/[\w.]+',
            'twitter': r'twitter\.com/[\w]+',
            'instagram': r'instagram\.com/[\w.]+',
            'linkedin': r'linkedin\.com/[\w/%-]+',
        }
        info['social'] = {}
        for platform, pattern in social_patterns.items():
            links = soup.find_all('a', href=re.compile(pattern))
            if links:
                info['social'][platform] = [link['href'] for link in links]

        # Business hours
        hours_pattern = re.compile(r'(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*day', re.I)
        hours_section = soup.find_all(string=hours_pattern)
        info['hours'] = [h.strip() for h in hours_section if len(h.strip()) > 0]

        return info

    async def store_page_data(self, data: Dict):
        try:
            self.db.execute('''
                INSERT OR REPLACE INTO pages
                (url, title, content_type, status_code, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (data['url'], data['title'], data['content_type'], 
                 data['status'], data['timestamp']))

            if data.get('business_info'):
                self.db.execute('''
                    INSERT OR REPLACE INTO business_info
                    (url, address, phone, email, social_media, business_hours)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    data['url'],
                    json.dumps(data['business_info'].get('address', [])),
                    json.dumps(data['business_info'].get('phones', [])),
                    json.dumps(data['business_info'].get('emails', [])),
                    json.dumps(data['business_info'].get('social', {})),
                    json.dumps(data['business_info'].get('hours', []))
                ))

            self.db.commit()
        except Exception as e:
            logging.error(f"Error storing data: {str(e)}")
            self.db.rollback()

    def save_data(self):
        try:
            # Export tables to CSV
            for table in ['pages', 'links', 'assets', 'business_info']:
                df = pd.read_sql_query(f'SELECT * FROM {table}', self.db)
                df.to_csv(self.output_dir / f'{table}.csv', index=False)

            # Save summary report
            summary = {
                'base_url': self.base_url,
                'total_pages': len(self.visited_urls),
                'timestamp': self.timestamp,
                'pages': list(self.visited_urls)
            }
            
            with open(self.output_dir / 'summary.json', 'w') as f:
                json.dump(summary, f, indent=2)

        except Exception as e:
            logging.error(f"Error saving data: {str(e)}")

    async def cleanup(self):
        self.db.close()

async def main():
    console = Console()
    console.print("\n[bold cyan]Advanced Web Scraper[/bold cyan]")
    
    url = input("\nEnter website URL: ").strip()
    max_depth = int(input("Enter max depth (default 3): ") or 3)
    max_pages = int(input("Enter max pages (default 100): ") or 100)
    
    try:
        scraper = ComprehensiveScraper(url, max_depth, max_pages)
        await scraper.scrape_site()
        console.print("\n[green]Scraping completed! Check the output directory for results.[/green]")
    except KeyboardInterrupt:
        console.print("\n[yellow]Scraping interrupted. Saving partial data...[/yellow]")
        scraper.save_data()
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())