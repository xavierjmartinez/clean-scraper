import time
import urllib.parse
from pathlib import Path
from typing import List

from bs4 import BeautifulSoup
import requests

from .. import utils
from ..cache import Cache


class Site:
    """Scrape file metadata and download files for the Oakland Police Department for SB16/SB1421/AB748 data."""

    name = "Oakland Police Department"

    def __init__(self, data_dir: Path = utils.CLEAN_DATA_DIR, cache_dir: Path = utils.CLEAN_CACHE_DIR):
        """Initialize a new instance."""
        self.base_url = "https://www.oaklandca.gov"
        self.disclosure_url = f"{self.base_url}/resources/oakland-police-officers-and-related-sb-1421-16-incidents"
        self.data_dir = data_dir
        self.cache_dir = cache_dir
        self.cache = Cache(cache_dir)

    @property
    def agency_slug(self) -> str:
        """Construct the agency slug."""
        mod = Path(__file__)
        state_postal = mod.parent.stem
        return f"{state_postal}_{mod.stem}"  # ca_oakland_pd

    def scrape_meta(self, throttle: int = 0) -> Path:
        """Gather metadata on downloadable files (videos, etc.)."""
        index_page_local = self._download_index_page(self.disclosure_url)
        metadata = self._get_asset_links(index_page_local)
        return metadata

    def scrape(self, throttle: int = 0, filter: str = "") -> List[Path]:
        """Download file assets from agency.

        Args:
            throttle (int): Number of seconds to wait between requests. Defaults to 0.
            filter (str): Only download URLs that match the filter. Defaults to None.

        Returns:
            List[Path]: List of local paths to downloaded files
        """
        downloaded_assets = []
        child_pages = self._get_child_page(None, throttle)

        for child_page in child_pages:
            html = self.cache.read(child_page["cache_path"])
            soup = BeautifulSoup(html, "html.parser")
            case_no_link = soup.find("a", text="Internal Affairs Case No.")
            if case_no_link:
                nextrequest_url = urllib.parse.urljoin(self.base_url, case_no_link["href"])
                response = requests.get(nextrequest_url)
                nextrequest_soup = BeautifulSoup(response.content, "html.parser")

                # Extract the NextRequest URL from the HTML
                nextrequest_url = nextrequest_soup.find("a", class_="document-link")["href"]

                # Follow pagination links to get all files
                visited_urls = set()
                while nextrequest_url and nextrequest_url not in visited_urls:
                    visited_urls.add(nextrequest_url)
                    response = requests.get(nextrequest_url)
                    nextrequest_soup = BeautifulSoup(response.content, "html.parser")
                    file_links = nextrequest_soup.find_all("a", class_="document-link")

                    child_page_dir = nextrequest_url.split("folder_filter=")[-1]

                    for file_link in file_links:
                        file_url = urllib.parse.urljoin(nextrequest_url, file_link["href"])
                        file_name = file_link.text.strip()
                        download_path = Path(self.agency_slug, "assets", child_page_dir, file_name)
                        time.sleep(throttle)
                        downloaded_assets.append(self.cache.download(str(download_path), file_url))

                    # Follow the next pagination link, if available
                    next_link = nextrequest_soup.find("a", class_="next")
                    if next_link:
                        nextrequest_url = urllib.parse.urljoin(nextrequest_url, next_link["href"])
                    else:
                        nextrequest_url = None

        return downloaded_assets

    # Helper functions
    def _get_asset_links(self, index_page: Path) -> Path:
        """Extract links to files and videos from the 'Internal Affairs Case No.' column."""
        metadata = []
        html = self.cache.read(index_page)
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")  # Assuming the data is in a table
        rows = table.find_all("tr")  # Get all table rows

        for row in rows:
            cols = row.find_all("td")  # Get all columns in the current row
            if len(cols) >= 3:  # Assuming the 'Internal Affairs Case No.' column is the 3rd column
                case_no_link = cols[2].find("a")  # Find the link in the 3rd column
                if case_no_link:
                    payload = {
                        "asset_url": case_no_link["href"],
                        "name": case_no_link.text.strip(),
                    }
                    metadata.append(payload)

        outfile = self.data_dir.joinpath(f"{self.agency_slug}.json")
        self.cache.write_json(outfile, metadata)
        return outfile

    def _get_child_page(self, index_page: Path, throttle: int = 0) -> List[dict]:
        """Get URLs for child pages from the 'Internal Affairs Case No.' links."""
        child_pages = []
        metadata = self.cache.read_json(self.data_dir.joinpath(f"{self.agency_slug}.json"))

        for asset in metadata:
            url = asset["asset_url"]
            time.sleep(throttle)
            page_meta = {
                "source_name": asset["name"],
                "url": urllib.parse.urljoin(self.base_url, url)
            }