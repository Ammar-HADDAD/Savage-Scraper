#!/usr/bin/env python3
"""
Products URLs Scraper - Extracts products URLs from category pages
"""

from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import json
import time
from datetime import datetime
from selenium.webdriver.remote.webelement import WebElement
from .SavageScraper import SavageScraper, run_multiprocess_scraper


class MainProductsURLSScraper(SavageScraper):
    """Scraper for extracting products URLs from N5 pages"""
    
    def __init__(self, config_dir=None, output_dir=None, logs_dir=None,error_pages_dir=None, 
                 is_headless=False, translation=False, process_id=None,
                 apply_four_stars_filter=True, apply_shipping_by_amazon_filter=True, 
                 apply_price_filter=True, max_pages=100):
        
        # Store scraper-specific parameters
        self.apply_four_stars_filter = apply_four_stars_filter
        self.apply_shipping_by_amazon_filter = apply_shipping_by_amazon_filter
        self.apply_price_filter = apply_price_filter
        self.max_pages = max_pages
        
        super().__init__(config_dir, output_dir, logs_dir, error_pages_dir, is_headless, translation, process_id)
    
    def _get_progress_tracking_key(self) -> str:
        """Get the key used for products progress tracking"""
        return "N5_url"
    
    def _get_output_file_path(self) -> Path:
        """Get the output file path for products data"""
        return self.output_dir / f"ProductsURLS_{self.key}.csv"
    
    def _get_required_selectors(self) -> List[str]:
        """Get required selector keys for products scraping"""
        return [
            'products_container',
            'products_url',
            'products_price',
            'products_reviews',
            'products_next_page',
            'four_stars_filter',
            'shipping_by_amazon_filter',
            'products_ready',
            'error_page_indicator',
            'error_page_handler'
        ]
    
    def _get_page_ready_selector(self) -> str:
        """Get the selector key for products page ready indicator"""
        return "products_ready"
    
    def _get_categories_selector(self) -> str:
        """Get the selector key for product elements (not used in this scraper)"""
        return "products_container"
    
    def _get_resume_key(self) -> str:
        """Get the key used to check if an item was already processed (for resume functionality)"""
        return "N5_url"  # For products scraper, we check if the N5_url was already processed
    
    def _process_category_element(self, element: WebElement, item: Dict[str, Any]) -> Dict[str, Any]:
        """Not used in products scraper - we override _process_single_item instead"""
        return None
    
    def _create_empty_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Create an empty result when no products are found"""
        return {
            "N1": item["N1"],
            "N2": item["N2"], 
            "N3": item["N3"],
            "N4": item["N4"],
            "N5": item["N5"],
            "product_reviews": 0,
            "product_price": 0,
            "N3_url": item["N3_url"],
            "N4_url": item["N4_url"],
            "N5_url": item["N5_url"],
            "product_url": "",
            "page": 1,
            "position_in_page": 1,
            'scraped_at': datetime.now().isoformat(),
            'process_id': self.process_id
        }
    
    def _process_single_item(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single N5 category to extract products across multiple pages"""
        all_products = []
        page = 1
        
        try:
            # Navigate to the N5 category page
            if not self._navigate_to_url(item[self.progress_tracking_key]):
                self.logger.error(f"Failed to navigate to {item[self.progress_tracking_key]}")
                return []

            # Apply filters
            if not self._apply_filters(item):
                self.logger.error("Failed to apply filters")
                return []

            # Process pages
            while page <= self.max_pages:
                self.logger.info(f"Processing page {page} for: {item['N1']} > {item['N2']} > {item['N3']} > {item['N4']} > {item['N5']}")
                
                # Extract products from current page
                page_products = self._extract_products_from_page(item, page)
                
                if not page_products:
                    self.logger.warning(f"No products found on page {page}")
                    break
                
                all_products.extend(page_products)
                self.logger.info(f"Extracted {len(page_products)} products from page {page}")
                
                # Check for next page
                if not self._has_next_page():
                    self.logger.info(f"No next page found after page {page}")
                    break
                
                # Navigate to next page
                if not self._navigate_to_next_page():
                    self.logger.error(f"Failed to navigate to page {page + 1}")
                    break
                    
                page += 1

            if not all_products:
                # Create empty result to mark as processed
                empty_result = self._create_empty_result(item)
                all_products.append(empty_result)
                self.logger.warning(f"No products extracted for {item[self.progress_tracking_key]}")
            else:
                self.logger.info(f"Successfully processed {len(all_products)} total products from {page} pages")
            
            return all_products
        
        except Exception as e:
            self.logger.error(f"Error processing category {item[self.progress_tracking_key]}: {e}")
            return []
    
    def _apply_filters(self, item: Dict[str, Any]) -> bool:
        """Apply various filters to the product listing"""
        try:
            # Apply 4-stars filter if enabled
            if self.apply_four_stars_filter:
                four_stars_filter = self._find_elements("four_stars_filter")
                if four_stars_filter:
                    four_stars_filter_url = four_stars_filter[0].get_attribute("href")
                    self.logger.info("Applying 4-stars filter")
                    if not self._navigate_to_url(four_stars_filter_url):
                        self.logger.warning("Failed to apply 4-stars filter")
                        return False
                    else:
                        self.logger.info("4 stars filter applied successfully")
                else:
                    self.logger.warning("4-stars filter not found")
                    return False

            # Apply shipping filter if enabled
            if self.apply_shipping_by_amazon_filter:
                shipping_by_amazon_filter = self._find_elements("shipping_by_amazon_filter")
                if shipping_by_amazon_filter:
                    shipping_by_amazon_url = shipping_by_amazon_filter[0].get_attribute("href")
                    self.logger.info("Applying shipping filter")
                    
                    if not self._navigate_to_url(shipping_by_amazon_url):
                        self.logger.warning("Failed to apply shipping by amazon filter")
                    else:
                        self.logger.info("Shipping filter applied successfully")
                else:
                    self.logger.warning("Amazon shipping filter not found, proceeding without it")

            # Apply price filter 
            if self.apply_price_filter:
                current_url = self.driver.current_url
                lower_price = item.get("lower_price", "0")
                higher_price = item.get("higher_price", "99999")

                self.logger.info("Applying price filter...")
                price_filter_url = f"{current_url}&low-price={lower_price}&high-price={higher_price}"

                if not self._navigate_to_url(price_filter_url):
                    self.logger.warning("Failed to apply price filter")
                    return False
                else:
                    self.logger.info(f"Price filter applied successfully ({lower_price}-{higher_price})")

            return True
            
        except Exception as e:
            self.logger.error(f"Error applying filters: {e}")
            return False
    
    def _has_next_page(self) -> bool:
        """Check if there is a next page button"""
        next_page_elements = self._find_elements("products_next_page")
        return bool(next_page_elements)
    
    def _navigate_to_next_page(self) -> bool:
        """Navigate to the next page"""
        try:
            next_page_elements = self._find_elements("products_next_page")
            if next_page_elements:
                next_page_url = next_page_elements[0].get_attribute("href")
                if next_page_url:
                    return self._navigate_to_url(next_page_url)
                else:
                    self.logger.warning("Next page URL is empty")
                    return False
            else:
                self.logger.info("Next page button not found")
                return False
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False
    
    def _extract_products_from_page(self, item: Dict[str, Any], page: int) -> List[Dict]:
        """Extract products from current page"""
        products = []
        
        try:
            # Wait for products to load
            if not self._wait_for_page_ready():
                self.logger.warning(f"Page not ready on page {page}")
            
            product_containers = self._find_elements("products_container")
            
            if not product_containers:
                self.logger.warning(f"No product containers found on page {page}")
                return products
            
            self.logger.debug(f"Found {len(product_containers)} product containers on page {page}")

            for i, container in enumerate(product_containers, 1):
                try:
                    # Extract product data using container-specific search
                    product_url_elements = self._find_elements("products_url", container)
                    # Skip if no URL found (essential field)
                    if not product_url_elements:
                        self.logger.debug(f"No URL found for product {i}, skipping")
                        continue

                    product_reviews = self._find_elements("products_reviews", container)
                    product_price = self._find_elements('products_price', container)

                    # Extract data with fallbacks
                    product_data = {
                        "N1": item["N1"],
                        "N2": item["N2"],
                        "N3": item["N3"],
                        "N4": item["N4"],
                        "N5": item["N5"],
                        "product_url": product_url_elements[0].get_attribute("href"),
                        "product_reviews": self.get_clean_text(product_reviews[0]) if product_reviews else "",
                        "product_price": product_price[0].get_attribute("innerHTML") if product_price else "",
                        "N3_url": item["N3_url"],
                        "N4_url": item["N4_url"],
                        "N5_url": item["N5_url"],
                        "page": page,
                        "position_in_page": i,
                        'scraped_at': datetime.now().isoformat(),
                        'process_id': self.process_id
                    }
                    products.append(product_data)
                    self.logger.debug(f"Extracted product {i}: {product_data.get('product_url', '')}")
                
                except Exception as e:
                    self.logger.warning(f"Error processing product container {i}: {e}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(products)} products from page {page}")
            return products
        
        except Exception as e:
            self.logger.error(f"Error extracting products from page {page}: {e}")
            return products


def load_n5_categories(output_dir: Path, key: str) -> List[Dict[str, Any]]:
    """Load N5 categories from input file"""
    input_file = output_dir / f"N1N5_{key}.csv"
    
    try:
        if not input_file.exists():
            print(f"Input file not found: {input_file}")
            return []
        
        input_data = pd.read_csv(input_file, encoding='utf-8')
        
        # Check required columns
        required_columns = ['N1', 'N2', 'N3', 'N4', 'N5', 'N3_url', 'N4_url', 'N5_url', 'lower_price', 'higher_price']
        missing_columns = [col for col in required_columns if col not in input_data.columns]
        if missing_columns:
            print(f"Input file is missing required columns: {missing_columns}")
            return []
        
        if input_data.empty:
            print("Input file is empty, no categories to process")
            return []
        
        print(f"Total rows in input file: {len(input_data)}")
        
        # Filter out rows where N5_url is empty or null
        before_filter = len(input_data)
        input_data = input_data.dropna(subset=['N5_url'])
        input_data = input_data[input_data['N5_url'].str.strip() != '']
        after_filter = len(input_data)
        
        if before_filter != after_filter:
            print(f"Filtered out {before_filter - after_filter} rows with empty N5_url")
        
        if input_data.empty:
            print("No valid N5 URLs found in input file after filtering")
            return []
        
        # Convert DataFrame to list of dictionaries and handle NaN values
        input_data = input_data.fillna('')
        categories = input_data.to_dict(orient='records')
        
        if not categories:
            print("Input file has no valid data")
            return []
        
        print(f"Loaded {len(categories)} N5 categories to process")
        return categories
        
    except Exception as e:
        print(f"Failed to load input categories: {e}")
        return []


def run_products_urls_scraper(config_dir=None, output_dir=None, logs_dir=None, 
                             num_processes=7, is_headless=True, translation=False,
                             apply_four_stars_filter=True, apply_shipping_by_amazon_filter=True,
                             apply_price_filter=True, max_pages=100):
    """Run Products URLs scraper with multiprocessing"""
    
    # Setup directories
    config_dir = Path(config_dir) if config_dir else Path("./config")
    output_dir = Path(output_dir) if output_dir else Path("./results")
    logs_dir = Path(logs_dir) if logs_dir else Path("./logs")
    
    # Load configuration to get key
    config_file = config_dir / "config.json"
    if not config_file.exists():
        print(f"Configuration file not found: {config_file}")
        return
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        key = config.get('KEY', 'default')
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return
    
    # Load N5 categories to process
    categories_to_process = load_n5_categories(output_dir, key)
    
    if not categories_to_process:
        print("No categories to process. Exiting.")
        return
    
    print(f"Starting Products URLs scraping for {len(categories_to_process)} N5 categories")
    
    # Create a custom scraper class with the specific parameters
    class ProductsURLSScraper(MainProductsURLSScraper):
        def __init__(self, config_dir=None, output_dir=None, logs_dir=None, 
                     is_headless=False, translation=False, process_id=None):
            super().__init__(
                config_dir=config_dir,
                output_dir=output_dir,
                logs_dir=logs_dir,
                is_headless=is_headless,
                translation=translation,
                process_id=process_id,
                apply_four_stars_filter=apply_four_stars_filter,
                apply_shipping_by_amazon_filter=apply_shipping_by_amazon_filter,
                apply_price_filter=apply_price_filter,
                max_pages=max_pages
            )
    
    # Run multiprocess scraper
    run_multiprocess_scraper(
        scraper_class=ProductsURLSScraper,
        items_to_scrape=categories_to_process,
        num_processes=num_processes,
        config_dir=config_dir,
        output_dir=output_dir,
        logs_dir=logs_dir,
        is_headless=is_headless,
        translation=translation
    )


if __name__ == "__main__":
    run_products_urls_scraper(
        config_dir="../config",
        output_dir="../results",
        logs_dir="../logs",
        num_processes=8,
        is_headless=True,
        translation=False,
        apply_four_stars_filter=True,
        apply_shipping_by_amazon_filter=True,
        apply_price_filter=True,
        max_pages=100
    )