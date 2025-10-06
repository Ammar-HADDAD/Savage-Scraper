#!/usr/bin/env python3
"""
Products Scraper - Extracts products from product URLs
"""

from selenium.webdriver.common.by import By
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import json
import time
from datetime import datetime
from selenium.webdriver.remote.webelement import WebElement
from .SavageScraper import SavageScraper, run_multiprocess_scraper


class ProductsScraper(SavageScraper):
    """Scraper for extracting products from product URLs"""
    
    def _get_progress_tracking_key(self) -> str:
        """Get the key used for products progress tracking"""
        return "product_url"
    
    def _get_output_file_path(self) -> Path:
        """Get the output file path for products data"""
        return self.output_dir / f"Products_{self.key}.csv"
    
    def _get_required_selectors(self) -> List[str]:
        """Get required selector keys for products scraping"""
        return [
            'product_name',
            'product_image',
            'product_rating',
            'product_price',
            'product_seller',
            'product_sender',
            'product_sales',
            'product_reviews',
            'product_metadata1',
            'product_metadata2',
            'product_metadata3',
            'product_tags',
            'product_ready',
            'error_page_indicator',
            'error_page_handler'
        ]
    
    def _get_page_ready_selector(self) -> str:
        """Get the selector key for product page ready indicator"""
        return "product_ready"
    
    def _get_categories_selector(self) -> str:
        """Get the selector key for product elements (not used in this scraper)"""
        return "product_name"
    
    def _get_resume_key(self) -> str:
        """Get the key used to check if an item was already processed (for resume functionality)"""
        return "product_url"  # For products scraper, we check if the product_url was already processed
    
    def _process_category_element(self, element: WebElement, item: Dict[str, Any]) -> Dict[str, Any]:
        """Not used in products scraper - we override _process_single_item instead"""
        return None
    
    def _create_empty_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Create an empty result when product extraction fails"""
        return {
            "name": "",
            "image": "",
            "rating": "",
            "price": "",
            "reviews": "",
            "seller": "",
            "sender": "",
            "sales": "",
            "metadata1": "{}",
            "metadata2": "{}",
            "metadata3": "{}",
            "tags": "",
            "N1": item["N1"],
            "N2": item["N2"],
            "N3": item["N3"],
            "N4": item["N4"],
            "N5": item["N5"],
            "N3_url": item["N3_url"],
            "N4_url": item["N4_url"],
            "N5_url": item["N5_url"],
            "product_url": item["product_url"],
            "page": item["page"],
            "position_in_page": item["position_in_page"],
            'scraped_at': datetime.now().isoformat(),
            'process_id': self.process_id
        }
    
    def _process_single_item(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single product URL to extract product details"""
        try:
            # Navigate to the product URL
            if not self._navigate_to_url(item[self.progress_tracking_key]):
                self.logger.error(f"Failed to navigate to {item[self.progress_tracking_key]}")
                return []

            # Wait for product to load
            if not self._wait_for_page_ready():
                self.logger.warning("Product page not ready")

            time.sleep(1)

            # Extract product information
            product_data = self._extract_product_data(item)
            
            if product_data:
                self.logger.info(f"Successfully processed product: {product_data.get('name', '')}")
                return [product_data]
            else:
                self.logger.warning("Failed to extract product data")
                return [self._create_empty_result(item)]
           
        except Exception as e:
            self.logger.error(f"Error processing product {item['product_url']}: {e}")
            return [self._create_empty_result(item)]
    
    def _extract_product_data(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract all product data from the current page"""
        try:
            # Find all product elements
            product_name = self._find_elements("product_name")
            product_image = self._find_elements("product_image")
            product_rating = self._find_elements("product_rating")
            product_price = self._find_elements("product_price")
            product_reviews = self._find_elements("product_reviews")
            product_seller = self._find_elements("product_seller")
            product_sender = self._find_elements("product_sender")
            product_sales = self._find_elements("product_sales")
            products_tags = self._find_elements("product_tags")
            
            # Extract metadata tables
            product_metadata1 = self._extract_metadata_table("product_metadata1")
            product_metadata2 = self._extract_metadata_table("product_metadata2")
            product_metadata3 = self._extract_metadata_table("product_metadata3")

            # Build product data
            product_data = {
                "name": product_name[0].text.strip() if product_name else "",   
                "image": product_image[0].get_attribute("src") if product_image else "",
                "rating": product_rating[0].text.strip() if product_rating else "",
                "price": self.get_clean_text(product_price[0]) if product_price else "",
                "reviews": product_reviews[0].text.strip() if product_reviews else "",
                "seller": product_seller[0].text.strip() if product_seller else "",
                "sender": product_sender[0].text.strip() if product_sender else "",
                "sales": self.get_clean_text(product_sales[0]) if product_sales else "",
                "metadata1": json.dumps(product_metadata1) if product_metadata1 else "{}",
                "metadata2": json.dumps(product_metadata2) if product_metadata2 else "{}",
                "metadata3": json.dumps(product_metadata3) if product_metadata3 else "{}",
                "tags": self.get_clean_text(products_tags[0]) if products_tags else "",
                "N1": item["N1"],
                "N2": item["N2"],
                "N3": item["N3"],
                "N4": item["N4"],
                "N5": item["N5"],
                "N3_url": item["N3_url"],
                "N4_url": item["N4_url"],
                "N5_url": item["N5_url"],
                "product_url": item["product_url"],
                "page": item["page"],
                "position_in_page": item["position_in_page"],
                'scraped_at': datetime.now().isoformat(),
                'process_id': self.process_id
            }
            
            self.logger.debug(f"Extracted product: {product_data.get('name', '')}")
            return product_data
            
        except Exception as e:
            self.logger.error(f"Error extracting product data: {e}")
            return None
    
    def _extract_metadata_table(self, selector_key: str) -> Dict[str, str]:
        """Extract table data and return as dictionary"""
        try:
            table_elements = self._find_elements(selector_key)
            if not table_elements:
                return {}
            
            table_element = table_elements[0]
            data = {}
            
            rows = table_element.find_elements(By.TAG_NAME, "tr")
            
            for row in rows:
                try:
                    cells = row.find_elements(By.CSS_SELECTOR, "th, td")
                    if len(cells) >= 2:
                        key = cells[0].text.strip()
                        value = cells[1].text.strip()
                        if key and value:
                            data[key] = value
                except Exception:
                    continue
            
            return data
            
        except Exception as e:
            self.logger.warning(f"Error extracting metadata table {selector_key}: {e}")
            return {}


def load_product_urls(output_dir: Path, key: str) -> List[Dict[str, Any]]:
    """Load product URLs from input file"""
    input_file = output_dir / f"ProductsURLS_{key}.csv"
    
    try:
        if not input_file.exists():
            print(f"Input file not found: {input_file}")
            return []
        
        input_data = pd.read_csv(input_file, encoding='utf-8')
        
        # Check required columns
        required_columns = ['N1', 'N2', 'N3', 'N4', 'N5', 'product_url', 'N3_url', 'N4_url', 'N5_url', 'page', 'position_in_page']
        missing_columns = [col for col in required_columns if col not in input_data.columns]
        if missing_columns:
            print(f"Input file is missing required columns: {missing_columns}")
            return []
        
        if input_data.empty:
            print("Input file is empty, no products to process")
            return []
        
        print(f"Total rows in input file: {len(input_data)}")
        
        # Filter out rows where product_url is empty or null
        before_filter = len(input_data)
        input_data = input_data.dropna(subset=['product_url'])
        input_data = input_data[input_data['product_url'].str.strip() != '']
        after_filter = len(input_data)
        
        if before_filter != after_filter:
            print(f"Filtered out {before_filter - after_filter} rows with empty product_url")
        
        if input_data.empty:
            print("No valid product URLs found in input file after filtering")
            return []
        
        # Convert DataFrame to list of dictionaries and handle NaN values
        input_data = input_data.fillna('')
        products = input_data.to_dict(orient='records')
        
        if not products:
            print("Input file has no valid data")
            return []
        
        print(f"Loaded {len(products)} product URLs to process")
        return products
        
    except Exception as e:
        print(f"Failed to load input products: {e}")
        return []


def run_products_scraper(config_dir=None, output_dir=None, logs_dir=None, 
                        num_processes=4, is_headless=True, translation=True):
    """Run Products scraper with multiprocessing"""
    
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
    
    # Load product URLs to process
    products_to_process = load_product_urls(output_dir, key)
    
    if not products_to_process:
        print("No products to process. Exiting.")
        return
    
    print(f"Starting Products scraping for {len(products_to_process)} product URLs")
    
    # Run multiprocess scraper
    run_multiprocess_scraper(
        scraper_class=ProductsScraper,
        items_to_scrape=products_to_process,
        num_processes=num_processes,
        config_dir=config_dir,
        output_dir=output_dir,
        logs_dir=logs_dir,
        is_headless=is_headless,
        translation=translation
    )

if __name__ == "__main__":
    run_products_scraper(
        config_dir="../config",
        output_dir="../results",
        logs_dir="../logs",
        num_processes=8,
        is_headless=True,
        translation=True
    )