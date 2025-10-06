#!/usr/bin/env python3
"""
N1-N3 Scraper - Extracts top-level categories examaples: N1, N2, N3
from a website's hamburger menu using Selenium.
"""

from selenium.webdriver.common.by import By
from pathlib import Path
from typing import List, Dict, Any
from tqdm import tqdm
from datetime import datetime
from selenium.webdriver.remote.webelement import WebElement
from .SavageScraper import SavageScraper, run_multiprocess_scraper
import json


class N1N3Scraper(SavageScraper):
    """Scraper for extracting N1, N2, and N3 level categories"""
    
    def _get_progress_tracking_key(self) -> str:
        """Get the key used for N1-N3 categories progress tracking"""
        # N1N3 scraper doesn't process individual items with URLs, 
        # it scrapes the main page once
        return "page_url"
    
    def _get_output_file_path(self) -> Path:
        """Get the output file path for N1-N3 data"""
        return self.output_dir / f"N1N3_{self.key}.csv"
    
    def _get_required_selectors(self) -> List[str]:
        """Get required selector keys for N1-N3 scraping"""
        return [
            'hamburger_menu', 
            'N1', 
            'N2N3', 
            'N1N2N3_ready', 
            'error_page_indicator', 
            'error_page_handler'
        ]
    
    def _get_page_ready_selector(self) -> str:
        """Get the selector key for N1-N3 page ready indicator"""
        return "N1N2N3_ready"
    
    def _get_categories_selector(self) -> str:
        """Get the selector key for N1 category elements"""
        return "N1"
    
    def _process_category_element(self, element: WebElement, item: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single N1 category element and extract its subcategories"""
        # This method is called for each N1 category
        # We need to extract all N2/N3 subcategories for this N1
        try:
            N1_id = element.get_attribute('data-menu-id')
            N1_name = element.find_element(By.CSS_SELECTOR, "div").get_attribute("innerHTML")
            
            if hasattr(self, 'logger'):
                self.logger.info(f"Processing N1 category: {N1_name}")
            
            # Find corresponding N2N3 subcategories using the lookup dictionary
            if hasattr(self, '_N2_N3_lookup') and N1_id in self._N2_N3_lookup:
                N1_subcategories = self._N2_N3_lookup[N1_id]
                
                li_elements = N1_subcategories.find_elements(By.XPATH, ".//li")
                results = []
                
                for li_element in li_elements:
                    try:
                        a_element = li_element.find_element(By.TAG_NAME, "a")
                        N3_url = a_element.get_attribute("href")
                        N3_name = a_element.get_attribute("innerHTML")

                        parent_section = li_element.find_element(By.XPATH, "./ancestor::section[1]")
                        N2_name = parent_section.find_element(By.XPATH, "./div").get_attribute("innerHTML")

                        result_item = {
                            "N1": N1_name,
                            "N2": N2_name,
                            "N3": N3_name,
                            "N3_url": N3_url,
                            'lower_price': "0",
                            'higher_price': "99999",
                            'scraped_at': datetime.now().isoformat(),
                            'process_id': self.process_id
                        }
                        results.append(result_item)
                            
                    except Exception as e:
                        if hasattr(self, 'logger'):
                            self.logger.warning(f"Error processing li element: {e}")
                        continue
                
                # Store results for this N1 category to be returned
                self._current_n1_results = results
                return {"processed": True, "count": len(results)}
            else:
                if hasattr(self, 'logger'):
                    self.logger.warning(f"No subcategories found for {N1_name}")
                return {"processed": True, "count": 0}
                
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"Error processing N1 element: {e}")
            return None
    
    def _create_empty_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Create an empty result when no categories are found"""
        return {
            "N1": "",
            "N2": "",
            "N3": "",
            "N3_url": "",
            'lower_price': "0",
            'higher_price': "99999",
            'scraped_at': datetime.now().isoformat(),
            'process_id': self.process_id
        }
    
    def _get_resume_key(self) -> str:
        """Get the key used to check if an item was already processed (for resume functionality)"""
        # For N1N3, we can use N3_url as the resume key
        return "N3_url"
    
    def _process_single_item(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Override the single item processing for N1N3 specific logic"""
        try:
            # Navigate to the main page
            if not self._navigate_to_url(self.base_url):
                if hasattr(self, 'logger'):
                    self.logger.error(f"Failed to navigate to {self.base_url}")
                return []
            
            # Open hamburger menu
            if not self._open_hamburger_menu():
                return []
            
            # Extract all categories
            return self._extract_all_categories()
            
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"Error in N1N3 processing: {e}")
            return []
    
    def _open_hamburger_menu(self) -> bool:
        """Open hamburger menu"""
        if hasattr(self, 'logger'):
            self.logger.info("Opening hamburger menu")
        
        for selector in self.selectors["hamburger_menu"]:
            if self._click_element(selector):
                if hasattr(self, 'logger'):
                    self.logger.info("Hamburger menu opened successfully")
                return True
        
        if hasattr(self, 'logger'):
            self.logger.error("Failed to open hamburger menu")
        return False
    
    def _extract_all_categories(self) -> List[Dict[str, Any]]:
        """Extract all N1, N2, N3 categories"""
        N1_elements = self._find_elements("N1")
        N2_N3_elements = self._find_elements("N2N3")
        
        if not N1_elements or not N2_N3_elements:
            if hasattr(self, 'logger'):
                self.logger.warning("No categories found")
            return []
        
        if hasattr(self, 'logger'):
            self.logger.info(f"Found {len(N1_elements)} N1 categories")
        
        # Build lookup dictionary for N2N3 elements
        self._N2_N3_lookup = {}
        for N2_N3_element in N2_N3_elements:
            menu_id = N2_N3_element.get_attribute('data-menu-id')
            if menu_id:
                self._N2_N3_lookup[menu_id] = N2_N3_element
        
        # Process each N1 category and collect all results
        all_results = []
        
        for N1_element in N1_elements:
            try:
                N1_id = N1_element.get_attribute('data-menu-id')
                N1_name = N1_element.find_element(By.CSS_SELECTOR, "div").get_attribute("innerHTML")
                
                # Find corresponding N2N3 subcategories
                if N1_id in self._N2_N3_lookup:
                    N1_subcategories = self._N2_N3_lookup[N1_id]
                    
                    li_elements = N1_subcategories.find_elements(By.XPATH, ".//li")
                    
                    if hasattr(self, 'logger'):
                        self.logger.info(f"Processing {len(li_elements)} subcategories for {N1_name}")
                    
                    for li_element in li_elements:
                        try:
                            a_element = li_element.find_element(By.TAG_NAME, "a")
                            N3_url = a_element.get_attribute("href")
                            N3_name = a_element.get_attribute("innerHTML")

                            parent_section = li_element.find_element(By.XPATH, "./ancestor::section[1]")
                            N2_name = parent_section.find_element(By.XPATH, "./div").get_attribute("innerHTML")

                            result_item = {
                                "N1": N1_name,
                                "N2": N2_name,
                                "N3": N3_name,
                                "N3_url": N3_url,
                                'lower_price': "0",
                                'higher_price': "99999",
                                'scraped_at': datetime.now().isoformat(),
                                'process_id': self.process_id
                            }
                            all_results.append(result_item)
                                
                        except Exception as e:
                            if hasattr(self, 'logger'):
                                self.logger.warning(f"Error processing li element: {e}")
                            continue
                else:
                    if hasattr(self, 'logger'):
                        self.logger.warning(f"No subcategories found for {N1_name}")
                        
            except Exception as e:
                if hasattr(self, 'logger'):
                    self.logger.error(f"Error processing N1 element: {e}")
                continue
        
        if hasattr(self, 'logger'):
            self.logger.info(f"Extracted total of {len(all_results)} N3 categories")
        
        return all_results


def run_n1n3_scraper(config_dir=None, output_dir=None, logs_dir=None, 
                     num_processes=1, is_headless=True, translation=False):
    """Run N1N3 scraper - typically with single process since it scrapes one main page"""
    
    # Setup directories
    config_dir = Path(config_dir) if config_dir else Path("./config")
    output_dir = Path(output_dir) if output_dir else Path("./results")
    logs_dir = Path(logs_dir) if logs_dir else Path("./logs")
    
    # Load configuration to get base URL
    config_file = config_dir / "config.json"
    if not config_file.exists():
        print(f"Configuration file not found: {config_file}")
        return
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        base_url = config.get('BASE_URL', '')
        if not base_url:
            print("BASE_URL not found in configuration")
            return
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return
    
    items_to_process = [{"page_url": base_url}]
    
    print("Starting N1N3 scraping...")
    
    # Run multiprocess scraper
    run_multiprocess_scraper(
        scraper_class=N1N3Scraper,
        items_to_scrape=items_to_process,
        num_processes=num_processes,  # 1 for N1N3
        config_dir=config_dir,
        output_dir=output_dir,
        logs_dir=logs_dir,
        is_headless=is_headless,
        translation=translation
    )


if __name__ == "__main__":
    run_n1n3_scraper(
        config_dir="./config",
        output_dir="./results",
        logs_dir="./logs",
        num_processes=1,
        is_headless=True,
        translation=False
    )
