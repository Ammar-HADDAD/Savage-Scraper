#!/usr/bin/env python3
"""
Scraping framework with built in support for multiprocessing, logging, progress tracking,
error handling, and resume functionality.
Designed to be subclassed for specific scraping tasks.

Author: Ammar HADDAD - ammarhaddad@outlook.fr
"""

import multiprocessing as mp
import threading
import queue
import time
import json
import logging
import sys
import signal
import os
import fcntl
from pathlib import Path
from typing import List, Dict, Any, Type, Optional
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.remote.webelement import WebElement
import random
import re


class SavageScraper(ABC):
    """Base class for scrapers"""
    
    # Common constants - can be overridden by subclasses
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    
    WAIT_TIMEOUT = 3
    
    def __init__(self, config_dir=None, output_dir=None, logs_dir=None, 
                 error_pages_dir=None, is_headless=False, translation=False, process_id=None):
        self.process_id = process_id or 0
        self.config_dir = Path(config_dir) if config_dir else Path("../config")
        self.output_dir = Path(output_dir) if output_dir else Path("../results")
        self.logs_dir = Path(logs_dir) if logs_dir else Path("../logs")
        self.error_pages_dir = Path(error_pages_dir) if error_pages_dir else Path("../error_pages")
        self.is_headless = is_headless
        self.translation = translation
        
        # Selenium components
        self.driver = None
        self.wait = None
        
        # Create directories
        self.config_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        self.error_pages_dir.mkdir(exist_ok=True)
        
        # Load configuration
        self._load_configuration()
        
        # Set up progress tracking
        self.progress_tracking_key = self._get_progress_tracking_key()
    
    def _load_configuration(self):
        """Load configuration from JSON file"""
        config_file = self.config_dir / "config.json"
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            self.key = self.config.get('KEY', 'default')
            self.country = self.config.get('COUNTRY', 'MA')
            self.base_url = self.config.get('BASE_URL', '')
            self.selectors = self.config.get('SELECTORS', {})
        except Exception as e:
            print(f"Error loading configuration: {e}")
            self.config = {}
            self.key = 'default'
    
    @abstractmethod
    def _get_progress_tracking_key(self) -> str:
        """Get the key used for progress tracking"""
        pass
    
    @abstractmethod
    def _get_output_file_path(self) -> Path:
        """Get the output file path"""
        pass
    
    @abstractmethod
    def _get_required_selectors(self) -> List[str]:
        """Get required selector keys"""
        pass
    
    @abstractmethod
    def _get_page_ready_selector(self) -> str:
        """Get the selector key for page ready indicator"""
        pass
    
    @abstractmethod
    def _get_categories_selector(self) -> str:
        """Get the selector key for category elements"""
        pass
    
    @abstractmethod
    def _process_category_element(self, element: WebElement, item: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single category element and return the result item"""
        pass
    
    @abstractmethod
    def _create_empty_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Create an empty result when no categories are found"""
        pass
    
    @abstractmethod
    def _get_resume_key(self) -> str:
        """Get the key used to check if an item was already processed (for resume functionality)"""
        pass
    
    def _load_existing_results(self) -> set:
        """Load existing results to support resume functionality"""
        output_file = self._get_output_file_path()
        if not output_file.exists():
            return set()
        
        try:
            existing_df = pd.read_csv(output_file, encoding='utf-8')
            if existing_df.empty:
                return set()
            
            resume_key = self._get_resume_key()
            if resume_key not in existing_df.columns:
                if hasattr(self, 'logger'):
                    self.logger.warning(f"Resume key '{resume_key}' not found in existing output file")
                return set()
            
            # Get unique values of the resume key
            existing_keys = set(existing_df[resume_key].dropna().astype(str))
            if hasattr(self, 'logger'):
                self.logger.info(f"Found {len(existing_keys)} existing entries in output file")
            return existing_keys
            
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.warning(f"Error loading existing results for resume: {e}")
            return set()
    
    def _filter_items_for_resume(self, items: List[Dict]) -> List[Dict]:
        """Filter out items that have already been processed"""
        existing_keys = self._load_existing_results()
        if not existing_keys:
            return items
        
        resume_key = self._get_resume_key()
        filtered_items = []
        
        for item in items:
            item_key = str(item.get(resume_key, ''))
            if item_key and item_key not in existing_keys:
                filtered_items.append(item)
        
        skipped_count = len(items) - len(filtered_items)
        if skipped_count > 0:
            if hasattr(self, 'logger'):
                self.logger.info(f"Resume: Skipping {skipped_count} already processed items")
            else:
                print(f"Resume: Skipping {skipped_count} already processed items")
        
        return filtered_items
    
    def setup_process_logging(self, log_queue: mp.Queue):
        """Setup logging for this process to send to main logger"""
        # Create a logger for this process
        self.logger = logging.getLogger(f"{self.__class__.__name__}_P{self.process_id}")
        self.logger.setLevel(logging.INFO)
        
        # Remove any existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Add queue handler
        queue_handler = QueueHandler(log_queue)
        queue_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            f'%(asctime)s - P{self.process_id} - %(name)s - %(levelname)s - %(message)s'
        )
        queue_handler.setFormatter(formatter)
        self.logger.addHandler(queue_handler)
        
        # Prevent propagation to avoid duplicate logs
        self.logger.propagate = False
    
    def _init_driver(self) -> bool:
        """Initialize Chrome WebDriver"""
        try:
            # Clean up any existing driver
            if self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.driver = None
            
            options = Options()
            
            if self.is_headless:
                options.add_argument("--headless")
            
            # Basic options for Linux server
            options.add_argument("--start-maximized")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-web-security")
            options.add_argument("--disable-features=VizDisplayCompositor")
            options.add_argument("--lang=en-US")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-logging")
            options.add_argument("--disable-gpu-logging")
            options.add_argument("--silent")
            
            # Random user agent
            user_agent = random.choice(self.USER_AGENTS)
            options.add_argument(f"--user-agent={user_agent}")

            # Anti-detection
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Suppress logging
            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
            }

            if self.translation:
                # Language preferences for automatic translation
                prefs.update({
                    "intl.accept_languages": "en-US,en",
                    "translate": {"enabled": True},
                    "translate_whitelists": {self.key: "en"}
                })
            
            options.add_experimental_option("prefs", prefs)
            
            if hasattr(self, 'logger'):
                self.logger.info(f"Initializing Chrome WebDriver for process {self.process_id}")
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.set_window_size(1920, 1080)
            
            self.wait = WebDriverWait(self.driver, self.WAIT_TIMEOUT)
            
            if hasattr(self, 'logger'):
                self.logger.info(f"WebDriver initialized successfully for process {self.process_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to initialize WebDriver for process {self.process_id}: {e}"
            if hasattr(self, 'logger'):
                self.logger.error(error_msg)
            else:
                print(error_msg)
            
            # Clean up on failure
            if self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.driver = None
            
            return False
    
    def _wait_for_page_ready(self) -> bool:
        """Wait for page ready indicator"""
        elements = self._find_elements(self._get_page_ready_selector())
        return bool(elements)
    
    def _navigate_to_url(self, url: str) -> bool:
        """Navigate to URL and wait for page ready"""
        try:
            if hasattr(self, 'logger'):
                self.logger.info(f"Navigating to: {url}")
            self.driver.get(url)
            
            # Wait for page to be ready
            if self._wait_for_page_ready():
                if hasattr(self, 'logger'):
                    self.logger.info("Page loaded successfully")
                return True
            else:
                if hasattr(self, 'logger'):
                    self.logger.warning("Page not ready, checking for error page")
                if self._is_error_page():
                    if self._handle_error_page():
                        return self._navigate_to_url(url)  # Retry
                else:
                    error_page_filename = self.error_pages_dir / f"./{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                    with open(error_page_filename, "w", encoding="utf-8") as f:
                        f.write(self.driver.page_source)
                    
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"Navigation failed: {e}")
        
        return False
    
    def _is_error_page(self) -> bool:
        """Check if current page is an error page"""
        error_elements = self._find_elements("error_page_indicator")
        if error_elements:
            if hasattr(self, 'logger'):
                self.logger.warning("Error page detected")
            return True
        return False
    
    def _handle_error_page(self) -> bool:
        """Handle error page by clicking appropriate elements"""
        for selector in self.selectors.get('error_page_handler', []):
            if self._click_element(selector):
                if hasattr(self, 'logger'):
                    self.logger.info("Error page handled successfully")
                return True

        if hasattr(self, 'logger'):
            self.logger.error("Failed to handle error page")
        return False
    
    def _find_elements(self, selector_key: str, container=None) -> List:
        """Find elements using configured selectors"""
        if selector_key not in self.selectors:
            if hasattr(self, 'logger'):
                self.logger.error(f"Selector key '{selector_key}' not found in configuration")
            return []
        
        container = container or self.driver
        
        for selector in self.selectors[selector_key]:
            try:
                by_type = self._get_selector_type(selector)
                self.wait.until(EC.presence_of_element_located((by_type, selector)))
                elements = container.find_elements(by_type, selector)
                if elements:
                    return elements
            except TimeoutException:
                continue
            except Exception:
                continue
        
        return []
    
    def _get_selector_type(self, selector: str):
        """Determine selector type (CSS or XPath)"""
        if selector.startswith(('//', './', '/', '(/','(./)')):
            return By.XPATH
        return By.CSS_SELECTOR
    
    def _click_element(self, selector: str) -> bool:
        """Click element by selector"""
        try:
            by_type = self._get_selector_type(selector)
            element = self.wait.until(EC.element_to_be_clickable((by_type, selector)))
            element.click()
            return True
        except Exception:
            return False
    
    def get_clean_text(self, element: WebElement) -> str:
        """Extract clean, visible text from a WebElement."""
        if not element:
            return ""
        
        try:
            # Try innerText first (best for visible text)
            text = element.get_attribute('innerText')
            if text and text.strip():
                return self._clean_text(text)
            
            # Fallback to .text
            text = element.text
            if text and text.strip():
                return self._clean_text(text)
            
            # Last resort: innerHTML with HTML tags removed
            text = element.get_attribute('innerHTML')
            if text and text.strip():
                # Remove HTML tags
                text = re.sub(r'<[^>]+>', '', text)
                return self._clean_text(text)
                
        except Exception:
            pass
        
        return ""
    
    def _clean_text(self, text: str) -> str:
        """Clean up extracted text"""
        if not text:
            return ""
        # Remove HTML tags first
        text = re.sub(r'<[^>]+>', '', text)
        # Replace HTML entities
        text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def process_batch(self, items_batch: List[Dict], log_queue: mp.Queue, 
                     output_queue: mp.Queue, progress_queue: mp.Queue):
        """Process a batch of items in a separate process"""
        try:
            # Setup logging for this process
            self.setup_process_logging(log_queue)
            
            self.logger.info(f"Starting batch processing of {len(items_batch)} items in process {self.process_id}")
            
            # Initialize driver in the worker process
            if not self._init_driver():
                self.logger.error("Failed to initialize WebDriver in worker process")
                # Signal failure for each item in the batch
                for _ in items_batch:
                    progress_queue.put(-1)
                return
            
            # Process items one by one and send results
            for i, item in enumerate(items_batch):
                try:
                    if not item.get(self.progress_tracking_key):
                        self.logger.warning(f"Skipping item without {self.progress_tracking_key}: {item}")
                        progress_queue.put(0)  # No results but item was processed
                        continue
                    
                    self.logger.info(f"[{i+1}/{len(items_batch)}] Processing item in process {self.process_id}")
                    
                    results = self._process_single_item(item)
                    if results:
                        # Send each result to output queue
                        for result in results:
                            output_queue.put(result)
                        
                        # Signal progress: number of output items generated from this input item
                        progress_queue.put(len(results))
                        self.logger.info(f"Successfully processed item - generated {len(results)} output items")
                    else:
                        self.logger.warning(f"No results from item processing")
                        progress_queue.put(0)  # No results but item was processed
                    
                except Exception as e:
                    self.logger.error(f"Error processing item {item}: {e}")
                    progress_queue.put(-1)  # Signal error for this input item
                    continue
            
            self.logger.info(f"Completed batch processing")
                
        except Exception as e:
            error_msg = f"Error in process {self.process_id}: {e}"
            import traceback
            error_details = f"{error_msg}\nTraceback: {traceback.format_exc()}"
            
            # Send error to log queue
            log_queue.put(logging.makeLogRecord({
                'name': f"{self.__class__.__name__}_P{self.process_id}",
                'level': logging.ERROR,
                'pathname': '',
                'lineno': 0,
                'msg': error_details,
                'args': (),
                'exc_info': None,
                'created': time.time()
            }))
            
            # Signal failure for any remaining items
            for _ in items_batch:
                progress_queue.put(-1)
        finally:
            # Clean up driver
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info("WebDriver closed successfully")
                except Exception as e:
                    self.logger.warning(f"Error closing WebDriver: {e}")
            
            # Signal process completion
            progress_queue.put('DONE')
    
    def _process_single_item(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single item to extract categories"""
        try:
            # Navigate to the category page
            if not self._navigate_to_url(item[self.progress_tracking_key]):
                self.logger.error(f"Failed to navigate to {item[self.progress_tracking_key]}")
                return []
            
            new_items = []
            
            # Find category elements on the page
            category_elements = self._find_elements(self._get_categories_selector())
            
            if not category_elements:
                self.logger.warning(f"No categories found, creating empty result")
                # Create empty entry to mark as processed
                empty_result = self._create_empty_result(item)
                new_items.append(empty_result)
            else:
                self.logger.info(f"Found {len(category_elements)} category elements")
            
                # Extract each category
                for category_element in category_elements:
                    try:
                        result_item = self._process_category_element(category_element, item)
                        if result_item:
                            new_items.append(result_item)
                    
                    except Exception as e:
                        self.logger.warning(f"Error processing category element: {e}")
                        continue
            
            self.logger.info(f"Successfully extracted {len(new_items)} categories")
            return new_items
        
        except Exception as e:
            self.logger.error(f"Error processing item: {e}")
            return []


class QueueHandler(logging.Handler):
    """logging handler - sends logs to a multiprocessing queue"""
    
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
    
    def emit(self, record):
        try:
            self.log_queue.put(record)
        except Exception:
            pass  # Ignore errors in logging


class LoggerProcess:
    """Process for handling all logging"""
    
    def __init__(self, log_queue: mp.Queue, log_file: Path):
        self.log_queue = log_queue
        self.log_file = log_file
        self.should_stop = False
    
    def run(self):
        """Main logger process loop"""
        # Setup file logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        logger = logging.getLogger('MainLogger')
        
        logger.info("Logger process started")
        
        while not self.should_stop:
            try:
                # Get log record with timeout
                record = self.log_queue.get(timeout=1.0)
                if record is None:  # Shutdown signal
                    break
                
                # Handle the log record
                if isinstance(record, logging.LogRecord):
                    logger.handle(record)
                else:
                    logger.info(str(record))
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in logger process: {e}")
        
        logger.info("Logger process stopped")
    
    def stop(self):
        """Signal logger to stop"""
        self.should_stop = True


class OutputManager:
    """Thread-safe output file manager"""
    
    def __init__(self, output_file: Path):
        self.output_file = output_file
        self.lock = threading.Lock()
        self._ensure_file_exists()
        self.headers_written = False
    
    def _ensure_file_exists(self):
        """Ensure output file exists"""
        if not self.output_file.exists():
            self.output_file.touch()
    
    @contextmanager
    def _file_lock(self, file_handle):
        """Context manager for file locking"""
        try:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
    
    def append_single_result(self, result: Dict, logger: logging.Logger = None):
        """Thread-safe append single result to output file"""
        log = logger or logging.getLogger('OutputManager')
        
        with self.lock:
            try:
                # Check if file is empty or headers not written
                file_is_empty = self.output_file.stat().st_size == 0
                
                # Convert result to DataFrame
                df_result = pd.DataFrame([result])
                
                # Write to file
                with open(self.output_file, 'a', encoding='utf-8', newline='') as f:
                    with self._file_lock(f):
                        # Write headers if file is empty
                        write_header = file_is_empty or not self.headers_written
                        df_result.to_csv(f, index=False, header=write_header)
                        self.headers_written = True
                
                log.debug(f"Appended single result to output file")
                
            except Exception as e:
                log.error(f"Error appending single result to output file: {e}")
                # Fallback: write to backup file
                backup_file = self.output_file.with_suffix(f'.backup_{int(time.time())}.csv')
                try:
                    pd.DataFrame([result]).to_csv(backup_file, index=False, encoding='utf-8')
                    log.warning(f"Result saved to backup file: {backup_file}")
                except Exception as backup_e:
                    log.error(f"Failed to save backup file: {backup_e}")


class OutputWriterProcess:
    """Process for writing output in real-time"""
    
    def __init__(self, output_queue: mp.Queue, output_manager: OutputManager, log_queue: mp.Queue):
        self.output_queue = output_queue
        self.output_manager = output_manager
        self.log_queue = log_queue
        self.should_stop = False
        self.items_written = 0
    
    def run(self):
        """Main output writer process loop"""
        # Setup logging for this process
        logger = logging.getLogger('OutputWriter')
        logger.addHandler(QueueHandler(self.log_queue))
        logger.setLevel(logging.INFO)
        
        logger.info("Output writer process started")
        
        while not self.should_stop:
            try:
                # Get result with timeout
                result = self.output_queue.get(timeout=1.0)
                if result is None:  # Shutdown signal
                    break
                
                # Write result immediately
                self.output_manager.append_single_result(result, logger)
                self.items_written += 1
                
                if self.items_written % 10 == 0:  # Log every 10 items
                    logger.info(f"Written {self.items_written} items to output file")
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in output writer process: {e}")
        
        logger.info(f"Output writer process stopped. Total items written: {self.items_written}")
    
    def stop(self):
        """Signal output writer to stop"""
        self.should_stop = True


class ProgressTracker:
    """Track and display progress across multiple processes"""
    
    def __init__(self, total_input_items: int, logger: logging.Logger):
        self.total_input_items = total_input_items
        self.processed_input_items = 0
        self.total_output_items = 0
        self.failed_items = 0
        self.start_time = time.time()
        self.logger = logger
        self.lock = threading.Lock()
        self.last_update = 0
    
    def update(self, count: int):
        """Update progress count"""
        with self.lock:
            if count == -1:  # Error signal
                self.failed_items += 1
                self.processed_input_items += 1  # Still counts as processing an input item
            elif count == 0:  # No results found but item was processed
                self.processed_input_items += 1
            else:
                self.total_output_items += count
                self.processed_input_items += 1  # One input item was processed
            
            # Log progress every 10 seconds or on significant milestones
            current_time = time.time()
            if (current_time - self.last_update > 10 or 
                self.processed_input_items >= self.total_input_items):
                
                self._log_progress()
                self.last_update = current_time
    
    def update_process_completion(self):
        """Called when a process signals completion"""
        # This is just for logging process completion, doesn't affect progress calculation..
        pass
    
    def _log_progress(self):
        """Log current progress"""
        if self.processed_input_items == 0:
            return
            
        # Calculate progress based on input items processed
        progress_pct = (self.processed_input_items / self.total_input_items) * 100
        elapsed_time = time.time() - self.start_time
        
        if self.processed_input_items > 0 and self.processed_input_items < self.total_input_items:
            avg_time_per_item = elapsed_time / self.processed_input_items
            remaining_items = self.total_input_items - self.processed_input_items
            eta_seconds = remaining_items * avg_time_per_item
            
            # Handle negative ETA
            if eta_seconds < 0:
                eta_str = "Completing..."
            else:
                eta_str = str(timedelta(seconds=int(eta_seconds)))
        else:
            eta_str = "Completed" if self.processed_input_items >= self.total_input_items else "Calculating..."
        
        # Successful output items (total - failed)
        successful_output = self.total_output_items
        
        self.logger.info(
            f"Input Progress: {self.processed_input_items}/{self.total_input_items} ({progress_pct:.1f}%) | "
            f"Output Items: {successful_output} | Failed: {self.failed_items} | "
            f"ETA: {eta_str}"
        )


def split_items_into_batches(items: List[Dict], num_processes: int) -> List[List[Dict]]:
    """Split items into roughly equal batches for processing"""
    if not items:
        return []
    
    batch_size = max(1, len(items) // num_processes)
    batches = []
    
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        if batch:
            batches.append(batch)
    
    # If we have more batches than processes, merge the last batches
    while len(batches) > num_processes and len(batches) > 1:
        last_batch = batches.pop()
        batches[-1].extend(last_batch)
    
    return batches


def run_multiprocess_scraper(scraper_class: Type[SavageScraper],
                           items_to_scrape: List[Dict],
                           num_processes: int = 1,
                           config_dir: Optional[Path] = None,
                           output_dir: Optional[Path] = None,
                           logs_dir: Optional[Path] = None,
                           is_headless: bool = True,
                           translation: bool = False):
    """Main function to run multiprocess scraping with real-time output"""
    
    if not items_to_scrape:
        print("No items to scrape")
        return
    
    # Setup directories
    config_dir = Path(config_dir) if config_dir else Path("./config")
    output_dir = Path(output_dir) if output_dir else Path("./results")
    logs_dir = Path(logs_dir) if logs_dir else Path("./logs")
    
    # Create a temporary scraper instance to get configuration and handle resume
    temp_scraper = scraper_class(
        config_dir=config_dir,
        output_dir=output_dir,
        logs_dir=logs_dir,
        is_headless=is_headless,
        translation=translation,
        process_id=0
    )
    
    # Filter items for resume functionality
    items_to_scrape = temp_scraper._filter_items_for_resume(items_to_scrape)
    
    if not items_to_scrape:
        print("No new items to process after resume filtering. Exiting.")
        return
    
    # Setup output manager
    output_file = temp_scraper._get_output_file_path()
    output_manager = OutputManager(output_file)
    
    # Setup logging
    log_file = logs_dir / f"{scraper_class.__name__.lower()}_logs.log"
    log_queue = mp.Queue()
    logger_process = LoggerProcess(log_queue, log_file)
    
    # Setup queues
    progress_queue = mp.Queue()
    output_queue = mp.Queue()
    
    # Start logger process
    logger_proc = mp.Process(target=logger_process.run)
    logger_proc.start()
    
    # Start output writer process
    output_writer = OutputWriterProcess(output_queue, output_manager, log_queue)
    output_writer_proc = mp.Process(target=output_writer.run)
    output_writer_proc.start()
    
    # Setup main logger
    main_logger = logging.getLogger('SavageScraper')
    main_logger.addHandler(QueueHandler(log_queue))
    main_logger.setLevel(logging.INFO)
    
    progress_tracker = ProgressTracker(len(items_to_scrape), main_logger)
    
    try:
        main_logger.info(f"Starting multiprocess scraping with {num_processes} processes")
        main_logger.info(f"Total items to process: {len(items_to_scrape)}")
        
        # Split items into batches
        batches = split_items_into_batches(items_to_scrape, num_processes)
        main_logger.info(f"Split into {len(batches)} batches")
        
        # Start worker processes
        processes = []
        for i, batch in enumerate(batches):
            scraper_instance = scraper_class(
                config_dir=config_dir,
                output_dir=output_dir,
                logs_dir=logs_dir,
                is_headless=is_headless,
                translation=translation,
                process_id=i + 1
            )
            
            process = mp.Process(
                target=scraper_instance.process_batch,
                args=(batch, log_queue, output_queue, progress_queue)
            )
            processes.append(process)
            process.start()
            main_logger.info(f"Started process {i + 1} with {len(batch)} items")
        
        # Monitor progress
        completed_processes = 0
        while completed_processes < len(processes):
            try:
                # Check for progress updates
                try:
                    progress_update = progress_queue.get(timeout=1.0)
                    if progress_update == 'DONE':
                        completed_processes += 1
                        main_logger.info(f"Process completed ({completed_processes}/{len(processes)})")
                        progress_tracker.update_process_completion()
                    elif isinstance(progress_update, int):
                        progress_tracker.update(progress_update)
                except queue.Empty:
                    pass
                
                # Check if any processes have died
                for i, process in enumerate(processes):
                    if not process.is_alive() and process.exitcode is not None:
                        if process.exitcode != 0:
                            main_logger.error(f"Process {i + 1} died with exit code {process.exitcode}")
                
            except KeyboardInterrupt:
                main_logger.warning("Received interrupt signal, shutting down processes...")
                break
        
        # Wait for all processes to complete
        for i, process in enumerate(processes):
            process.join(timeout=30)
            if process.is_alive():
                main_logger.warning(f"Force terminating process {i + 1}")
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    main_logger.error(f"Force killing process {i + 1}")
                    process.kill()
        
        main_logger.info("All processes completed")
        progress_tracker._log_progress()  # Final progress log
        
    except Exception as e:
        main_logger.error(f"Error in multiprocess scraping: {e}")
    finally:
        # Cleanup output writer
        output_writer.stop()
        output_queue.put(None)  # Signal output writer to stop
        output_writer_proc.join(timeout=5)
        if output_writer_proc.is_alive():
            output_writer_proc.terminate()
        
        # Cleanup logger
        logger_process.stop()
        log_queue.put(None)  # Signal logger to stop
        logger_proc.join(timeout=5)
        if logger_proc.is_alive():
            logger_proc.terminate()
        
        main_logger.info("Scraping completed")


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, initiating graceful shutdown...")
        # This will be caught by the main process
        raise KeyboardInterrupt()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)