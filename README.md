# SavageScraper

A robust, production-ready web scraping framework built with Python, featuring advanced multiprocessing capabilities, real-time progress tracking, comprehensive error handling, and automatic resume functionality.

## Overview

SavageScraper is designed to handle large-scale web scraping operations efficiently by distributing work across multiple processes. It provides a solid foundation for building custom scrapers with built-in support for:

- Parallel processing with configurable worker processes
- Real-time output writing to prevent data loss
- Automatic resume capability for interrupted scraping sessions
- Comprehensive logging with process-level tracking
- Selenium-based browser automation with anti-detection features
- Thread-safe file operations with proper locking mechanisms
- Progress tracking with ETA calculations

## Features

### Core Capabilities

**Multiprocessing Architecture**
- Distribute scraping tasks across multiple Chrome instances
- Configurable number of worker processes
- Automatic batch splitting for balanced workload distribution
- Independent process logging and error handling

**Real-Time Data Management**
- Immediate output writing via dedicated writer process
- Thread-safe file operations with fcntl locking
- Automatic CSV header management
- Backup file creation on write failures

**Resume Functionality**
- Automatically detects previously processed items
- Skips completed work to save time and resources
- Configurable resume key for flexible tracking
- Seamless continuation after interruptions

**Robust Error Handling**
- Per-item error tracking without stopping entire process
- Automatic error page detection and handling
- HTML snapshot saving for debugging failed pages
- Graceful shutdown with signal handlers

**Advanced Logging**
- Centralized logging across all processes
- Process-level log identification
- Simultaneous file and console output
- Configurable log levels and formatting

**Browser Automation**
- Selenium WebDriver with Chrome
- Headless and headed modes
- Random user agent rotation
- Anti-detection measures
- Automatic translation support
- Configurable timeouts and waits

## Installation

### Requirements

```
Python 3.7+
```

### Dependencies

```bash
pip install selenium pandas
```

### Chrome WebDriver

Ensure ChromeDriver is installed and accessible in your system PATH. The version must match your installed Chrome browser.

```bash
# Example for Ubuntu/Debian
sudo apt-get install chromium-chromedriver

# Or download from:
# https://chromedriver.chromium.org/downloads
```

## Usage

### Basic Implementation

Create a custom scraper by subclassing `SavageScraper` and implementing the required abstract methods:

```python
from savage_scraper import SavageScraper, run_multiprocess_scraper
from pathlib import Path

class MyCustomScraper(SavageScraper):
    
    def _get_progress_tracking_key(self) -> str:
        return "url"
    
    def _get_output_file_path(self) -> Path:
        return self.output_dir / "results.csv"
    
    def _get_required_selectors(self) -> list:
        return ["page_ready", "categories"]
    
    def _get_page_ready_selector(self) -> str:
        return "page_ready"
    
    def _get_categories_selector(self) -> str:
        return "categories"
    
    def _process_category_element(self, element, item):
        return {
            "source_url": item["url"],
            "category_name": self.get_clean_text(element),
            "timestamp": datetime.now().isoformat()
        }
    
    def _create_empty_result(self, item):
        return {
            "source_url": item["url"],
            "category_name": "",
            "timestamp": datetime.now().isoformat()
        }
    
    def _get_resume_key(self) -> str:
        return "source_url"

# Run the scraper
items = [
    {"url": "https://example.com/page1"},
    {"url": "https://example.com/page2"}
]

run_multiprocess_scraper(
    scraper_class=MyCustomScraper,
    items_to_scrape=items,
    num_processes=4,
    is_headless=True,
    translation=False
)
```

### Configuration

Create a `config.json` file in your config directory:

```json
{
    "KEY": "example",
    "COUNTRY": "US",
    "BASE_URL": "https://example.com",
    "SELECTORS": {
        "page_ready": [
            ".content-loaded",
            "//div[@id='main']"
        ],
        "categories": [
            ".category-item",
            "//div[@class='category']"
        ],
        "error_page_indicator": [
            ".error-message",
            "//div[@class='captcha']"
        ],
        "error_page_handler": [
            "#verify-button",
            "//button[@type='submit']"
        ]
    }
}
```

### Directory Structure

```
project/
├── config/
│   └── config.json
├── results/
│   └── output.csv
├── logs/
│   └── scraper_logs.log
├── error_pages/
│   └── error_*.html
└── your_scraper.py
```

## Abstract Methods

Your custom scraper must implement these methods:

### _get_progress_tracking_key()
Returns the dictionary key used to track items being processed.

```python
def _get_progress_tracking_key(self) -> str:
    return "url"  # or "id", "item_key", etc.
```

### _get_output_file_path()
Returns the path where results should be saved.

```python
def _get_output_file_path(self) -> Path:
    return self.output_dir / "scraped_data.csv"
```

### _get_required_selectors()
Returns list of selector keys that must be in config.

```python
def _get_required_selectors(self) -> list:
    return ["page_ready", "categories", "details"]
```

### _get_page_ready_selector()
Returns selector key for page load confirmation.

```python
def _get_page_ready_selector(self) -> str:
    return "page_ready"
```

### _get_categories_selector()
Returns selector key for main content elements.

```python
def _get_categories_selector(self) -> str:
    return "categories"
```

### _process_category_element(element, item)
Processes a single element and returns result dictionary.

```python
def _process_category_element(self, element, item):
    return {
        "id": item["id"],
        "name": self.get_clean_text(element),
        "extracted_at": datetime.now().isoformat()
    }
```

### _create_empty_result(item)
Creates empty result when no elements found.

```python
def _create_empty_result(self, item):
    return {
        "id": item["id"],
        "name": None,
        "extracted_at": datetime.now().isoformat()
    }
```

### _get_resume_key()
Returns the output column used for resume detection.

```python
def _get_resume_key(self) -> str:
    return "id"  # Must match output CSV column
```

## Advanced Features

### Custom Element Finding

Use the built-in `_find_elements()` method for robust element location:

```python
# Find elements using configured selectors
elements = self._find_elements("my_selector_key")

# Find within a container
container = self._find_elements("container")[0]
nested = self._find_elements("nested_selector", container=container)
```

### Text Extraction

Use `get_clean_text()` for reliable text extraction:

```python
element = self._find_elements("selector")[0]
text = self.get_clean_text(element)  # Handles innerText, .text, innerHTML
```

### Navigation

Navigate to URLs with automatic waiting:

```python
if self._navigate_to_url("https://example.com/page"):
    # Page loaded successfully
    pass
```

### Error Page Handling

Configure error detection and recovery:

```json
{
    "SELECTORS": {
        "error_page_indicator": [".captcha", ".error"],
        "error_page_handler": ["#verify-button"]
    }
}
```

## Configuration Options

### Scraper Initialization

```python
scraper = MyCustomScraper(
    config_dir="./config",      # Configuration directory
    output_dir="./results",     # Output directory
    logs_dir="./logs",          # Logs directory
    error_pages_dir="./errors", # Error page snapshots
    is_headless=True,           # Headless Chrome mode
    translation=False,          # Auto-translate pages
    process_id=1                # Process identifier
)
```

### Multiprocess Runner

```python
run_multiprocess_scraper(
    scraper_class=MyCustomScraper,
    items_to_scrape=items_list,
    num_processes=4,            # Number of parallel processes
    config_dir=Path("./config"),
    output_dir=Path("./results"),
    logs_dir=Path("./logs"),
    is_headless=True,
    translation=False
)
```

## Performance Considerations

### Process Count
- More processes = faster scraping but higher resource usage
- Recommended: 2-8 processes depending on system resources
- Each process runs a separate Chrome instance

### Memory Usage
- Each Chrome instance uses approximately 200-500 MB
- Monitor system resources when scaling processes
- Consider batch size when working with large datasets

### Rate Limiting
- Implement delays in your custom scraper if needed
- Respect target website's robots.txt
- Consider using proxy rotation for large-scale operations

## Logging

Logs include:
- Process-level identification
- Timestamps for all operations
- Success/failure for each item
- Progress updates with ETA
- Error details with stack traces
- Final statistics

Example log output:
```
2025-01-15 10:30:45 - P1 - MyCustomScraper - INFO - Starting batch processing
2025-01-15 10:30:46 - P1 - MyCustomScraper - INFO - [1/50] Processing item
2025-01-15 10:30:47 - P1 - MyCustomScraper - INFO - Successfully extracted 5 categories
2025-01-15 10:30:50 - SavageScraper - INFO - Input Progress: 150/500 (30.0%) | Output Items: 750 | Failed: 2 | ETA: 0:15:30
```

## Error Handling

### Automatic Recovery
- Failed items are marked but don't stop processing
- Error pages are saved for debugging
- Process crashes are detected and logged

### Manual Recovery
- Use resume functionality to retry failed batches
- Review error page HTML snapshots
- Adjust selectors based on error patterns

## Best Practices

1. **Start Small**: Test with a small dataset before scaling
2. **Monitor Resources**: Watch CPU, memory, and network usage
3. **Configure Timeouts**: Adjust based on target site performance
4. **Use Selectors Wisely**: Provide multiple fallback selectors
5. **Implement Delays**: Add random delays if scraping rate-limited sites
6. **Handle Edge Cases**: Create robust `_process_category_element()` implementation
7. **Regular Backups**: Output is written in real-time, but verify data integrity
8. **Respect Robots.txt**: Check and follow website scraping policies

## Troubleshooting

### Chrome Driver Issues
- Ensure ChromeDriver version matches Chrome browser
- Check PATH environment variable
- Verify executable permissions

### Memory Problems
- Reduce number of processes
- Enable headless mode
- Clear Chrome cache between runs

### Selector Issues
- Test selectors in browser DevTools
- Provide multiple fallback selectors
- Check for dynamic content loading
- Review saved error pages

### Resume Not Working
- Verify resume key matches output CSV column
- Check CSV encoding (UTF-8)
- Ensure output file is not corrupted

## License

Created by Ammar HADDAD (ammarhaddad@outlook.fr)

## Support

For issues, questions, or contributions, please contact the author or refer to the source code documentation.

## Changelog

### Version 1.0
- Initial release with multiprocessing support
- Real-time output writing
- Resume functionality
- Comprehensive logging
- Error handling and recovery