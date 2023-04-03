# README.md

## buff_scraper

`buff_scraper` is a Python-based web scraping tool designed specifically to extract button data from BUFF163 items. The tool gathers information such as button names, descriptions, and prices, and stores it in a structured format (CSV) for further analysis or usage.

### Features

- Designed for BUFF163 item data extraction
- Extracts button names, descriptions, and prices
- Stores scraped data in CSV format

### Prerequisites

- Python 3.6 or higher
- Pip (Python package manager)

### Installation

Clone the repository:

git clone https://github.com/ayricky/buff_scraper.git

Install the required packages:

cd buff_scraper
pip install -r requirements.txt


### Usage

python src/buff_scraper.py -n 5
Use the `-n` option to specify the number of proxied headless Chrome drivers.

### Customization

You can customize the scraper by modifying the `buff_scraper.py` file to add support for additional websites or to extract additional information. Be sure to comply with each website's terms of service and robots.txt file to avoid any legal issues.

### License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

### Disclaimer

This tool is for educational purposes only. The developer is not responsible for any misuse or any harm caused by using this tool. Always respect website owners' terms of service and robots.txt files.
