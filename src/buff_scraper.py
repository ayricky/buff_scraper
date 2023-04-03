import argparse
import csv
import sqlite3
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import asyncio
import aiohttp

import yaml
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table
from rich.text import Text
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from aiohttp_socks import ProxyConnector

class MaxRetriesExceeded(Exception):
    pass

class WebDriverNotInitialized(Exception):
    pass

def initialize_csv():
    with open('output.csv', mode='w', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        writer.writerow([
            'item_id',
            'raw_name',
            'name',
            'drop_down_index',
            'option_index',
            'button_text',
            'option_text',
            'option_value',
            'additional_options'
        ])

class Worker:
    def __init__(self, proxy):
        self.proxy = proxy
        self.webdriver = self.init_webdriver(proxy)
        self.performance_score = 0
        self.is_working = False
        self.current_task_id = None

    def init_webdriver(self, proxy):
        options = webdriver.ChromeOptions()
        options.add_argument(f'user-agent={UserAgent().random}')
        proxy_config = f'{proxy["proxy_type"].lower()}://{proxy["proxy"]}'
        options.add_argument(f'--proxy-server={proxy_config}')
        options.add_argument("--log-level=3")
        options.add_argument("--headless")

        # Add the following line to suppress DevTools messages
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        service = Service(executable_path="C:\\Users\\Ricky\\lab\\chromedriver_win32\\chromedriver.exe")
        driver = webdriver.Chrome(service=service, options=options)

        return driver

    def scrape_data(self, task):
        if self.webdriver is None:
            raise WebDriverNotInitialized("Webdriver not initialized")
        self.current_task_id = task['id']
        self.is_working = True
        url = f"https://buff.163.com/goods/{task['id']}"
        self.webdriver.get(url)

        WebDriverWait(self.webdriver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".w-Select-Multi"))
        )

        html = self.webdriver.page_source
        soup = BeautifulSoup(html, "html.parser")
        dropdown_containers = soup.select(".w-Select-Multi")
        return_data = []
        try:
            for i, dropdown_container in enumerate(dropdown_containers):
                button_text = dropdown_container.select_one("h3").text
                options = dropdown_container.select("h6")

                for j, option in enumerate(options):
                    option_value = option["value"]
                    option_text = option.text
                    additional_options = []
                    for p in option.find_next_siblings('div'):
                        for value in p.select('p'):
                            additional_options.append(value.text)

                    return_data.append({
                        "item_id": task['id'],
                        "name": task['name'],
                        "raw_name": task['raw_name'],
                        "drop_down_index": i,
                        "option_index": j,
                        "button_text": button_text,
                        "option_text": option_text,
                        "option_value": option_value,
                        "additional_options": additional_options
                    })

            self.performance_score += 1  # Increase the performance score for a successful scrape
            time.sleep(2)

        finally:
            self.is_working = False
            self.current_task_id = None
            return return_data

    def scrape_data_with_timeout(self, task):
        try:
            retry = 0
            while retry < 3:
                try:
                    result = self.scrape_data(task)
                    break
                except WebDriverException as e:
                    retry += 1
                    time.sleep(3)
                except TimeoutException:
                    raise
            else:
                raise MaxRetriesExceeded("Maximum retries exceeded for the task")
        except MaxRetriesExceeded:
            raise
        except Exception:
            raise

        return result

class Scraper:
    def __init__(self, tasks, proxies, num_workers):
        self.queue = Queue()
        self.csv_write_lock = threading.Lock()

        self.workers = []

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.working_proxies = loop.run_until_complete(self.get_working_proxies(proxies))

        # Initialize progress bar
        with Progress() as progress:
            create_worker_task_id = progress.add_task("[cyan]Initializing web drivers...", total=num_workers)

            for _ in range(num_workers):
                if self.working_proxies:
                    proxy_info = self.working_proxies.pop(0)
                    worker = Worker(proxy_info)
                    if worker.webdriver is not None:
                        self.workers.append(worker)
                progress.advance(create_worker_task_id)  # Update the progress bar
                if not self.working_proxies:
                    progress.update(create_worker_task_id, completed=num_workers)  # Complete the progress bar
                    break

            for task in tasks:
                self.queue.put(task)  # Add the task to the queue

            for _ in range(len(self.workers)):  # Add sentinel values for each worker
                self.queue.put(None)

    async def test_proxy(self, proxy_info, progress, task_id):
        url = "https://buff.163.com/"
        timeout = aiohttp.ClientTimeout(total=15)

        proxy = proxy_info['proxy']
        proxy_type = proxy_info['proxy_type']
        connector = ProxyConnector.from_url(f"{proxy_type}://{proxy}")

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            try:
                async with session.get(url) as response:
                    return response.status == 200
            except Exception:
                return False
            finally:
                await connector.close()
                progress.advance(task_id)

    async def get_working_proxies(self, proxies):
        with Progress() as progress:
            test_proxy_task_id = progress.add_task("[cyan]Testing proxies...", total=len(proxies))
            tasks = [asyncio.create_task(self.test_proxy(proxy_info, progress, test_proxy_task_id)) for proxy_info in proxies]
            results = await asyncio.gather(*tasks)

        working_proxies = [proxy_info for proxy_info, is_working in zip(proxies, results) if is_working]
        return working_proxies
    
    def start_scraping(self, dashboard, terminate_event):
        def worker_scrape(worker):
            while True:
                item = self.queue.get()
                if item is None:
                    break

                try:
                    with ThreadPoolExecutor(max_workers=1) as task_executor:
                        future = task_executor.submit(worker.scrape_data_with_timeout, item)
                        scraped_data = future.result(timeout=300)  # Set the timeout value here

                        if scraped_data is not None:
                            with self.csv_write_lock:
                                with open('output.csv', mode='a', newline='', encoding='utf-8') as output_file:
                                    writer = csv.writer(output_file)
                                    for data in scraped_data:
                                        writer.writerow([
                                            data['item_id'],
                                            data['name'],
                                            data['raw_name'],
                                            data['drop_down_index'],
                                            data['option_index'],
                                            data['button_text'],
                                            data['option_text'],
                                            data['option_value'],
                                            data['additional_options']
                                        ])

                except TimeoutError:
                    self.queue.put(item)

                except (WebDriverNotInitialized, Exception) as e:
                    print(f"Error occurred: {e}\nTraceback:\n{traceback.format_exc()}")
                    self.queue.put(item)  # Re-queue the item
                    if worker.webdriver is not None:
                        worker.webdriver.quit()  # Close the old webdriver
                    scraped_data = None

                    # Create a new worker
                    new_worker = None
                    while not new_worker:
                        try:
                            new_proxy = self.working_proxies.pop(0)  # Get a new proxy from the list
                            temp_worker = Worker(new_proxy)
                            if temp_worker.webdriver is not None:  # Check if the worker.webdriver is not None
                                new_worker = temp_worker
                                self.workers.append(new_worker)
                                self.workers.remove(worker)
                                worker = new_worker
                            else:
                                self.working_proxies = self.working_proxies[1:]  # Remove the proxy from the list if it doesn't work
                        except IndexError:
                            # No more proxies left
                            break
                        except Exception as err:
                            # Failed to create a new worker, remove the proxy from the list
                            self.working_proxies = self.working_proxies[1:]
                            print(f"Error occurred while creating a new worker: {err}")
                finally:
                    dashboard.update_progress(dashboard.completed_tasks + 1)

        with ThreadPoolExecutor(max_workers=len(self.workers)) as executor:
            tasks = [executor.submit(worker_scrape, worker) for worker in self.workers]
            for future in as_completed(tasks):
                future.result()

            terminate_event.set() 

    def close_all_drivers(self):
        with ThreadPoolExecutor(max_workers=len(self.workers)) as executor:
            executor.map(lambda worker: worker.webdriver.quit() if worker.webdriver else None, self.workers)


class Dashboard:
    def __init__(self, workers, total_tasks):
        self.workers = workers
        self.total_tasks = total_tasks
        self.completed_tasks = 0
        self.console = Console()
        self.table = Table("Worker", "Status", "Task ID", "Performance Score")
        self.start_time = time.time()
        self.elapsed_time = 0
        self.estimated_time_remaining = None

    def update_table(self):
        self.elapsed_time = time.time() - self.start_time
        if self.completed_tasks > 0:
            time_per_task = self.elapsed_time / self.completed_tasks
            remaining_tasks = self.total_tasks - self.completed_tasks
            self.estimated_time_remaining = remaining_tasks * time_per_task
        else:
            self.estimated_time_remaining = None

        progress = f"{self.completed_tasks}/{self.total_tasks}"
        tasks_in_queue = self.total_tasks - self.completed_tasks

        elapsed_time_text = Text(f"Time Elapsed: {time.strftime('%H:%M:%S', time.gmtime(self.elapsed_time))}", style="bold yellow")
        if self.estimated_time_remaining:
            eta_text = Text(f"ETA: {time.strftime('%H:%M:%S', time.gmtime(self.estimated_time_remaining))}", style="bold yellow")
        else:
            eta_text = Text("ETA: N/A", style="bold yellow")

        progress_text = Text(f"Progress: {progress}", style="bold green")
        tasks_in_queue_text = Text(f"Tasks in Queue: {tasks_in_queue}", style="bold red")

        header_columns = Columns([elapsed_time_text, eta_text, progress_text, tasks_in_queue_text])

        self.table = Table("Worker", "Status", "Task ID", "Performance Score", title=header_columns)
        self.table.style = "bold blue"  # Set the table style
        for i, worker in enumerate(self.workers):
            status = "Working" if worker.is_working else "Idle"
            task_id = str(worker.current_task_id) if worker.current_task_id else "N/A"
            self.table.add_row(str(i + 1), status, task_id, str(worker.performance_score))

    def update_progress(self, completed_tasks):
        self.completed_tasks = completed_tasks

    def render(self, terminate_event):
        with Live(self.table, console=self.console, refresh_per_second=1) as live:
            while not terminate_event.is_set():
                self.update_table()
                live.update(Panel(self.table))
                time.sleep(1)

def main():
    parser = argparse.ArgumentParser(description="Web scraper with multiple workers.")
    parser.add_argument('-n', '--num_workers', type=int, default=5, help="Number of workers to use for scraping.")
    args = parser.parse_args()

    with open('working_proxies.yaml') as proxies_file:
        proxies = yaml.safe_load(proxies_file)

    conn = sqlite3.connect("csgo_items.db")
    cur = conn.cursor()
    query = "SELECT buff_id, raw_name, name FROM (SELECT buff_id, raw_name, name, ROW_NUMBER() OVER (PARTITION BY name ORDER BY RANDOM()) AS rn FROM items WHERE is_stattrak = 0 AND is_souvenir = 0 AND item_type IN ('Skin', 'Knife', 'Gloves')) WHERE rn = 1"
    cur.execute(query)
    items = cur.fetchall()
    initialize_csv()
    tasks = [
        {'id': item[0], 'raw_name': item[1], 'name': item[2]}
        for item in items
    ]

    num_workers = args.num_workers

    terminate_event = threading.Event()

    scraper = Scraper(tasks, proxies, num_workers)
    dashboard = Dashboard(scraper.workers, len(tasks))
    thread_scraper = threading.Thread(target=scraper.start_scraping, args=(dashboard, terminate_event))
    thread_scraper.start()
    dashboard.render(terminate_event)
    thread_scraper.join()

    # Update and print the dashboard one final time after finishing
    dashboard.update_table()
    dashboard.console.print(Panel(dashboard.table))

    # Close all drivers
    scraper.close_all_drivers()
    conn.close()

if __name__ == '__main__':
    main()