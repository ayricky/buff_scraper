import argparse
import csv
import sqlite3
import threading
import time
import traceback
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, PriorityQueue
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
import itertools

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
            'wear',
            'skin_line',
            'drop_down_index',
            'option_index',
            'button_text',
            'option_text',
            'option_value',
            'additional_options'
        ])

class Worker:
    worker_id_counter = itertools.count()  # Add a counter for unique worker IDs

    def __init__(self, proxy):
        self.worker_id = next(self.worker_id_counter)  # Assign a unique ID to each worker
        self.proxy = proxy
        self.webdriver = self.init_webdriver(proxy)
        self.performance_score = 0
        self.is_working = False
        self.current_task_id = None

    def __lt__(self, other):
        return self.performance_score < other.performance_score

    def __eq__(self, other):
        return self.performance_score == other.performance_score

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
                        "wear": task['wear'],
                        "skin_line": task['skin_line'],
                        "drop_down_index": i,
                        "option_index": j,
                        "button_text": button_text,
                        "option_text": option_text,
                        "option_value": option_value,
                        "additional_options": additional_options
                    })

            time.sleep(2)

        finally:
            self.is_working = False
            self.current_task_id = None
            return return_data

    def scrape_data_with_timeout(self, task, timeout=20):  # Add a timeout parameter
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.scrape_data, task)
            try:
                return future.result(timeout=timeout)  # Add the timeout here
            except concurrent.futures.TimeoutError:
                future.cancel()  # Cancel the task if it times out
                raise

class Scraper:
    def __init__(self, tasks, proxies, num_workers):
        self.task_queue = Queue()
        self.worker_queue = PriorityQueue()
        self.csv_write_lock = threading.Lock()
        self.task_queue_lock = threading.Lock()
        self.all_tasks_done = threading.Condition(self.task_queue_lock)
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
                        self.worker_queue.put((-worker.performance_score, worker))
                progress.advance(create_worker_task_id)  # Update the progress bar
                if not self.working_proxies:
                    progress.update(create_worker_task_id, completed=num_workers)  # Complete the progress bar
                    break

            for task in tasks:
                self.task_queue.put(task)  # Add the task to the task queue


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
        sentinel = object()  # Create a sentinel object to signal that there are no more tasks to process

        def worker_scrape(worker):
            nonlocal sentinel_count, sentinel_count_lock
            queue_timeout = 10

            while True:
                try:
                    task = self.task_queue.get(timeout=queue_timeout)
                except self.task_queue.Empty:
                    # If the worker didn't get a task within the timeout, check if all tasks are done
                    with sentinel_count_lock:
                        if sentinel_count == len(self.workers):
                            terminate_event.set()
                    continue

                if task is sentinel:
                    self.task_queue.put(task)  # Put the sentinel back into the task_queue for other workers
                    with sentinel_count_lock:
                        sentinel_count += 1
                        if sentinel_count == len(self.workers):
                            terminate_event.set()
                    break

                try:
                    scraped_data = worker.scrape_data_with_timeout(task)

                    if scraped_data is not None:
                        with self.csv_write_lock:
                            with open('output.csv', mode='a', newline='', encoding='utf-8') as output_file:
                                writer = csv.writer(output_file)
                                for data in scraped_data:
                                    writer.writerow([
                                        data['item_id'],
                                        data['name'],
                                        data['raw_name'],
                                        data['wear'],
                                        data['skin_line'],
                                        data['drop_down_index'],
                                        data['option_index'],
                                        data['button_text'],
                                        data['option_text'],
                                        data['option_value'],
                                        data['additional_options']
                                    ])

                        worker.performance_score += 1
                        dashboard.update_progress(dashboard.completed_tasks + 1)
                        self.task_queue.task_done()

                except concurrent.futures.TimeoutError:
                    worker.webdriver.quit()  # Quit the WebDriver instance associated with the worker
                    self.task_queue.put(task)
                    worker.is_working = False
                    self.worker_queue.put((-worker.performance_score, worker))

                except (WebDriverNotInitialized, Exception) as e:
                    print(f"Error occurred: {e}\nTraceback:\n{traceback.format_exc()}")
                    self.task_queue.put(task)
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
                            self.worker_queue.put((-worker.performance_score, worker))
                        except IndexError:
                            # No more proxies left
                            break
                        except Exception as err:
                            # Failed to create a new worker, remove the proxy from the list
                            self.working_proxies = self.working_proxies[1:]
                            print(f"Error occurred while creating a new worker: {err}")

        with ThreadPoolExecutor(max_workers=len(self.workers)) as executor:
            sentinel_count = 0
            sentinel_count_lock = threading.Lock()

            while not self.task_queue.empty() or sentinel_count < len(self.workers):
                _, worker = self.worker_queue.get()

                if not worker.is_working:
                    worker.is_working = True
                    executor.submit(worker_scrape, worker)

            self.task_queue.join()  # Wait for all tasks to be completed

            # Add sentinel values to the task_queue when there are no more tasks to process
            if sentinel_count < len(self.workers):
                self.task_queue.put(sentinel)

                # Wait for all the workers to encounter the sentinel value
                while sentinel_count < len(self.workers):
                    time.sleep(1)

            terminate_event.set()


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

        # Sort the workers by performance score
        sorted_workers = sorted(self.workers, key=lambda w: w.performance_score, reverse=True)

        self.table = Table("Worker ID", "Status", "Task ID", "Performance Score", title=header_columns)
        self.table.style = "bold blue"  # Set the table style

        # Display workers in the table based on their sorted performance score
        for worker in sorted_workers:
            status = "Working" if worker.is_working else "Idle"
            task_id = str(worker.current_task_id) if worker.current_task_id else "N/A"
            self.table.add_row(str(worker.worker_id), status, task_id, str(worker.performance_score))


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
    query = """SELECT buff_id, raw_name, name, wear, skin_line FROM (
                SELECT buff_id, raw_name, name, wear, skin_line,
                ROW_NUMBER() OVER (PARTITION BY name ORDER BY RANDOM()) AS rn
                FROM items
                WHERE is_stattrak = 0 AND is_souvenir = 0 AND item_type IN ('Skin', 'Knife', 'Gloves')
          ) WHERE rn = 1"""
    cur.execute(query)
    items = cur.fetchall()
    initialize_csv()
    tasks = [
        {'id': item[0], 'raw_name': item[1], 'name': item[2], 'wear': item[3], 'skin_line': item[4]}
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

    # Close all drivers with a progress bar
    scraper.close_all_drivers()
    conn.close()

if __name__ == '__main__':
    main()