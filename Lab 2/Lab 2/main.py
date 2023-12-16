import requests
import os
import zipfile
from pathlib import Path
from urllib.parse import urlparse

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_and_extract(url, download_dir):
    # Визначення назви файлу з URL
    filename = os.path.basename(urlparse(url).path)
    zip_path = download_dir / filename

    # Завантаження файлу
    response = requests.get(url)
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    # Розпакування ZIP-файлу і видалення його
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(download_dir)
    os.remove(zip_path)

def main():
    # Створення каталогу downloads, якщо він не існує
    download_dir = Path('downloads')
    download_dir.mkdir(parents=True, exist_ok=True)

    # Завантаження файлів
    for url in download_uris:
        download_and_extract(url, download_dir)

# Запуск програми
main()
