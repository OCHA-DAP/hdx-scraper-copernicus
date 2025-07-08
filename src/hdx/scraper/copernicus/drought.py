#!/usr/bin/python
"""copernicus scraper"""

import logging
from datetime import datetime, timedelta
from json import loads
from typing import List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from geopandas import GeoDataFrame
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve

from hdx.scraper.copernicus.utilities import get_lines

logger = logging.getLogger(__name__)


class Drought:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        global_boundaries: GeoDataFrame,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_folder = retriever.temp_dir
        self.global_boundaries = {}
        self.global_data = {}
        self.downloaded_data = {}
        self.country_data = {}
        self.dates = {}
        layer = loads(global_boundaries.to_json())["features"]
        for row in layer:
            iso = row["properties"]["ISO_3"]
            self.global_boundaries[iso] = [row["geometry"]]

    def get_data(self, download_country: bool) -> bool:
        file_patterns = self._configuration["file_patterns"]
        for data_type, file_pattern in file_patterns.items():
            file_type = self._configuration["file_types"][data_type]
            dataset_files = _get_dataset_files(
                self._configuration["dataset_info"][data_type]["name"]
            )
            base_url = f"{self._configuration['base_url']}{file_pattern}/"
            lines = get_lines(self._retriever, base_url, f"drought_{data_type}_ftp.txt")
            subfolders = []
            for line in lines:
                subfolder = line.get("href")
                if "ver" not in subfolder:
                    continue
                subfolders.append(subfolder)
            subfolder = subfolders[-1]
            sub_lines = get_lines(
                self._retriever,
                f"{base_url}{subfolder}",
                filename=f"drought_{data_type}_{subfolder.replace('/', '')}.txt",
            )
            for sub_line in sub_lines:
                subsubfolder = sub_line.get("href")
                if not subsubfolder.endswith(".zip"):
                    continue
                start_date, end_date = _parse_date(subsubfolder)
                dict_of_lists_add(self.dates, data_type, start_date)
                dict_of_lists_add(self.dates, data_type, end_date)
                if subsubfolder in dataset_files:
                    continue
                zip_url = f"{base_url}{subfolder}{subsubfolder}"
                dict_of_lists_add(self.global_data, data_type, zip_url)
                if file_type == "GeoJSON" or download_country:
                    file_path = self._retriever.download_file(zip_url)
                    dict_of_lists_add(self.downloaded_data, data_type, file_path)

        if len(self.global_data) == 0:
            return False
        return True

    def generate_global_dataset(self, data_type: str) -> Optional[Dataset]:
        dataset_info = self._configuration["dataset_info"][data_type]
        dataset = Dataset(
            {
                "name": dataset_info["name"],
                "title": dataset_info["title"],
                "notes": dataset_info["notes"],
                "methodology": "Other",
                "methodology_other": dataset_info["methodology_other"],
                "caveats": dataset_info["caveats"],
            }
        )
        dataset.set_expected_update_frequency(dataset_info["data_update_frequency"])
        time_period = self.dates[data_type]
        dataset.set_time_period(min(time_period), _parse_dekad(max(time_period)))
        dataset_tags = self._configuration["tags"]
        dataset.add_tags(dataset_tags)
        dataset.add_other_location("world")

        file_type = self._configuration["file_types"][data_type]
        if file_type == "GeoJSON":
            for file_path in self.downloaded_data[data_type]:
                start_date, end_date = _parse_date(basename(file_path))
                end_date = _parse_dekad(end_date)
                resource = Resource(
                    {
                        "name": basename(file_path),
                        "description": f"Data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                    }
                )
                resource.set_format(file_type)
                resource.set_file_to_upload(file_path)
                dataset.add_update_resource(resource)
        elif file_type == "GeoTIFF":
            for file_url in self.global_data[data_type]:
                start_date, end_date = _parse_date(basename(file_url))
                end_date = _parse_dekad(end_date)
                resource = Resource(
                    {
                        "name": basename(file_url),
                        "description": f"Data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                        "url": file_url,
                        "format": "GeoTIFF",
                    }
                )
                dataset.add_update_resource(resource)

        return dataset


def _get_dataset_files(dataset_name: str) -> List:
    dataset = Dataset.read_from_hdx(dataset_name)
    if not dataset:
        return []
    resource_names = [resource["name"] for resource in dataset.get_resources()]
    return resource_names


def _parse_date(file_name: str) -> Tuple:
    file_name = file_name.split("_")
    if len(file_name) == 1:
        file_name = file_name[0].split("-")
    start_date = file_name[-3]
    end_date = file_name[-2]
    start_date = parse_date(start_date, date_format="%Y%m%d")
    end_date = parse_date(end_date, date_format="%Y%m%d")
    return start_date, end_date


def _parse_dekad(date: datetime) -> datetime:
    day = date.day
    if day in [1, 11]:
        end_date = date + timedelta(days=9)
        return end_date
    end_date = date + relativedelta(day=31)
    return end_date
