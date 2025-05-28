#!/usr/bin/python
"""copernicus scraper"""

import logging
import re
from json import loads
from os.path import join
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile

import rasterio
from geopandas import GeoDataFrame, overlay
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_dicts_add, dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from rasterio.mask import mask
from rasterio.merge import merge
from requests import head
from slugify import slugify

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
        self.global_boundaries = global_boundaries
        self.global_data = {}
        self.downloaded_data = {}
        self.country_data = {}
        self.dates = {}

    def get_data(self, download_country: bool) -> bool:
        file_patterns = self._configuration["file_patterns"]
        for data_type, file_pattern in file_patterns.items():
            dataset_files = _get_dataset_files(file_pattern)
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
                if download_country:
                    file_path = self._retriever.download_file(zip_url)
                    dict_of_lists_add(self.downloaded_data, data_type, file_path)

        if len(self.global_data) == 0:
            return False
        return True

    def generate_global_dataset(self, data_type: str) -> Optional[Dataset]:
        dataset_name = "global-human-settlement-layer-ghsl"
        dataset_title = "Copernicus Global Human Settlement Layer (GHSL)"

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        time_period = [value for _, value in self.data_year.items()]
        dataset.set_time_period_year_range(min(time_period), max(time_period))
        dataset_tags = self._configuration["tags"]
        dataset.add_tags(dataset_tags)
        dataset.add_other_location("world")
        dataset["customviz"] = [
            {
                "url": "https://human-settlement.emergency.copernicus.eu/visualisation.php#lnlt=@50.93074,12.87598,5z&v=301&ln=0&gr=ds&lv=10000000000000000000000000000000000000011111&lo=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&pg=V"
            }
        ]

        resource_info = self._configuration["resource_info"]
        for data_type, file_url in self.global_data.items():
            file_size = head(file_url).headers.get("Content-Length")
            file_size = round(int(file_size) / (1024**3), 1)
            resource_desc = resource_info[data_type]["description"].replace(
                "YYYY", str(self.data_year[data_type])
            )
            resource = Resource(
                {
                    "name": resource_info[data_type]["name"],
                    "description": f"{resource_desc} ({file_size} GB)",
                    "url": file_url,
                    "format": "GeoTIFF",
                }
            )
            dataset.add_update_resource(resource)

        return dataset


def _get_dataset_files(data_type: str) -> List:
    dataset = Dataset.read_from_hdx(slugify(f"global-{data_type}"))
    if not dataset:
        return []
    resource_names = [resource["name"] for resource in dataset.get_resources()]
    return resource_names


def _parse_date(file_name: str) -> Tuple:
    file_name = file_name.split("_")
    start_date = file_name[-3]
    end_date = file_name[-2]
    start_date = parse_date(start_date)
    end_date = parse_date(end_date)
    return start_date, end_date
