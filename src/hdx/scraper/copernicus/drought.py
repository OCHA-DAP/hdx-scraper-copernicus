#!/usr/bin/python
"""copernicus scraper"""

import logging
from datetime import datetime, timedelta
from json import loads
from os import mkdir
from os.path import basename, join
from shutil import copy
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile

import rasterio
from dateutil.relativedelta import relativedelta
from geopandas import GeoDataFrame
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from rasterio.mask import mask

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
        updated = False
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
                zip_url = f"{base_url}{subfolder}{subsubfolder}"
                dict_of_lists_add(self.global_data, data_type, zip_url)
                if file_type == "GeoJSON":
                    file_path = self._retriever.download_file(
                        zip_url, filename=basename(zip_url)
                    )
                    dict_of_lists_add(self.downloaded_data, data_type, file_path)
                elif download_country and subsubfolder not in dataset_files:
                    file_path = self._retriever.download_file(
                        zip_url, filename=basename(zip_url)
                    )
                    dict_of_lists_add(self.downloaded_data, data_type, file_path)
            global_data = [basename(f) for f in self.global_data[data_type]]
            if sorted(global_data) != sorted(dataset_files):
                updated = True
        return updated

    def unzip_data(self, data_type: str) -> Dict:
        file_paths = {}
        file_type = self._configuration["file_types"][data_type]
        if file_type == "GeoJSON":
            return file_paths
        for zip_file_path in self.downloaded_data[data_type]:
            zip_folder = join(self._temp_folder, basename(zip_file_path)[:-4])
            mkdir(zip_folder)
            with ZipFile(zip_file_path, "r") as z:
                file_list = z.namelist()
                z.extractall(zip_folder)
            file_paths[zip_folder] = file_list
        return file_paths

    def process(self, iso3: str, data_type: str, file_paths: Dict) -> Dict | None:
        if len(file_paths) == 0:
            return None
        if iso3 == "ATA":
            return None
        logger.info(f"Processing {iso3}")
        country_name = Country.get_country_name_from_iso3(iso3)
        if not country_name:
            logger.error(f"Couldn't find country {iso3}, skipping")
            return None
        iso_geometry = self.global_boundaries[iso3]
        for folder, files in file_paths.items():
            country_folder = join(
                self._temp_folder, f"{iso3.lower()}-{basename(folder)}"
            )
            mkdir(country_folder)
            country_files = []
            for raster_name in files:
                raster_path = join(folder, raster_name)
                country_file = join(country_folder, raster_name)
                if not raster_name.endswith(".tif"):
                    copy(raster_path, country_folder)
                    country_files.append(country_file)
                    continue
                try:
                    with rasterio.open(raster_path, "r") as global_raster:
                        mask_raster, mask_transform = mask(
                            global_raster, iso_geometry, all_touched=True, crop=True
                        )
                        mask_meta = global_raster.meta.copy()
                except ValueError:
                    continue
                mask_meta.update(
                    {
                        "height": mask_raster.shape[1],
                        "width": mask_raster.shape[2],
                        "transform": mask_transform,
                    }
                )
                with rasterio.open(
                    country_file, "w", **mask_meta, compress="LZW"
                ) as dest:
                    dest.write(mask_raster)
                country_files.append(country_file)
            tifs = [f for f in country_files if f.endswith(".tif")]
            if len(tifs) == 0:
                logger.info(f"No data for {iso3}, skipping")
                return None
            country_zip = join(
                self._temp_folder, f"{iso3.lower()}-{basename(folder)}.zip"
            )
            with ZipFile(country_zip, "w") as z:
                for country_file in country_files:
                    z.write(
                        country_file,
                        join(
                            f"{iso3.lower()}-{basename(folder)}", basename(country_file)
                        ),
                    )
            dict_of_lists_add(self.country_data, iso3, country_zip)
        return self.country_data[iso3]

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

    def generate_dataset(self, iso3: str, data_type: str) -> Optional[Dataset]:
        country_name = Country.get_country_name_from_iso3(iso3)
        dataset_info = self._configuration["dataset_info"][data_type]
        dataset = Dataset(
            {
                "name": dataset_info["name"].replace("global", iso3.lower()),
                "title": f"{country_name}: {dataset_info['title']}",
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
        dataset.add_country_location(iso3)

        for file_path in self.country_data[iso3]:
            start_date, end_date = _parse_date(basename(file_path))
            end_date = _parse_dekad(end_date)
            resource = Resource(
                {
                    "name": basename(file_path),
                    "description": f"Data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                }
            )
            resource.set_format("GeoTIFF")
            resource.set_file_to_upload(file_path)
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
