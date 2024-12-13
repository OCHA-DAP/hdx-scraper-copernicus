#!/usr/bin/python
"""copernicus scraper"""

import logging
import re
from os.path import join
from typing import List, Optional
from zipfile import ZipFile

from bs4 import BeautifulSoup
from geopandas import GeoDataFrame, read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from shapely.validation import make_valid

logger = logging.getLogger(__name__)

_MODELED_YEAR_PATTERN = "(?<!\\d)r2\\d{3}(?!\\d)"
_DATA_YEAR_PATTERN = "(?<!\\d)e2\\d{3}(?!\\d)"


class Copernicus:
    def __init__(self, configuration: Configuration, retriever: Retrieve):
        self._configuration = configuration
        self._retriever = retriever
        self.folder = retriever.saved_dir if retriever.save else retriever.temp_dir
        self.global_data = GeoDataFrame()
        self.latest_data = {}
        self.latest_data_urls = {}
        self.data = {}

    def get_lines(self, url: str, filename: Optional[str] = None) -> List[str]:
        text = self._retriever.download_text(url, filename=filename)
        soup = BeautifulSoup(text, "html.parser")
        lines = soup.find_all("a")
        return lines

    def get_boundaries(self):
        dataset = Dataset.read_from_hdx(self._configuration["boundary_dataset"])
        resources = dataset.get_resources()
        for resource in resources:
            if self._configuration["boundary_resource"] not in resource["name"]:
                continue
            _, file_path = resource.download(self.folder)
            lyr = read_file(file_path)
            lyr = lyr.to_crs(crs="ESRI:54009")
            for i, _ in lyr.iterrows():
                if not lyr.geometry[i].is_valid:
                    lyr.loc[i, "geometry"] = make_valid(lyr.geometry[i])
            lyr.loc[lyr["ISO_3"] == "PSE", "Color_Code"] = "PSE"
            lyr = lyr.dissolve(by="Color_Code", as_index=False)
            lyr = lyr.drop(
                [f for f in lyr.columns if f.lower() not in ["color_code", "geometry"]],
                axis=1,
            )
            self.global_data = lyr
            return

    def get_ghs_data(self, current_year: int):
        file_patterns = self._configuration["file_patterns"]
        base_url = self._configuration["base_url"]
        lines = self.get_lines(base_url, "ghsl_ftp.txt")
        for data_type, subfolder_pattern in file_patterns.items():
            subfolders = []
            for line in lines:
                subfolder = line.get("href")
                if subfolder_pattern not in subfolder:
                    continue
                subfolders.append(subfolder)
            subfolder = _select_latest_data(_MODELED_YEAR_PATTERN, subfolders)
            sub_lines = self.get_lines(
                f"{base_url}{subfolder}",
                filename=f"{subfolder.replace("/", "")}.txt",
            )
            subsubfolders = []
            for sub_line in sub_lines:
                subsubfolder = sub_line.get("href")
                if not subsubfolder.endswith(f"{self._configuration['resolution']}/"):
                    continue
                if "NRES" in subsubfolder:
                    continue
                subsubfolders.append(subsubfolder)
            subsubfolder = _select_latest_data(
                _DATA_YEAR_PATTERN, subsubfolders, current_year
            )
            tile_lines = self.get_lines(
                f"{base_url}{subfolder}{subsubfolder}V1-0/tiles/",
                filename=f"{subsubfolder.replace("/", "")}.txt",
            )
            for tile_line in tile_lines:
                zip_file = tile_line.get("href")
                if ".zip" not in zip_file:
                    continue
                zip_url = f"{base_url}{subfolder}{subsubfolder}V1-0/tiles/{zip_file}"
                dict_of_lists_add(self.latest_data_urls, data_type, zip_url)
                zip_file_path = self._retriever.download_file(zip_url)
                if self._retriever.use_saved:
                    file_path = join(self.folder, f"{zip_file[:-4]}.tif")
                else:
                    with ZipFile(zip_file_path, "r") as z:
                        file_path = z.extract(f"{zip_file[:-4]}.tif", self.folder)
                dict_of_lists_add(self.latest_data, data_type, file_path)

    def process(self) -> List:
        return []

    def generate_dataset(self, dataset_name: str) -> Optional[Dataset]:
        dataset_title = None
        dataset_time_period = None
        dataset_tags = None
        dataset_country_iso3 = None

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_period)
        dataset.add_tags(dataset_tags)
        # Only if needed
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Add resources here

        return dataset


def _select_latest_data(
    pattern: str, files: List[str], max_year: Optional[int] = None
) -> str:
    year_matches = [re.findall(pattern, f, re.IGNORECASE) for f in files]
    year_matches = [int(y[0][1:]) if len(y) > 0 else 0 for y in year_matches]
    if max_year:
        year_matches = [y if y <= max_year else 0 for y in year_matches]
    max_index = year_matches.index(max(year_matches))
    latest_data = files[max_index]
    return latest_data
