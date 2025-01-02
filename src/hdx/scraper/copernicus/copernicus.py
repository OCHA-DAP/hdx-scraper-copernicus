#!/usr/bin/python
"""copernicus scraper"""

import logging
import re
from json import loads
from os.path import join
from typing import Dict, List, Optional
from zipfile import ZipFile

import rasterio
from bs4 import BeautifulSoup
from geopandas import overlay, read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_dicts_add, dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from rasterio.mask import mask
from rasterio.merge import merge
from shapely.validation import make_valid
from slugify import slugify

logger = logging.getLogger(__name__)

_MODELED_YEAR_PATTERN = "(?<!\\d)r2\\d{3}(?!\\d)"
_DATA_YEAR_PATTERN = "(?<!\\d)e2\\d{3}(?!\\d)"


class Copernicus:
    def __init__(self, configuration: Configuration, retriever: Retrieve):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_folder = retriever.temp_dir
        self.tiling_schema = None
        self.global_data = {}
        self.tiles_by_country = {}
        self.latest_data = {}
        self.country_data = {}
        self.data_year = {}

    def get_lines(self, url: str, filename: Optional[str] = None) -> List[str]:
        text = self._retriever.download_text(url, filename=filename)
        soup = BeautifulSoup(text, "html.parser")
        lines = soup.find_all("a")
        return lines

    def get_tiling_schema(self):
        url = self._configuration["tiling_schema"]["url"]
        zip_file_path = self._retriever.download_file(url)
        with ZipFile(zip_file_path, "r") as z:
            z.extractall(self._temp_folder)
        file_path = join(self._temp_folder, self._configuration["tiling_schema"]["filename"])
        lyr = read_file(file_path)
        lyr = lyr.drop(
            [f for f in lyr.columns if f.lower() not in ["tile_id", "geometry"]],
            axis=1,
        )
        self.tiling_schema = lyr

    def get_boundaries(self):
        dataset = Dataset.read_from_hdx(self._configuration["boundary_dataset"])
        resources = dataset.get_resources()
        for resource in resources:
            if self._configuration["boundary_resource"] not in resource["name"]:
                continue
            if self._retriever.use_saved:
                file_path = self._retriever.download_file(
                    resource["url"], filename=resource["name"]
                )
            else:
                folder = self._retriever.saved_dir if self._retriever.save else self._temp_folder
                _, file_path = resource.download(folder)
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
            joined_lyr = overlay(self.tiling_schema, lyr, how="intersection")
            for i, row in joined_lyr.iterrows():
                iso = row["Color_Code"]
                if iso[:2] == "XX":
                    continue
                dict_of_lists_add(self.tiles_by_country, row["Color_Code"], row["tile_id"])
            lyr = loads(lyr.to_json())["features"]
            for row in lyr:
                iso = row["properties"]["Color_Code"]
                if iso[:2] == "XX":
                    continue
                self.global_data[iso] = [row["geometry"]]
            return list(self.global_data.keys())

    def get_ghs_data(self, current_year: int, state_dict: Dict) -> bool:
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
            subfolder, _ = _select_latest_data(_MODELED_YEAR_PATTERN, subfolders)
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
            subsubfolder, year = _select_latest_data(
                _DATA_YEAR_PATTERN, subsubfolders, current_year
            )
            if year <= state_dict.get(data_type, state_dict["DEFAULT"]).year:
                return False
            self.data_year[data_type] = year
            state_dict[data_type] = parse_date(f"{year}-01-01")
            tile_lines = self.get_lines(
                f"{base_url}{subfolder}{subsubfolder}V1-0/tiles/",
                filename=f"{subsubfolder.replace("/", "")}.txt",
            )
            for tile_line in tile_lines:
                zip_file = tile_line.get("href")
                if ".zip" not in zip_file:
                    continue
                zip_url = f"{base_url}{subfolder}{subsubfolder}V1-0/tiles/{zip_file}"
                zip_file_path = self._retriever.download_file(zip_url)
                with ZipFile(zip_file_path, "r") as z:
                    file_path = z.extract(f"{zip_file[:-4]}.tif", self._temp_folder)
                dict_of_lists_add(self.latest_data, data_type, file_path)
        return True

    def process(self, iso3: str) -> Dict | None:
        logger.info(f"Processing {iso3}")
        country_name = Country.get_country_name_from_iso3(iso3)
        if not country_name:
            logger.error(f"Couldn't find country {iso3}, skipping")
            return None
        iso_geometry = self.global_data[iso3]
        iso_tiles = self.tiles_by_country[iso3]
        for data_type, raster_list in self.latest_data.items():
            files_to_mosaic = []
            for raster_file in raster_list:
                tile = "_".join(raster_file.split(".")[0].split("_")[-2:])
                if tile not in iso_tiles:
                    continue
                with rasterio.open(raster_file, "r") as dataset:
                    mask_raster, mask_transform = mask(
                        dataset, iso_geometry, all_touched=True, crop=True
                    )
                    mask_meta = dataset.meta.copy()
                mask_meta.update(
                    {
                        "height": mask_raster.shape[1],
                        "width": mask_raster.shape[2],
                        "transform": mask_transform,
                    }
                )
                country_file = raster_file.replace("GLOBE_", "")[:-4] + f"_{iso3}.tif"
                with rasterio.open(country_file, "w", **mask_meta, compress="LZW") as dest:
                    dest.write(mask_raster)
                open_file = rasterio.open(country_file)
                files_to_mosaic.append(open_file)
            mosaic_raster, mosaic_transform = merge(files_to_mosaic)
            mosaic_meta = open_file.meta.copy()
            mosaic_meta.update(
                {
                    "height": mosaic_raster.shape[1],
                    "width": mosaic_raster.shape[2],
                    "transform": mosaic_transform,
                }
            )
            file_name = "_".join(raster_list[0].replace("GLOBE_", "").split("_")[:-2])
            mosaic_file = f"{file_name}_{iso3}.tif"
            with rasterio.open(mosaic_file, "w", **mosaic_meta, compress="LZW") as dest:
                dest.write(mosaic_raster)
            dict_of_dicts_add(self.country_data, iso3, data_type, mosaic_file)
        return self.country_data[iso3]

    def generate_dataset(self, iso3: str) -> Optional[Dataset]:
        country_name = Country.get_country_name_from_iso3(iso3)
        dataset_name = slugify(f"{iso3}-ghsl")
        dataset_title = f"{country_name}: Copernicus Global Human Settlement Layer (GHSL)"

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
        dataset.add_country_location(iso3)

        resource_info = self._configuration["resource_info"]
        for data_type, file_to_upload in self.country_data[iso3].items():
            resource_desc = resource_info[data_type]["description"].replace(
                "YYYY", str(self.data_year[data_type])
            )
            resource = Resource(
                {
                    "name": resource_info[data_type]["name"],
                    "description": resource_desc,
                }
            )
            resource.set_format("GeoTiff")
            resource.set_file_to_upload(self.country_data[iso3][data_type])
            dataset.add_update_resource(resource)

        return dataset


def _select_latest_data(
    pattern: str, files: List[str], max_year: Optional[int] = None
) -> (int, str):
    year_matches = [re.findall(pattern, f, re.IGNORECASE) for f in files]
    year_matches = [int(y[0][1:]) if len(y) > 0 else 0 for y in year_matches]
    if max_year:
        year_matches = [y if y <= max_year else 0 for y in year_matches]
    max_year = max(year_matches)
    max_index = year_matches.index(max_year)
    latest_data = files[max_index]
    return latest_data, max_year
