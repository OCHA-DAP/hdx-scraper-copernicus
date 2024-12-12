#!/usr/bin/python
"""copernicus scraper"""

import logging
from typing import List, Optional

from geopandas import GeoDataFrame, read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Copernicus:
    def __init__(self, configuration: Configuration, retriever: Retrieve):
        self._configuration = configuration
        self._retriever = retriever
        self.global_data = GeoDataFrame()
        self.data = {}

    def get_boundaries(self):
        dataset = Dataset.read_from_hdx(self._configuration["boundary_dataset"])
        resources = dataset.get_resources()
        for resource in resources:
            if self._configuration["boundary_resource"] not in resource["name"]:
                continue
            if self._retriever.save:
                folder = self._retriever.saved_dir
            else:
                folder = self._retriever.temp_dir
            _, file_path = resource.download(folder)
            self.global_data = read_file(file_path)
            return

    def get_ghs_data(self):
        return

    def process(self) -> List:
        return []

    def generate_dataset(self) -> Optional[Dataset]:

        # To be generated
        dataset_name = None
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
