import logging
from typing import List, Optional

from bs4 import BeautifulSoup
from geopandas import GeoDataFrame, read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve
from shapely.validation import make_valid

logger = logging.getLogger(__name__)


def get_lines(
    retriever: Retrieve, url: str, filename: Optional[str] = None
) -> List[str]:
    text = retriever.download_text(url, filename=filename)
    soup = BeautifulSoup(text, "html.parser")
    lines = soup.find_all("a")
    return lines


def get_boundaries(
    configuration: Configuration, retriever: Retrieve, temp_folder: str
) -> GeoDataFrame:
    dataset = Dataset.read_from_hdx(configuration["boundary_dataset"])
    resources = dataset.get_resources()
    for resource in resources:
        if configuration["boundary_resource"] not in resource["name"]:
            continue
        if retriever.use_saved:
            file_path = retriever.download_file(
                resource["url"], filename=resource["name"]
            )
        else:
            folder = retriever.saved_dir if retriever.save else temp_folder
            _, file_path = resource.download(folder)
        lyr = read_file(file_path)
        lyr = lyr.to_crs(crs="ESRI:54009")
        for i, row in lyr.iterrows():
            if not lyr.geometry[i].is_valid:
                lyr.loc[i, "geometry"] = make_valid(lyr.geometry[i])
            if row["STATUS"] and row["STATUS"][:4] == "Adm.":
                lyr.loc[i, "ISO_3"] = row["Color_Code"]
        lyr = lyr.dissolve(by="ISO_3", as_index=False)
        lyr = lyr.drop(
            [f for f in lyr.columns if f.lower() not in ["iso_3", "geometry"]],
            axis=1,
        )
        return lyr
