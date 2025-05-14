from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.copernicus.copernicus import Copernicus


class TestCopernicus:
    def test_copernicus(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with temp_dir(
            "TestCopernicus",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                configuration["dataset_dates"] = {
                    "default": {"modeled": 2023, "estimated": 2019}
                }
                copernicus = Copernicus(
                    configuration,
                    retriever,
                )
                updated = copernicus.get_ghs_data(2024, True)
                assert updated is True
                assert copernicus.data_year == {"built": 2020, "population": 2020}
                assert copernicus.global_data == {
                    "built": "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_S_GLOBE_R2023A/GHS_BUILT_S_E2020_GLOBE_R2023A_54009_100/V1-0/GHS_BUILT_S_E2020_GLOBE_R2023A_54009_100_V1_0.zip",
                    "population": "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2020_GLOBE_R2023A_54009_100/V1-0/GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.zip",
                }
                assert copernicus.latest_data == {
                    "built": [
                        join(
                            tempdir,
                            "GHS_BUILT_S_E2020_GLOBE_R2023A_54009_100_V1_0_R7_C10.tif",
                        ),
                        join(
                            tempdir,
                            "GHS_BUILT_S_E2020_GLOBE_R2023A_54009_100_V1_0_R7_C11.tif",
                        ),
                    ],
                    "population": [
                        join(
                            tempdir,
                            "GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0_R7_C10.tif",
                        ),
                        join(
                            tempdir,
                            "GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0_R7_C11.tif",
                        ),
                    ],
                }

                dataset = copernicus.generate_global_dataset()
                assert dataset == {
                    "name": "global-human-settlement-layer-ghsl",
                    "title": "Copernicus Global Human Settlement Layer (GHSL)",
                    "dataset_date": "[2020-01-01T00:00:00 TO 2020-12-31T23:59:59]",
                    "tags": [
                        {
                            "name": "facilities-infrastructure",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "populated places-settlements",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "world"}],
                    "customviz": [
                        {
                            "url": "https://human-settlement.emergency.copernicus.eu/visualisation.php#lnlt=@50.93074,12.87598,5z&v=301&ln=0&gr=ds&lv=10000000000000000000000000000000000000011111&lo=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&pg=V"
                        }
                    ],
                }

                copernicus.get_tiling_schema()
                assert len(copernicus.tiling_schema) == 375
                iso3s = copernicus.get_boundaries()
                assert iso3s == ["CUB", "JAM"]
                assert copernicus.tiles_by_country == {
                    "CUB": ["R7_C10", "R7_C11"],
                    "JAM": ["R7_C11"],
                }

                country_data = copernicus.process("CUB")
                assert country_data == {
                    "built": join(
                        tempdir, "GHS_BUILT_S_E2020_R2023A_54009_100_V1_0_CUB.tif"
                    ),
                    "population": join(
                        tempdir, "GHS_POP_E2020_R2023A_54009_100_V1_0_CUB.tif"
                    ),
                }

                dataset = copernicus.generate_dataset("CUB")
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "cub-ghsl",
                    "title": "Cuba: Copernicus Global Human Settlement Layer (GHSL)",
                    "dataset_date": "[2020-01-01T00:00:00 TO 2020-12-31T23:59:59]",
                    "tags": [
                        {
                            "name": "facilities-infrastructure",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "populated places-settlements",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "cub"}],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "The GHSL relies on the design and implementation of "
                    "spatial data processing technologies that allow automatic data analytics and "
                    "information extraction from large amounts of heterogeneous geospatial data "
                    "including global, fine-scale satellite image data streams, census data, and "
                    "crowd sourced or volunteered geographic information sources.  \r\n\r\n"
                    "Methodology [link](https://human-settlement.emergency.copernicus.eu/"
                    "documents/GHSL_Data_Package_2023.pdf?t=1727170839).",
                    "caveats": "Pesaresi M., Politis P. (2023): GHS-BUILT-S R2023A - GHS built-up "
                    "surface grid, derived from Sentinel2 composite and Landsat, multitemporal "
                    "(1975-2030). European Commission, Joint Research Centre (JRC)\r\nPID: "
                    "http://data.europa.eu/89h/9f06f36f-4b11-47ec-abb0-4f8b7b1d72ea\r\ndoi:"
                    "10.2905/9F06F36F-4B11-47EC-ABB0-4F8B7B1D72EA \r\n\r\nSchiavina M., Freire "
                    "S., Carioli A., MacManus K. (2023): GHS-POP R2023A - GHS population grid "
                    "multitemporal (1975-2030). European Commission, Joint Research Centre (JRC)"
                    "\r\nPID: http://data.europa.eu/89h/2ff68a52-5b5b-4a22-8f40-c41da8332cfe\r\n"
                    "doi:10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE \r\n",
                    "dataset_source": "European Commission, Joint Research Centre (JRC)",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "47677055-92e2-4f68-bf1b-5d570f27e791",
                    "data_update_frequency": 365,
                    "notes": "Open and free data for assessing the human presence on the planet."
                    "\r\nThe Global Human Settlement Layer (GHSL) project produces global spatial "
                    "information, evidence-based analytics, and knowledge describing the human "
                    "presence on the planet. The GHSL relies on the design and implementation of "
                    "spatial data processing technologies that allow automatic data analytics and "
                    "information extraction from large amounts of heterogeneous geospatial data "
                    "including global, fine-scale satellite image data streams, census data, and "
                    "crowd sourced or volunteered geographic information sources.  \r\nThe JRC, "
                    "together with the Directorate-General for Regional and Urban Policy (DG "
                    "REGIO) and Directorate-General for Defence Industry and Space (DG DEFIS) "
                    "are working towards a regular and operational monitoring of global built-up "
                    "and population based on the processing of Sentinel Earth Observation data "
                    "produced by European Copernicus space program. In addition, the EU Agency "
                    "for the Space Programme (EUSPA) undertakes activities related to user uptake "
                    "of data, information and services.",
                    "subnational": "1",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "GHS Built-up Surface",
                        "description": "Product: GHS-BUILT-S, Epoch: 2020, Resolution: 100m, "
                        "Coordinate system: Mollweide, Classification: total - residential (RES) "
                        "& non residential (NRES) classification.",
                        "format": "geotiff",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "GHS Population Grid",
                        "description": "Product: GHS-POP, Epoch: 2020, Resolution: 100m, "
                        "Coordinate system: Mollweide",
                        "format": "geotiff",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
