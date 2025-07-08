import datetime
from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.copernicus.drought import Drought
from hdx.scraper.copernicus.ghsl import GHSL
from hdx.scraper.copernicus.utilities import get_boundaries


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
                boundaries_wgs, boundaries_mollweide = get_boundaries(
                    configuration, retriever, tempdir
                )
                drought = Drought(configuration["drought"], retriever, boundaries_wgs)
                updated = drought.get_data(True)
                assert updated is True
                assert drought.dates == {
                    "drought_tracking": [
                        datetime.datetime(
                            2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                        datetime.datetime(
                            2024, 12, 21, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                        datetime.datetime(
                            2025, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                        datetime.datetime(
                            2025, 6, 11, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                    ],
                    "fapar": [
                        datetime.datetime(
                            2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                        datetime.datetime(
                            2024, 12, 21, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                        datetime.datetime(
                            2025, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                        datetime.datetime(
                            2025, 6, 1, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                    ],
                }
                assert drought.downloaded_data == {
                    "drought_tracking": [
                        "tests/fixtures/input/jspa3_m_wld_20240101_20241221_t.zip",
                        "tests/fixtures/input/jspa3_m_wld_20250101_20250611_t.zip",
                    ],
                    "fapar": [
                        "tests/fixtures/input/fpanv_m_gdo_20250101_20250601_t.zip"
                    ],
                }
                assert drought.global_data == {
                    "drought_tracking": [
                        "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/GDO_Meteorological_Drought_Tracking/ver1-0-1/jspa3_m_wld_20240101_20241221_t.zip",
                        "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/GDO_Meteorological_Drought_Tracking/ver1-0-1/jspa3_m_wld_20250101_20250611_t.zip",
                    ],
                    "fapar": [
                        "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/GDO_Fraction_of_Absorbed_Photosynthetically_Active_Radiation_Anomalies_fAPAR_VIIRS/ver3-0-0/fpanv_m_gdo_20240101_20241221_t.zip",
                        "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/GDO_Fraction_of_Absorbed_Photosynthetically_Active_Radiation_Anomalies_fAPAR_VIIRS/ver3-0-0/fpanv_m_gdo_20250101_20250601_t.zip",
                    ],
                }

                dataset = drought.generate_global_dataset("drought_tracking")
                assert dataset == {
                    "name": "global-meteorological-drought-tracking",
                    "title": "Copernicus Meteorological Drought Tracking (ERA5)",
                    "notes": "The Indicator for Meteorological Drought Tracking provides a clear spatio-temporal identification of persistent low-precipitation conditions at the global scale and at near real-time. It provides the outline and the duration and total spatial extent of drought clusters.  \n  \nThe indicator is based on the Standardized Precipitation Index (SPI) for 3-months accumulation periods (i.e. SPI-3). The SPI indicator is derived from the ERA5 fifth generation reanalysis for the global climate and weather of the ECMWF, with baseline 1991-2020. Drought events are identified by means of a three dimensional density-based clustering algorithm (DBSCAN).",
                    "methodology": "Other",
                    "methodology_other": "https://drought.emergency.copernicus.eu/data/factsheets/factsheet_met_drought_tracking.pdf",
                    "caveats": "A range of different degrees of clustering can be obtained using different model settings and parameterization, meaning that a same parameterization at the global scale can likely result in overestimation/underestimation of drought events in certain areas.",
                    "data_update_frequency": "14",
                    "dataset_date": "[2024-01-01T00:00:00 TO 2025-06-20T23:59:59]",
                    "tags": [
                        {
                            "name": "drought",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "world"}],
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "jspa3_m_wld_20250101_20250611_t.zip",
                        "description": "Data from 2025-01-01 to 2025-06-20",
                        "format": "geojson",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "jspa3_m_wld_20240101_20241221_t.zip",
                        "description": "Data from 2024-01-01 to 2024-12-31",
                        "format": "geojson",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]

                dataset = drought.generate_global_dataset("fapar")
                assert dataset == {
                    "name": "global-anomalies-fapar-viirs",
                    "title": "Copernicus Vegetation Index Anomaly (FAPAR Anomaly)",
                    "notes": "Fraction of Absorbed Photosynthetically Active Radiation (FAPAR) is a biophysical dimensionless quantity (its values range from 0/no absorption to 1/total absorption) used to assess the greenness and health of vegetation. FAPAR anomalies can be used as an indicator to detect and monitor the impacts of agricultural drought on the growth and productivity of vegetation.  \n  \nThe data is presented in 10-day time composite of the Visible Infrared Imaging Radiometer Suite (VIIRS).",
                    "methodology": "Other",
                    "methodology_other": "FAPAR and FAPAR anomalies datasets come as raster files for every 10-day interval. The deviation of the FAPAR values from the long-term mean (anomaly) are calculated at each spatial location (grid-cell), with a reference baseline that ranges from the year 2012 to the last available full year (see Section 3). Negative FAPAR anomalies suggest conditions of relative vegetation stress, especially plant water stress due to drought, during that 10-day interval. In contrast, positive FAPAR anomalies indicate relatively favorable vegetation growth conditions during that 10-day interval.  \nMore information: https://drought.emergency.copernicus.eu/data/factsheets/factsheet_fapar_viirs.pdf",
                    "caveats": "Variations in the vegetation health and/or cover could be related to stress factors not related to droughts (e.g., plant diseases, pests, hail, flooding). To determine if changes in FAPAR are linked with a drought event or not, FAPAR data should be interpreted jointly with other indicators (as in the EDO Combined Drought Indicator).",
                    "data_update_frequency": "14",
                    "dataset_date": "[2024-01-01T00:00:00 TO 2025-06-10T23:59:59]",
                    "tags": [
                        {
                            "name": "drought",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "world"}],
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "fpanv_m_gdo_20240101_20241221_t.zip",
                        "description": "Data from 2024-01-01 to 2024-12-31",
                        "url": "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/GDO_Fraction_of_Absorbed_Photosynthetically_Active_Radiation_Anomalies_fAPAR_VIIRS/ver3-0-0/fpanv_m_gdo_20240101_20241221_t.zip",
                        "format": "geotiff",
                        "resource_type": "api",
                        "url_type": "api",
                    },
                    {
                        "name": "fpanv_m_gdo_20250101_20250601_t.zip",
                        "description": "Data from 2025-01-01 to 2025-06-10",
                        "url": "https://drought.emergency.copernicus.eu/data/Drought_Observatories_datasets/GDO_Fraction_of_Absorbed_Photosynthetically_Active_Radiation_Anomalies_fAPAR_VIIRS/ver3-0-0/fpanv_m_gdo_20250101_20250601_t.zip",
                        "format": "geotiff",
                        "resource_type": "api",
                        "url_type": "api",
                    },
                ]

                file_paths = drought.unzip_data("drought_tracking")
                assert file_paths == {}

                file_paths = drought.unzip_data("fapar")
                assert file_paths == {
                    join(tempdir, "fpanv_m_gdo_20250101_20250601_t"): [
                        "fpanv_m_gdo_20250101_t_300_z01.tif",
                        "copyright.txt",
                        "README.txt",
                    ],
                }

                country_data = drought.process("CUB", file_paths)
                assert country_data == [
                    join(tempdir, "cub_fpanv_m_gdo_20250101_20250601_t.zip")
                ]

                dataset = drought.generate_dataset("CUB", "fapar")
                assert dataset == {
                    "name": "cub-anomalies-fapar-viirs",
                    "title": "Cuba: Copernicus Vegetation Index Anomaly (FAPAR Anomaly)",
                    "notes": "Fraction of Absorbed Photosynthetically Active Radiation (FAPAR) is a biophysical dimensionless quantity (its values range from 0/no absorption to 1/total absorption) used to assess the greenness and health of vegetation. FAPAR anomalies can be used as an indicator to detect and monitor the impacts of agricultural drought on the growth and productivity of vegetation.  \n  \nThe data is presented in 10-day time composite of the Visible Infrared Imaging Radiometer Suite (VIIRS).",
                    "methodology": "Other",
                    "methodology_other": "FAPAR and FAPAR anomalies datasets come as raster files for every 10-day interval. The deviation of the FAPAR values from the long-term mean (anomaly) are calculated at each spatial location (grid-cell), with a reference baseline that ranges from the year 2012 to the last available full year (see Section 3). Negative FAPAR anomalies suggest conditions of relative vegetation stress, especially plant water stress due to drought, during that 10-day interval. In contrast, positive FAPAR anomalies indicate relatively favorable vegetation growth conditions during that 10-day interval.  \nMore information: https://drought.emergency.copernicus.eu/data/factsheets/factsheet_fapar_viirs.pdf",
                    "caveats": "Variations in the vegetation health and/or cover could be related to stress factors not related to droughts (e.g., plant diseases, pests, hail, flooding). To determine if changes in FAPAR are linked with a drought event or not, FAPAR data should be interpreted jointly with other indicators (as in the EDO Combined Drought Indicator).",
                    "data_update_frequency": "14",
                    "dataset_date": "[2024-01-01T00:00:00 TO 2025-06-10T23:59:59]",
                    "tags": [
                        {
                            "name": "drought",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "cub"}],
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "cub_fpanv_m_gdo_20250101_20250601_t.zip",
                        "description": "Data from 2025-01-01 to 2025-06-10",
                        "format": "geotiff",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]

                ghsl = GHSL(configuration["ghsl"], retriever, boundaries_mollweide)
                updated = ghsl.get_data(2024, True, False)
                assert updated is True
                assert ghsl.data_year == {"built": 2020, "population": 2020}
                assert ghsl.global_data == {
                    "built": "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_S_GLOBE_R2023A/GHS_BUILT_S_E2020_GLOBE_R2023A_54009_100/V1-0/GHS_BUILT_S_E2020_GLOBE_R2023A_54009_100_V1_0.zip",
                    "population": "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2020_GLOBE_R2023A_54009_100/V1-0/GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.zip",
                }
                assert ghsl.latest_data == {
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

                dataset = ghsl.generate_global_dataset()
                assert dataset == {
                    "name": "global-human-settlement-layer-ghsl",
                    "title": "Copernicus Global Human Settlement Layer (GHSL)",
                    "notes": "Open and free data for assessing the human presence on the planet.\r\nThe Global Human Settlement Layer (GHSL) project produces global spatial information, evidence-based analytics, and knowledge describing the human presence on the planet. The GHSL relies on the design and implementation of spatial data processing technologies that allow automatic data analytics and information extraction from large amounts of heterogeneous geospatial data including global, fine-scale satellite image data streams, census data, and crowd sourced or volunteered geographic information sources.  \r\nThe JRC, together with the Directorate-General for Regional and Urban Policy (DG REGIO) and Directorate-General for Defence Industry and Space (DG DEFIS) are working towards a regular and operational monitoring of global built-up and population based on the processing of Sentinel Earth Observation data produced by European Copernicus space program. In addition, the EU Agency for the Space Programme (EUSPA) undertakes activities related to user uptake of data, information and services.",
                    "methodology": "Other",
                    "methodology_other": "The GHSL relies on the design and implementation of spatial data processing technologies that allow automatic data analytics and information extraction from large amounts of heterogeneous geospatial data including global, fine-scale satellite image data streams, census data, and crowd sourced or volunteered geographic information sources.  \r\n\r\nMethodology [link](https://human-settlement.emergency.copernicus.eu/documents/GHSL_Data_Package_2023.pdf?t=1727170839).",
                    "caveats": "Pesaresi M., Politis P. (2023): GHS-BUILT-S R2023A - GHS built-up surface grid, derived from Sentinel2 composite and Landsat, multitemporal (1975-2030). European Commission, Joint Research Centre (JRC)\r\nPID: http://data.europa.eu/89h/9f06f36f-4b11-47ec-abb0-4f8b7b1d72ea\r\ndoi:10.2905/9F06F36F-4B11-47EC-ABB0-4F8B7B1D72EA \r\n\r\nSchiavina M., Freire S., Carioli A., MacManus K. (2023): GHS-POP R2023A - GHS population grid multitemporal (1975-2030). European Commission, Joint Research Centre (JRC)\r\nPID: http://data.europa.eu/89h/2ff68a52-5b5b-4a22-8f40-c41da8332cfe\r\ndoi:10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE \r\n",
                    "data_update_frequency": "365",
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

                ghsl.get_tiling_schema()
                assert len(ghsl.tiling_schema) == 375
                iso3s = ghsl.get_boundaries()
                assert iso3s == ["CUB", "JAM"]
                assert ghsl.tiles_by_country == {
                    "CUB": ["R7_C10", "R7_C11"],
                    "JAM": ["R7_C11"],
                }

                country_data = ghsl.process("CUB")
                assert country_data == {
                    "built": join(
                        tempdir, "GHS_BUILT_S_E2020_R2023A_54009_100_V1_0_CUB.tif"
                    ),
                    "population": join(
                        tempdir, "GHS_POP_E2020_R2023A_54009_100_V1_0_CUB.tif"
                    ),
                }

                dataset = ghsl.generate_dataset("CUB")
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
                    "methodology_other": "The GHSL relies on the design and implementation of spatial data processing technologies that allow automatic data analytics and information extraction from large amounts of heterogeneous geospatial data including global, fine-scale satellite image data streams, census data, and crowd sourced or volunteered geographic information sources.  \r\n\r\nMethodology [link](https://human-settlement.emergency.copernicus.eu/documents/GHSL_Data_Package_2023.pdf?t=1727170839).",
                    "caveats": "Pesaresi M., Politis P. (2023): GHS-BUILT-S R2023A - GHS built-up surface grid, derived from Sentinel2 composite and Landsat, multitemporal (1975-2030). European Commission, Joint Research Centre (JRC)\r\nPID: http://data.europa.eu/89h/9f06f36f-4b11-47ec-abb0-4f8b7b1d72ea\r\ndoi:10.2905/9F06F36F-4B11-47EC-ABB0-4F8B7B1D72EA \r\n\r\nSchiavina M., Freire S., Carioli A., MacManus K. (2023): GHS-POP R2023A - GHS population grid multitemporal (1975-2030). European Commission, Joint Research Centre (JRC)\r\nPID: http://data.europa.eu/89h/2ff68a52-5b5b-4a22-8f40-c41da8332cfe\r\ndoi:10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE \r\n",
                    "dataset_source": "European Commission, Joint Research Centre (JRC)",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "47677055-92e2-4f68-bf1b-5d570f27e791",
                    "data_update_frequency": "365",
                    "notes": "Open and free data for assessing the human presence on the planet.\r\nThe Global Human Settlement Layer (GHSL) project produces global spatial information, evidence-based analytics, and knowledge describing the human presence on the planet. The GHSL relies on the design and implementation of spatial data processing technologies that allow automatic data analytics and information extraction from large amounts of heterogeneous geospatial data including global, fine-scale satellite image data streams, census data, and crowd sourced or volunteered geographic information sources.  \r\nThe JRC, together with the Directorate-General for Regional and Urban Policy (DG REGIO) and Directorate-General for Defence Industry and Space (DG DEFIS) are working towards a regular and operational monitoring of global built-up and population based on the processing of Sentinel Earth Observation data produced by European Copernicus space program. In addition, the EU Agency for the Space Programme (EUSPA) undertakes activities related to user uptake of data, information and services.",
                    "subnational": "1",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "GHS Built-up Surface",
                        "description": "Product: GHS-BUILT-S, Epoch: 2020, Resolution: 100m, Coordinate system: Mollweide, Classification: total - residential (RES) & non residential (NRES) classification.",
                        "format": "geotiff",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "GHS Population Grid",
                        "description": "Product: GHS-POP, Epoch: 2020, Resolution: 100m, Coordinate system: Mollweide",
                        "format": "geotiff",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
