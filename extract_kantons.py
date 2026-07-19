import argparse
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
import numpy as np
import pandas as pd
import pystac_client
import rasterio
import shapely
from rasterio.mask import mask

# A Kanton with zero valid pixels for a given date (e.g. full cloud cover)
# makes np.mean() operate on an empty array. This is expected and handled
# below (round(nan) raises ValueError, which falls back to the sentinel
# value), so the warning numpy prints for it is just noise.
warnings.filterwarnings(
    "ignore", message="Mean of empty slice", category=RuntimeWarning)
warnings.filterwarnings(
    "ignore", message="invalid value encountered in scalar divide", category=RuntimeWarning)

"""
Extract per-Kanton VHI raster statistics (forest and vegetation) from the
data.geo.admin.ch STAC collection "ch.swisstopo.swisseo_vhi_v100" and store
the result as GeoParquet files, mirroring the layout of parquet_files/ but
using the Kanton boundaries instead of the FOEN warnregions.

Usage:
    python extract_kantons.py --start-date 2024-01-01 --end-date 2024-12-31

Existing output parquet files are skipped, so the script can be re-run to
only fetch newly published dates.
"""

STAC_URL = "https://data.geo.admin.ch/api/stac/v0.9/"
COLLECTION_ID = "ch.swisstopo.swisseo_vhi_v100"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KANTON_SHAPEFILE = os.path.join(
    SCRIPT_DIR, "Boundaries_G1_Canton_20260101.shp")
OUTPUT_ROOT = os.path.join(SCRIPT_DIR, "parquet_files_kt")

# depend on the Kanton shapefile in use
REGIONNR_COL = "KTNR"
REGIONNAME_COL = "KTNAME"

MISSING_VALUES = 110  # matches the source item metadata's "missing_data"
NO_DATA_VALUES = 255  # matches the source item metadata's "no_data"
SCALING_FACTOR = 1
MEAN_TYPE = "vhi"

DATA_TYPES = ["forest", "vegetation"]


def scale_raster_values(raster_values, scaling_factor):
    if scaling_factor == 1:
        return raster_values
    decimal_places = len(str(scaling_factor)) - 1
    return [round(value / scaling_factor, decimal_places) for value in raster_values]


def load_kanton_gdf():
    return gpd.read_file(KANTON_SHAPEFILE)[
        [REGIONNR_COL, REGIONNAME_COL, "geometry"]]


def export(raster_url, gdf, filename, dateISO8601, missing_values, no_data_values, scaling_factor, mean_type):
    mean_descriptor = mean_type + "_mean"
    availpercen = "availability_percentage"
    date_column = "date"

    gdf = gdf.copy()

    with rasterio.open(raster_url) as src:
        raster_values = []
        availability_percentages = []
        for _, row in gdf.iterrows():
            geom = row["geometry"]
            try:
                out_image, _ = mask(src, [geom], crop=True)
                values = out_image[0].flatten()

                missing_values_count = np.count_nonzero(
                    values == missing_values)

                valid_values = values[(values != src.nodata) & (
                    values != missing_values) & (values != no_data_values)]
                valid_values_count = valid_values.size

                mean_value_rounded = round(np.mean(valid_values))

                total_cells = valid_values_count + missing_values_count
                availability_percentage_rounded = round(
                    (valid_values_count / total_cells) * 100, 1)

                raster_values.append(mean_value_rounded)
                availability_percentages.append(
                    availability_percentage_rounded)
            except ValueError:
                # empty intersection between polygon and raster
                raster_values.append(missing_values)
                availability_percentages.append(missing_values)

    gdf[mean_descriptor] = scale_raster_values(raster_values, scaling_factor)
    gdf[availpercen] = availability_percentages

    gdf.drop(columns=[REGIONNAME_COL], inplace=True)

    gdf[REGIONNR_COL] = gdf[REGIONNR_COL].astype(int)
    if scaling_factor == 1:
        gdf[mean_descriptor] = gdf[mean_descriptor].astype(int)
    else:
        gdf[mean_descriptor] = gdf[mean_descriptor].astype(float)

    # Round the coordinates to 0 decimals resulting in approx 0.2m displacement of the vertexes
    gdf.geometry = shapely.wkt.loads(
        shapely.wkt.dumps(gdf.geometry, rounding_precision=0))

    gdf[date_column] = pd.to_datetime(
        dateISO8601).tz_convert("UTC").floor("s")

    gdf.to_parquet(filename + ".parquet", compression="gzip")


def find_cog_href(item, data_type):
    suffix = f"{data_type}-10m.tif"
    for key, asset in item.assets.items():
        if key.endswith(suffix) and "cloud-optimized" in (asset.media_type or ""):
            return asset.href
    return None


def process_task(kanton_gdf, raster_href, output_path, dateISO8601):
    print(f"Processing {os.path.basename(output_path)}.parquet", flush=True)
    export(
        raster_href,
        kanton_gdf,
        output_path,
        dateISO8601,
        MISSING_VALUES,
        NO_DATA_VALUES,
        SCALING_FACTOR,
        MEAN_TYPE,
    )


def process_date_range(start_date, end_date, max_workers=4):
    for data_type in DATA_TYPES:
        os.makedirs(os.path.join(OUTPUT_ROOT, data_type), exist_ok=True)

    service = pystac_client.Client.open(STAC_URL)
    service.add_conforms_to("COLLECTIONS")
    service.add_conforms_to("ITEM_SEARCH")

    search = service.search(
        collections=[COLLECTION_ID],
        datetime=f"{start_date}T00:00:00Z/{end_date}T23:59:59Z",
    )

    kanton_gdf = load_kanton_gdf()

    # Each date/data_type item is an independent job that's mostly spent
    # waiting on the network, so they're farmed out to a thread pool
    # (GDAL's I/O releases the GIL) instead of processed one at a time.
    tasks = []
    skipped, missing_asset = 0, 0
    for item in search.items():
        dateISO8601 = item.datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

        for data_type in DATA_TYPES:
            filename = f"ch.swisstopo.swisseo_vhi_v100_{item.id}_{data_type}-kantons"
            output_path = os.path.join(OUTPUT_ROOT, data_type, filename)

            if os.path.exists(output_path + ".parquet"):
                print(f"Skipping {filename}.parquet, already exists", flush=True)
                skipped += 1
                continue

            raster_href = find_cog_href(item, data_type)
            if raster_href is None:
                print(f"No {data_type} raster asset found for {item.id}, skipping", flush=True)
                missing_asset += 1
                continue

            tasks.append((raster_href, output_path, dateISO8601))

    processed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_task, kanton_gdf,
                             raster_href, output_path, dateISO8601)
            for raster_href, output_path, dateISO8601 in tasks
        ]
        for future in as_completed(futures):
            future.result()  # re-raise any exception from the worker thread
            processed += 1

    print(
        f"Done. Processed: {processed}, skipped (already existed): {skipped}, missing asset: {missing_asset}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract per-Kanton VHI statistics from the swisstopo STAC collection.")
    parser.add_argument("--start-date", required=True,
                         help="Start date in YYYY-MM-DD")
    parser.add_argument("--end-date", required=True,
                         help="End date in YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=4,
                         help="Number of dates/types to fetch concurrently (default: 4)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    process_date_range(args.start_date, args.end_date, max_workers=args.workers)
