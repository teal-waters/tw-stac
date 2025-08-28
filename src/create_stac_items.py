"""Create stac items for all 1/3 arc-second USGS dems."""

import json
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path

import obstore
import rasterio
import rio_stac
import rio_stac.stac
from pystac import Asset
from pystac import MediaType

# Items should have a standard datetime (So they don't come in as multi-dimensional)
# Since we are crawling the "current" folder there should only be one version
# of anything. The alternative might be to record the timestamp of the file,
# as there is no metadata with the data itself that indicates production time
# (The xmls seem to give a range of dates in the past from e.g. 1999 to 2013.
DATETIME = datetime.now()


def create_stac_items(output_file: TextIOWrapper) -> None:
    """Run the process."""
    prefix = "s3://prd-tnm/StagedProducts/Elevation/13/TIFF/current"
    store = obstore.store.from_url(
        prefix,
        skip_signature=True,
        region="us-west-2",
    )

    output_file.write("[")

    for chunk in store.list(chunk_size=1000):
        for item in chunk:
            s3_url = f"{prefix}/{item['path']}"
            if s3_url.endswith(".tif"):
                with rasterio.open(s3_url) as src:
                    #                    raster_info = {
                    #                        "raster:bands": rio_stac.stac.get_raster_info(
                    #                            src, max_size=1024
                    #                        )
                    #                    }
                    https_url = s3_url.replace(
                        "s3://prd-tnm", "https://prd-tnm.s3.amazonaws.com"
                    )
                    assets = dict(
                        dem=Asset(
                            media_type=MediaType.COG,
                            href=https_url,
                            roles=["data"],
                            # extra_fields={**raster_info},
                        )
                    )
                    stac_item = rio_stac.create_stac_item(
                        src,
                        input_datetime=DATETIME,
                        assets=assets,
                        with_proj=True,
                        with_raster=True,
                        with_eo=True,
                        collection="usgs_dem_13",
                    )
                item_json = json.dumps(
                    stac_item.to_dict(), indent=2
                )  # .encode("utf-8")
                output_file.write(item_json + ",\n")
                # output_path = output_folder / Path(s3_url).with_suffix(".json").name
                # output_store.put(str(output_path), item_json)
                # print(output_path)
    output_file.write("]")


if __name__ == "__main__":
    output_file = Path("usgs/dem/13/items.json")
    with (
        rasterio.Env(AWS_NO_SIGN_REQUEST="YES", AWS_REGION="us-west-2"),
        open(output_file, "w") as output_sink,
    ):
        create_stac_items(output_sink)
