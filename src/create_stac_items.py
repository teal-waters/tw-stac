"""Create stac items for all 1/3 arc-second USGS dems."""

import json
from datetime import datetime
from pathlib import Path

import obstore
import rasterio
import rio_stac
from pystac import Asset
from pystac import MediaType
from pystac.validation import validate

# Items should have a standard datetime (So they don't come in as multi-dimensional)
# Since we are crawling the "current" folder there should only be one version
# of anything. The alternative might be to record the timestamp of the file,
# as there is no metadata with the data itself that indicates production time
# (The xmls accompanying individual files seem to give a range of dates in the
#  past from e.g. 1999 to 2013.)
DATETIME = datetime.now()
CONTAINER = "tw-staging"
ACCOUNT_NAME = "tealwaters"


def create_stac_items(
    output_folder: Path, container: str = CONTAINER, account_name: str = ACCOUNT_NAME
) -> None:
    """Run the process."""
    input_prefix = "s3://prd-tnm/StagedProducts/Elevation/13/TIFF/current"
    input_store = obstore.store.from_url(
        input_prefix,
        skip_signature=True,
        region="us-west-2",
    )

    output_store = obstore.store.from_url(
        f"az://{container}", account_name=account_name
    )

    for chunk in input_store.list(chunk_size=1000):
        for item in chunk:
            s3_url = f"{input_prefix}/{item['path']}"
            if s3_url.endswith(".tif"):
                with rasterio.open(s3_url) as src:
                    https_url = s3_url.replace(
                        "s3://prd-tnm", "https://prd-tnm.s3.amazonaws.com"
                    )
                    assets = dict(
                        elevation=Asset(
                            media_type=MediaType.COG,
                            href=https_url,
                            roles=["data"],
                        )
                    )
                    # I tried to add raster extension but non-conus files have
                    # the nodata values mis-set which results in -Infinity values
                    # for minimum, which pgstac can't read (and which I think
                    # are invalid by the stac spec).
                    # If we wanted to work around this, we would have to detect
                    # if items were outside conus (by parsing the nXXwYYY values in
                    # the s3_url) and setting the nodata value ourselves (to -nan)
                    stac_item = rio_stac.create_stac_item(
                        src,
                        id=Path(s3_url).stem,
                        input_datetime=DATETIME,
                        assets=assets,
                        with_proj=True,
                        with_eo=True,
                        collection="usgs_dem_13",
                    )
                item_json = json.dumps(stac_item.to_dict(), indent=2).encode("utf-8")
                output_path = output_folder / Path(s3_url).with_suffix(".json").name
                self_href = f"https://{account_name}.blob.core.windows.net/{container}/{output_path}"
                stac_item.set_self_href(self_href)
                validate(stac_item)
                output_store.put(str(output_path), item_json)
                print(output_path)


if __name__ == "__main__":
    output_folder = Path("usgs/dem/13")
    # environment settings are to read source bucket & without credentials
    with rasterio.Env(AWS_NO_SIGN_REQUEST="YES", AWS_REGION="us-west-2"):
        create_stac_items(output_folder=output_folder)
