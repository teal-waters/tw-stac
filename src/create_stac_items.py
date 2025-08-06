"""Create stac items for all 1/3 arc-second USGS dems."""

import json
from pathlib import Path

import obstore
import rasterio
import rio_stac
import rio_stac.stac
from pystac import Asset
from pystac import MediaType


def create_stac_items(output_folder: Path) -> None:
    """Run the process."""
    prefix = "s3://prd-tnm/StagedProducts/Elevation/13/TIFF/current"
    store = obstore.store.from_url(
        prefix,
        skip_signature=True,
        region="us-west-2",
    )

    output_store = obstore.store.from_url("az://tw-staging", account_name="tealwaters")

    for chunk in store.list(chunk_size=1000):
        for item in chunk:
            s3_url = f"{prefix}/{item['path']}"
            if s3_url.endswith(".tif"):
                with rasterio.open(s3_url) as src:
                    raster_info = {
                        "raster:bands": rio_stac.stac.get_raster_info(
                            src, max_size=1024
                        )
                    }
                    assets = dict(
                        dem=Asset(
                            media_type=MediaType.COG,
                            href=s3_url,
                            roles=["data"],
                            extra_fields={**raster_info},
                        )
                    )
                    stac_item = rio_stac.create_stac_item(
                        src,
                        assets=assets,
                        with_proj=True,
                        with_raster=True,
                        with_eo=True,
                        collection="USGS_DEM_13",
                    )
                item_json = json.dumps(stac_item.to_dict(), indent=2).encode("utf-8")
                output_path = output_folder / Path(s3_url).with_suffix(".json").name
                output_store.put(str(output_path), item_json)
                print(output_path)


if __name__ == "__main__":
    output_folder = Path("usgs/dem/13")
    with rasterio.Env(AWS_NO_SIGN_REQUEST="YES", AWS_REGION="us-west-2"):
        create_stac_items(output_folder)
