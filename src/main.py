"""Create stac items for all 1/3 arc-second USGS dems."""

import os
from pathlib import Path

import obstore
import rasterio
import rio_stac


def main(output_folder: Path) -> None:
    """Run the process."""
    prefix = "s3://prd-tnm/StagedProducts/Elevation/13/TIFF/current"
    store = obstore.store.from_url(
        prefix,
        skip_signature=True,
        region="us-west-2",
    )
    for chunk in store.list(chunk_size=1000):
        for item in chunk:
            s3_url = f"{prefix}/{item['path']}"
            if s3_url.endswith(".tif"):
                with rasterio.open(s3_url) as src:
                    stac_item = rio_stac.create_stac_item(src)

                output_path = output_folder / Path(s3_url).with_suffix(".json").name
                stac_item.save_object(dest_href=output_path)
                print(output_path)


if __name__ == "__main__":
    output_folder = Path("data")
    os.makedirs(output_folder, exist_ok=True)
    with rasterio.Env(AWS_NO_SIGN_REQUEST="YES", AWS_REGION="us-west-2"):
        main(output_folder)
