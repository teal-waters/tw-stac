"""Create a pystac Collection for USGS 1/3 arcsecond DEM data."""

import json
from datetime import datetime
from datetime import timezone
from pathlib import Path

import obstore
from pystac import Collection
from pystac import Extent
from pystac import Provider
from pystac import SpatialExtent
from pystac import Summaries
from pystac import TemporalExtent
from pystac.provider import ProviderRole


def create_collection(output_folder: Path) -> None:
    """Create USGS DEM collection."""
    extent = Extent(
        SpatialExtent([[-180, -90, 180, 90]]),
        TemporalExtent([[datetime(1980, 1, 1, 0, 0, 0, 0, timezone.utc), None]]),
    )

    usgs_13_collection = Collection(
        id="usgs_dem_13",
        description="USGS 1/3 arcsecond DEM",
        title="1/3 arcsecond DEM",
        extent=extent,
        license="public-domain",
        keywords=["Elevation"],
        providers=[
            Provider(
                name="TealWaters",
                roles=[ProviderRole.PROCESSOR, ProviderRole.HOST],
                url="https://tealwaters.com",
            ),
            Provider(
                name="USGS",
                roles=[
                    ProviderRole.PRODUCER,
                    ProviderRole.PROCESSOR,
                    ProviderRole.LICENSOR,
                ],
                url="https://data.usgs.gov/datacatalog/data/USGS:da4a1ad0-af04-4228-85b7-66e3df87edfe",
            ),
        ],
        summaries=Summaries(
            {
                "gsd": [10],
            },
        ),
    )

    storage_account = "tealwaters"
    container = "tw-staging"
    output_path = output_folder / f"{usgs_13_collection.id}.json"
    collection_url = (
        f"https://{storage_account}.blob.core.windows.net/{container}/{output_path}"
    )
    usgs_13_collection.validate()
    usgs_13_collection.set_self_href(collection_url)
    output_store = obstore.store.from_url(
        f"az://{container}", account_name=storage_account
    )
    collection_json = json.dumps(usgs_13_collection.to_dict(), indent=2).encode("utf-8")
    output_store.put(str(output_path), collection_json)
    print(collection_url)


if __name__ == "__main__":
    output_folder = Path("usgs/dem/13")
    create_collection(output_folder)
