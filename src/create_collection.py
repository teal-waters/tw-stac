"""Create a pystac Collection for USGS 1/3 arcsecond DEM data."""

from datetime import datetime
from datetime import timezone
from pathlib import Path

from pystac import Collection
from pystac import Extent
from pystac import Provider
from pystac import SpatialExtent
from pystac import Summaries
from pystac import TemporalExtent

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
            roles=["processor", "host"],
            url="https://tealwaters.com",
        ),
        Provider(
            name="USGS",
            roles=["producer", "processor", "licensor"],
            url="https://data.usgs.gov/datacatalog/data/USGS:da4a1ad0-af04-4228-85b7-66e3df87edfe",
        ),
    ],
    summaries=Summaries(
        {
            "gsd": [10],
        },
    ),
)

collection_url = f"data/{usgs_13_collection.id}.json"
usgs_13_collection.validate()
usgs_13_collection.set_self_href(collection_url)
usgs_13_collection.save_object(dest_href=Path("data") / f"{usgs_13_collection.id}.json")
