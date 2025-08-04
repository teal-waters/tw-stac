"""Ingest USGS DEM stac collection & items into pgstac database""" """"""

from pathlib import Path
from typing import Iterable

import pystac
from pypgstac.db import PgstacDB
from pypgstac.load import Loader
from pystac import Collection
from pystac import Item

PG_URL = "127.0.0.1:5439"
PG_USER = "username"
PG_PASS = "password"

PG_DB = PgstacDB(f"postgresql://{PG_USER}:{PG_PASS}@{PG_URL}/postgis")


def ingest_collection(collection: Collection, pg_db: PgstacDB = PG_DB) -> None:
    """Ingest a pystac Collection into the given database."""
    loader = Loader(pg_db)
    loader.load_collections(iter([collection.to_dict()]))


def ingest_items(
    items: Iterable[Item], collection: Collection, pg_db: PgstacDB = PG_DB
) -> None:
    """Ingest collection into pgstac database."""
    for item in items:
        item.collection_id = collection.id
        item.add_link(
            pystac.Link(
                pystac.RelType.COLLECTION,
                collection.id,
                media_type=pystac.MediaType.JSON,
            )
        )

    loader = Loader(pg_db)
    loader.load_items(iter([item.to_dict() for item in items]))


def get_items(folder: Path, glob: str) -> list[Item]:
    """Get a list of all items in the given folder & glob."""
    return [Item.from_file(file) for file in folder.glob(glob)]


if __name__ == "__main__":
    collection: Collection = pystac.read_file(Path("data") / "usgs_dem_13.json")  # pyright: ignore
    ingest_collection(collection)
    items = get_items(Path("data"), "USGS_13*.json")
    ingest_items(items, collection)
