"""Ingest USGS DEM stac collection & items into pgstac database."""

import json

import obstore
import pystac
import typer
from pypgstac.db import PgstacDB
from pypgstac.load import Loader
from pystac import Collection
from pystac import Item

PG_DB = PgstacDB()

app = typer.Typer()


@app.command()
def ingest_collection(url: str, pg_db: PgstacDB = PG_DB) -> None:
    """Ingest a pystac Collection into the given database."""
    collection = Collection.from_file(url)
    loader = Loader(pg_db)
    loader.load_collections(iter([collection.to_dict()]))


@app.command()
def load_and_ingest_items(
    container: str,
    prefix: str,
    account_name: str,
    collection_url: str,
    pg_db: PgstacDB = PG_DB,
) -> None:
    """Load items in the given container & prefix and ingest into the database.

    Args:
        container:
        prefix:
        account_name:
        collection_url:
        pg_db:

    """
    items = get_items(container, prefix, account_name)
    collection = Collection.from_file(collection_url)
    return ingest_items(items, collection, pg_db)


def ingest_items(
    items: list[Item], collection: Collection, pg_db: PgstacDB = PG_DB
) -> None:
    """Ingest items into pgstac database."""
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


def get_items(
    container: str, prefix: str, account_name: str = "tealwaters"
) -> list[Item]:
    """Get a list all STAC items.

    Args:
        container: The container name.
        prefix: The prefix or folder which has items we wish to collect.
        account_name: The account name

    Returns:
        A list of STAC Items.

    Raises:
        Exception: If there are no items.
    """
    store = obstore.store.AzureStore(
        container_name=container, prefix=prefix, account_name=account_name
    )

    items = []
    for chunk in store.list(chunk_size=1000):
        for file in chunk:
            if file["path"].endswith(".json"):
                item = Item.from_dict(
                    json.loads(store.get(file["path"]).bytes().to_bytes())
                )
                items.append(item)
    if len(items) == 0:
        raise Exception("No STAC Items found")
    return items


if __name__ == "__main__":
    app()
