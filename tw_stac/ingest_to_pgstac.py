"""Functions to ingest STAC collections and items into a Postgresql STAC database."""

import json

import obstore
import pystac
import typer
from pypgstac.db import PgstacDB
from pypgstac.load import Loader
from pystac import Collection
from pystac import Item

CONTAINER = "tw-staging"
ACCOUNT_NAME = "tealwaters"
PG_DB = PgstacDB()

app = typer.Typer()


@app.command()
def ingest_collection(collection_url: str) -> None:
    """Load a collection into a PGStacDB.

    Args:
        collection_url: The url of the collection json file.
        db: The database.
    """
    collection = Collection.from_file(collection_url)
    loader = Loader(PG_DB)
    loader.load_collections(iter([collection.to_dict()]))


@app.command()
def load_and_ingest_items(
    prefix: str,
    collection_url: str,
    container: str = CONTAINER,
    account_name: str = ACCOUNT_NAME,
) -> None:
    """Load items and ingest into the STAC database.

    Args:
        prefix: The folder prefix for the items.
        collection_url: The URL to the corresponding collection.
        container: The blob storage container that holds the items.
        account_name: The account name which holds the container.

    """
    items = load_items(container, prefix, account_name)
    collection = Collection.from_file(collection_url)
    return ingest_items(items, collection)


def load_items(
    container: str, prefix: str, account_name: str = ACCOUNT_NAME
) -> list[Item]:
    """Get a list all STAC items.

    Args:
        prefix: The prefix or folder which has items we wish to collect.
        container: The container name.
        account_name: The account name.

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
                loaded = json.loads(store.get(file["path"]).bytes().to_bytes())
                # Weak check to see if this is actually an item before loading.
                if not loaded.get("type") == "Collection":
                    items.append(Item.from_dict(loaded))
    if len(items) == 0:
        raise Exception("No STAC Items found")
    return items


def ingest_items(
    items: list[Item], collection: Collection, db: PgstacDB = PG_DB
) -> None:
    """Ingest items into pgstac database.

    Args:
        items: A list of STAC items.
        collection: The collections in which to place the items
        pg_db: The database.
    """
    for item in items:
        item.collection_id = collection.id
        item.add_link(
            pystac.Link(
                pystac.RelType.COLLECTION,
                collection.id,
                media_type=pystac.MediaType.JSON,
            )
        )

    loader = Loader(db)
    loader.load_items(iter([item.to_dict() for item in items]))


if __name__ == "__main__":
    app()
