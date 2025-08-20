"""Ingest USGS DEM 1/3 arcsecond collection to STAC DB on Azure."""

from tw_stac.kubernetes import run_command_on_stac_pod


def main() -> None:
    """Ingest the collection and items from blob storage."""
    command = """git clone git@github.com/teal-waters/tw_stac.git \
    cd tw_stac \
    pip install . \
    export COLLECTION_URL = https://tealwaters.blob.core.windows.net/tw-staging/usgs/dem/13/USGS_dem_13.json \
    python tw_stac.ingest_to_pgstac ingest_collection $COLLECTION_URL \
    python tw_stac.ingest_to_pgstac load_and_ingest_items --prefix usgs/dem/13 --collection_url $COLLECTION_URL
    """
    run_command_on_stac_pod(command)


if __name__ == "__main__":
    main()
