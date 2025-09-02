"""Ingest USGS DEM 1/3 arcsecond collection to STAC DB on Azure."""

from tw_stac.kubernetes import run_command_on_stac_pod


def main() -> None:
    """Ingest the collection and items from blob storage."""
    command = """rm -rf tw-stac && \
    git clone https://github.com/teal-waters/tw-stac.git && \
    cd tw-stac && \
    pip install . && \
    export COLLECTION_URL=https://tealwaters.blob.core.windows.net/tw-staging/usgs/dem/13/usgs_dem_13.json && \
    python tw_stac/ingest_to_pgstac.py ingest-collection $COLLECTION_URL && \
    python tw_stac/ingest_to_pgstac.py load-and-ingest-items usgs/dem/13 $COLLECTION_URL
    """
    run_command_on_stac_pod(command)


if __name__ == "__main__":
    main()
