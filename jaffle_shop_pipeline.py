"""Optimized Jaffle Shop data pipeline using dlt.

This module contains an optimized data pipeline for extracting orders data from the
Jaffle Shop API. The pipeline includes performance optimizations such as:
- Large page sizes (10,000 records per request)
- Connection pooling and retry logic
- Efficient batching and filtering
- Incremental loading based on ordered_at timestamp
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from dlt.sources.helpers.requests import Client


@dlt.source
def jaffle_shop_source():
    """Optimized dlt source for extracting Jaffle Shop orders data.

    This source extracts orders data from the Jaffle Shop API with performance optimizations
    including large page sizes, connection pooling, retries, and efficient batching.

    Returns:
        List[dlt.Resource]: A list containing the orders resource.
    """
    session = Client(
        request_timeout=60,
        max_connections=20,
        request_max_attempts=3,
        raise_for_status=False,
    ).session

    client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1", session=session
    )
    paginator = HeaderLinkPaginator()

    @dlt.resource(
        primary_key="id",
        write_disposition="append",
        parallelized=True,  # Enable parallel extraction
    )
    def orders(
        updated_at=dlt.sources.incremental("ordered_at", initial_value="2017-08-01"),
    ):
        """Extract orders data with incremental loading and filtering.

        Extracts orders from the Jaffle Shop API, filtering for orders with total <= $500
        and using incremental loading based on the ordered_at timestamp.

        Args:
            updated_at (dlt.sources.incremental): Incremental loading state tracking
                the last processed ordered_at timestamp. Defaults to "2017-08-01".

        Yields:
            List[Dict[str, Any]]: Batches of filtered order records. Each batch contains
                up to 500 orders with order_total <= 500.
        """
        params = {"page_size": 10_000}
        if updated_at.last_value:
            params["since"] = updated_at.last_value

        batch = []
        batch_size = 500

        for page in client.paginate("/orders", paginator=paginator, params=params):
            filtered_items = [
                item for item in page if int(item.get("order_total", 0)) <= 500
            ]
            batch.extend(filtered_items)

            if len(batch) >= batch_size:
                yield batch[:batch_size]
                batch = batch[batch_size:]

        if batch:
            yield batch

    return [orders]


if __name__ == "__main__":
    """Main execution block for running the Jaffle Shop pipeline.
    
    Creates and runs a dlt pipeline that extracts orders data from the Jaffle Shop API
    and loads it into a DuckDB database. The pipeline uses the 'jaffle_shop_optimized'
    name and stores data in the 'jaffle_data' dataset.
    """
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_optimized",
        destination="duckdb",
        dataset_name="jaffle_data",
    )
    info = pipeline.run(jaffle_shop_source())
    print(info)
