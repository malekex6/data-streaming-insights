"""The table options must be accepted by Beam's own table builder.

Beam creates staging tables with bigquery.Table(**additional_bq_parameters)
(bigquery_tools._create_table). A wrong type there (e.g. expirationMs as a
string) only fails at run time, on the first write, so check it here.
"""

import pytest
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery

from dataflow.schemas import bq_schemas as s

TABLES = [
    (s.BRONZE_SCHEMA, s.BRONZE_PARAMS),
    (s.DEAD_LETTER_SCHEMA, s.DEAD_LETTER_PARAMS),
    (s.SILVER_STAGING_SCHEMA, s.SILVER_STAGING_PARAMS),
    (s.SALES_PER_MINUTE_SCHEMA, s.GOLD_STAGING_PARAMS),
    (s.REGION_ROLLING_SCHEMA, s.GOLD_STAGING_PARAMS),
    (s.USER_SESSIONS_SCHEMA, s.GOLD_STAGING_PARAMS),
]


@pytest.mark.parametrize("schema,params", TABLES)
def test_beam_can_build_the_table(schema, params):
    table = bigquery.Table(
        tableReference=bigquery.TableReference(projectId="p", datasetId="d", tableId="t"),
        schema=bigquery_tools.get_bq_tableschema(schema),
        **params)
    assert table.timePartitioning.type == "DAY"
    assert table.timePartitioning.expirationMs > 0
