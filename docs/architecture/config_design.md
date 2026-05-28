{
  "name": "db1-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    "database.hostname": "db1-host",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "cdc_pass",
    "database.dbname": "db1",

    "topic.prefix": "db1",                              # Kafka topic namespace

    "plugin.name": "pgoutput",
    "slot.name": "db1_slot",                            # WAL replication slot

    "table.include.list": "public.orders,public.users", # which tables to track

    "tombstones.on.delete": "false",
    "include.schema.changes": "false",

    "snapshot.mode": "initial"                          # initial sync behavior
  }
}