from depot_client.depot_client import DepotClient, get_depot_secret
from logger.logger import log
from exceptions.data_os_exceptions import DataOSException


def _create_depot_secret_sql(depot_details):
    if depot_details['type'].lower() in ['abfss', "wasbs"]:
        secret = get_depot_secret(depot_details['depot'])
        sql = f"CREATE SECRET azsecret (TYPE AZURE,CONNECTION_STRING 'DefaultEndpointsProtocol=https;" \
              f"AccountName={secret['azurestorageaccountname']};" \
              f"AccountKey={secret['azurestorageaccountkey']};" \
              f"TableEndpoint=https://{secret['azurestorageaccountname']}.table.core.windows.net/;');"
        return sql

    elif depot_details['type'].lower() == 's3':
        secret = get_depot_secret(depot_details['depot'])
        depot_spec = depot_details[depot_details['type']]
        sql = f"""CREATE SECRET awssecret (TYPE S3, KEY_ID '{secret['awsaccesskeyid']}', 
        SECRET '{secret['awssecretaccesskey']}' """
        if 'region' in depot_spec.keys():
            sql += f", REGION '{depot_spec['region']}'"
        if 'endpoint' in depot_spec.keys():
            sql += f", ENDPOINT '{depot_spec['endpoint']}'"
        sql += ");"
        return sql


def _create_dataset_view_sql(depot_details, view_name):
    if depot_details['type'].lower() in ['abfss', "wasbs"]:
        container = depot_details[depot_details['type']]['container']
        relative_path: str = depot_details[depot_details['type']]['relativePath']
        relative_path = relative_path if not relative_path.startswith("/") else relative_path[1:]
        sql = f"""CREATE VIEW {view_name} as (
        SELECT * FROM iceberg_scan("azure://{container}/{relative_path}/{depot_details['collection']}/{depot_details['dataset']}",
        allow_moved_paths=true, metadata_compression_codec="gzip", skip_schema_inference=true));"""
        return sql

    if depot_details['type'].lower() == 's3':
        bucket = depot_details[depot_details['type']]['bucket']
        relative_path = depot_details[depot_details['type']]['relativePath']
        relative_path = relative_path if not relative_path.startswith("/") else relative_path[1:]
        sql = f"""CREATE VIEW {view_name} as (
        SELECT * FROM iceberg_scan("s3://{bucket}/{relative_path}/{depot_details['collection']}/{depot_details['dataset']}",
        metadata_compression_codec="gzip", skip_schema_inference=true));"""
        return sql


class PgDuckMigrations(DepotClient):
    def __init__(self, config: dict, db):
        super().__init__()
        self.resolved_depot = {}
        self.unique_depots = set()
        self.config = config
        self.depot_types = ['abfss', 'wasbs', 's3']
        self.conn = db

    def _depot_check(self):
        if self.config.get("datasets", []) is not None:
            for dataset in self.config.get("datasets", []):
                res = self.resolve(dataset['address'])
                self.resolved_depot[dataset['address']] = res
                if res['type'].lower() not in self.depot_types:
                    log.error(f"depot type - `{res['type']}` not supported.")
                    raise DataOSException(f"depot type - `{res['type']}` not supported.")
                else:
                    if not ('format' in res[res['type']].keys() and res[res['type']]['format'].lower() == 'iceberg'):
                        log.error(f"only iceberg format depot supported.")
                        raise DataOSException(f"only iceberg format depot supported.")
                self.unique_depots.add(res['depot'])

            if len(self.unique_depots) > 1:
                log.error(f"more than one iceberg format depot provide,  only one iceberg depot supported.")
                raise DataOSException(f"more than one iceberg format depot provide,  only iceberg one depot supported.")
        else:
            log.error("`datasets` should have at least one object with keys `name` and `address`.")
            raise DataOSException("`datasets` should have at least one object with keys `name` and `address`.")

    def execute_secret_sql(self):
        self._depot_check()
        is_exist = False
        for dataset in set(self.resolved_depot.keys()):
            depot_details = self.resolved_depot[dataset]
            if is_exist is False:
                try:
                    sql = _create_depot_secret_sql(depot_details)
                    log.debug(sql)
                    res = self.conn.execute(sql).fetchall()
                    log.debug(f"secret created for depot - `{depot_details['depot']}`, response - {res}")
                    is_exist = True
                except Exception as e:
                    log.error(f"secrets not created - `{depot_details['depot']}`, error - {e}")
            else:
                log.debug(f"secret already created for depot - `{depot_details['depot']}`")

    def execute_view_sql(self):
        for dataset in self.config.get("datasets", [{}]):
            depot_details = self.resolved_depot[dataset['address']]
            view_name = dataset['name']
            try:
                sql = _create_dataset_view_sql(depot_details, view_name)
                log.debug(f"executing sql - `{sql}` to create view {view_name}")
                res = self.conn.execute(sql).fetchall()
                log.info(f"view - `{view_name}` created for dataset - `{dataset}`, response - {res}")
            except Exception as e:
                log.error(f"could not create view - `{view_name}` for dataset - `{dataset}`, error - {e}")

    def execute_table_sql(self):
        for sql in self.config.get('sqls', []):
            try:
                res = self.conn.execute(sql).fetchall()
                log.info(f"dataset created using sql `{sql}`, response - {res}")
            except Exception as e:
                log.error(f"could not create dataset using sql - `{sql}`, error - {e}")
