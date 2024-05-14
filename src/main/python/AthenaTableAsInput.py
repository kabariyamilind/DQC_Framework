from src.main.python.InputSourceAbstract import InputSourceAbstract


class AthenaTableAsInput(InputSourceAbstract):

    def readInputDataCreateSparkDF(self, spark, config,params):
        # db = config['input_data_params']['db']
        schema_name = config['input_data_params']['schema_name']
        table_name = config['input_data_params']['table_name']

        spark.sql("use {0}".format(schema_name))
        # spark.sql("show tables".format(table_name)).show(truncate=False)
        select_cond = "select * from {0}.{1} where {2}>=\'{3}\' and {2}<\'{4}\' and {2} not like '%â¬%'" \
            .format(schema_name, table_name, params['filterColumnName'], str(params['start_date']),
                    str(params['end_date']))

        df_athena = spark.sql(select_cond)

        return df_athena

class AthenaMericsTableRead():
    def readMetricsTable(self,spark,table_name):

        select_cond = 'select * from development.{}'.format(table_name)
        df_metrics = spark.sql(select_cond)
        return df_metrics
