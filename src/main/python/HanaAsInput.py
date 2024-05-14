from src.main.python.InputSourceAbstract import InputSourceAbstract
from src.main.utils.Connection import Connection
import json

class HanaAsInput(InputSourceAbstract):

    def replace_query_params(self,query, params):
        # Replace each placeholder in the query with the corresponding value from the dictionary
        for key, value in params.items():
            placeholder = "{" + key + "}"
            query = query.replace(placeholder, str(value))

        return query
    def readInputDataCreateSparkDF(self, spark, config,params):
        """

        :param spark:
        :param config:
        :param params:
        :return:
        """
        # db = config['input_data_params']['db']
        # schema_name = config['input_data_params']['schema_name']
        # table_name = config['input_data_params']['table_name']
        # print("select * from {0}.{1} where {2}>=\'{3}\' and {2}<\'{4}\'" \
        #       .format(schema_name, table_name, params['filterColumnName'], str(params['start_date']),
        #               str(params['end_date'])))
        # user, passwd, host, port = redshiftConnection.getRedshiftConnectionParams()

        connection = Connection(spark)
        user, passwd, host, port = connection.getHanaConnectionParams()
        db = config['input_data_params']['db']
        if config['input_data_params']['custom_query_to_run'].upper()=='NO':
            schema_name = config['input_data_params']['schema_name']
            table_name = config['input_data_params']['table_name']
            print("select * from {0}.{1} where {2}>=\'{3}\' and {2}<\'{4}\'"\
                .format(schema_name,table_name,params['filterColumnName'],str(params['start_date']),str(params['end_date'])))
            select_cond = "select * from {0}.{1} where {2}>=\'{3}\' and {2}<\'{4}\'"\
                .format(schema_name,table_name,params['filterColumnName'],str(params['start_date']),str(params['end_date']))


            inputDf = spark.read.format("jdbc").option("driver","com.sap.db.jdbc.Driver") \
                .option("url", "jdbc:sap://{0}:{1}/{2}?user={3}&password={4}".format(host, port, db,
                                                                                          user, passwd)) \
                .option("query", select_cond) \
                .option("tempdir", "s3://-dev/Milind/tmp/") \
                .option("aws_iam_role", "arn:aws:iam:::role/Role_for_reading") \
                .load()
            #.option("forward_spark_s3_credentials", "true") \
            # inputDf.write.mode("overwrite").parquet("s3://tuldl-dev/Milind/tempdata/")
            # inputDf1 = spark.read.parquet("s3://tuldl-dev/Milind/tempdata/")
            #     # \.
            print(inputDf.count())
        else:
            query = config['input_data_params']['custom_query']
            query_param = config['input_data_params']['custom_query_params']
            print(query,query_param)
            query_param['start_date'] = '\''+str(params['start_date'])+'\''
            query_param['end_date'] = '\''+str(params['end_date'])+'\''
            query_param['data_date'] = '\'' + str(params['data_date']) + '\''
            formatted_query = self.replace_query_params(query, query_param)
            print(formatted_query)
            inputDf = spark.read.format("jdbc").option("driver", "com.sap.db.jdbc.Driver") \
                .option("url", "jdbc:sap://{0}:{1}/{2}?user={3}&password={4}".format(host, port, db,
                                                                                     user, passwd)) \
                .option("query", formatted_query) \
                .option("tempdir", "s3://-dev/Milind/tmp/") \
                .option("aws_iam_role", "arn:aws:iam:::role/Role_for_reading") \
                .load()

        return inputDf