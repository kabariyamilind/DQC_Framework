import yaml
import boto3
# sys.path.append(os.getcwd()+"\\src\\test\\resource")
# print(sys.path)


class ParseConfig():
    def __init__(self, spark):
        self.spark = spark

    def parseYAMLConfig(self, YAMLPath):
        """

        :param YAMLPath:
        :return:
        """
        path_split = YAMLPath.split("/")
        print(path_split[0])
        if path_split[0] == "s3:":
            s3_client = boto3.client('s3', region_name='ap-south-1')
            bucket_name = path_split[2]
            key = "/".join(path_split[3:])
            print(bucket_name,key)
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            yamlConfigs = yaml.safe_load(response["Body"])
        else:
            with open(YAMLPath) as file_data:
                yamlConfigs = yaml.safe_load(file_data)
        return yamlConfigs


    def prepareDictonary(self,yamlConfigs):
        """

        :param yamlConfigs:
        :return:
        """
        dictColMatMapping ={}
        for key in yamlConfigs.keys():
            dictColMatMapping[key] = self.spark._jvm.scala.collection.JavaConversions.iterableAsScalaIterable(yamlConfigs[key]).toSeq()
        return dictColMatMapping
