
import platform
import os, re
import subprocess
import wget
import staroid
import requests

SPARK_ARTIFACTS={
    "3.0.0": {
        "image": "opendatastudio/spark-py:v3.0.0-staroid",
        "dist": "https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz"
    }
}

class SparkCluster:
    def __init__(self, opends, cluster_name, spark_conf=None, spark_version="3.0.0", spark_home=None):
        self.__opends = opends
        self.__cluster_name = cluster_name
        self.__spark_conf = spark_conf
        self.__spark_version = spark_version
        self.__spark_home = spark_home

    def install(self):
        self.__opends.download_chisel_if_not_exists()

        if self.__spark_home == None:
            # download spark if spark_home is not set
            cache_dir = self.__opends.create_or_get_cache_dir()

            # check if spark is already downloaded
            spark_home = "{}/{}".format(cache_dir, os.path.basename(SPARK_ARTIFACTS[self.__spark_version]["dist"]).replace(".tgz", ""))
            if not os.path.exists(spark_home):
                # download spark distribution
                download_url = SPARK_ARTIFACTS[self.__spark_version]["dist"]
                filename = wget.download(download_url, cache_dir)

                # extract
                subprocess.run(["tar", "-xzf", filename, "-C", cache_dir])

                # remove dist file after extract
                subprocess.run(["rm", "-f", "{}/{}".format(cache_dir, os.path.basename(SPARK_ARTIFACTS[self.__spark_version]["dist"]))])
            
            self.__spark_home = spark_home

        import findspark
        findspark.init(self.__spark_home)

    def create_cluster(self):
        # create cluster
        org = self.__opends.__staroid.get_org()

        pass

    def start_cluster(self):
        # create start namespace
        # start tunnel service
        pass

    def open_tunnel(self):
        pass

    def create_spark_session(self):
        return None
