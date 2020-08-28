
import platform
import os, re, time
import subprocess
import wget
import staroid
import requests

SPARK_ARTIFACTS={
    "3.0.0": {
        "image": "opendatastudio/spark-py:v3.0.0-staroid",
        "dist": "https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz",
        "commit_url": "GITHUB/open-datastudio/spark-serverless:master"
    },
    "2.4.6": {
        "image": "opendatastudio/spark-py:v2.4.6-staroid",
        "dist": "https://downloads.apache.org/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz",
        "commit_url": "GITHUB/open-datastudio/spark-serverless:master"
    }
}

class SparkCluster:
    def __init__(
        self,
        opends,
        cluster_name,
        spark_conf=None,
        spark_version="3.0.0",
        spark_home=None,
        worker_num=1
    ):
        self.__opends = opends
        self.__cluster_name = cluster_name
        self.__spark_conf = spark_conf
        self.__spark_version = spark_version
        self.__spark_home = spark_home
        self.__worker_num = worker_num

    def install(self):
        "Install"
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
        return self

    def start(self):
        "Start cluster"

        # run previous steps
        self.install()

        # create start namespace
        commit_url = SPARK_ARTIFACTS[self.__spark_version]["commit_url"]
        ns = self.__opends._start_instance_on_staroid(self.__cluster_name, commit_url)
        self.__opends._start_tunnel(
            self.__cluster_name,
            [
                "8001:localhost:57683", # kubernetes api
                "R:22321:0.0.0.0:22321",
                "R:22322:0.0.0.0:22322",
                "R:4040:0.0.0.0:4040"
            ]
        )

        # wait for tunnel to be established by checking localhost:8001/version
        start_time = time.time()
        timeout = 300
        established = False
        while time.time() - start_time < timeout:
            try:
                r = requests.get("http://localhost:8001/version", timeout=(3, 5))
                if r.status_code == 200:
                    established = True
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(3)

        if established:
            return ns
        else:
            return None

    def stop(self):
        self.__opends._stop_tunnel(self.__cluster_name)
        self.__opends._stop_instance_on_staroid(self.__cluster_name)

    def delete(self):
        self.stop()
        self.__opends._delete_instance_on_staroid(self.__cluster_name)

    def session(self):
        "Get spark session"

        ns = self.start()

        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName(self.__cluster_name) \
            .config("spark.master", "k8s://http://localhost:8001") \
            .config("spark.kubernetes.namespace", ns.namespace()) \
            .config("spark.kubernetes.container.image", SPARK_ARTIFACTS[self.__spark_version]["image"]) \
            .config("spark.driver.port", 22321) \
            .config("spark.blockManager.port", 22322) \
            .config("spark.executor.instances", self.__worker_num) \
            .getOrCreate()
        return spark
