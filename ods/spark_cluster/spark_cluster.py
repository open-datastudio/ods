
import platform
import os, re, time, sys
import subprocess
import wget
import staroid
import requests
from kubernetes import client

SPARK_ARTIFACTS={
    "3.0.0": {
        "image": "opendatastudio/spark-py:v3.0.0-staroid-20200830-01",
        "dist": "https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz",
        "commit_url": "GITHUB/open-datastudio/spark-serverless:master",
        "jars": {
            "hadoop-aws": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar",
            "aws-java-sdk": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar"
        }
    },
    "2.4.6": {
        "image": "opendatastudio/spark-py:v2.4.6-staroid",
        "dist": "https://downloads.apache.org/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz",
        "commit_url": "GITHUB/open-datastudio/spark-serverless:master"
    }
}

SPARK_IMAGE_PYTHON_PATH={
    "3.6": "/home/spark/.pyenv/versions/3.6.9/bin/python3",
    "3.7": "/home/spark/.pyenv/versions/3.7.7/bin/python3",
    "3.8": "/home/spark/.pyenv/versions/3.8.1/bin/python3"
}

class SparkCluster:
    def __init__(
        self,
        opends,
        cluster_name,
        spark_conf=None,
        spark_version="3.0.0",
        spark_home=None,
        worker_num=1,
        worker_type="standard-4",
        worker_isolation="dedicated",
        delta=False,
        aws=True
    ):
        self.__opends = opends
        self.__cluster_name = cluster_name
        self.__spark_conf = spark_conf
        self.__spark_version = spark_version
        self.__spark_home = spark_home
        self.__worker_num = worker_num
        self.__worker_type = worker_type
        self.__worker_isolation = worker_isolation
        self.__delta = delta
        self.__aws = aws

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

        # download additional jars
        if self.__aws:
            self.__download_jar("{}/jars".format(self.__spark_home), SPARK_ARTIFACTS[self.__spark_version]["jars"]["hadoop-aws"])
            self.__download_jar("{}/jars".format(self.__spark_home), SPARK_ARTIFACTS[self.__spark_version]["jars"]["aws-java-sdk"])

        import findspark
        findspark.init(self.__spark_home)
        return self

    def __download_jar(self, download_dir, url):
        "download file to download_dir if not exists"

        filename = os.path.basename(url)
        if not os.path.exists("{}/{}".format(download_dir, filename)):
            wget.download(url, download_dir)

    def start(self):
        "Start cluster"

        # run previous steps
        self.install()

        local_kube_api_port = 8001
        local_kube_api_addr = "http://localhost:{}".format(local_kube_api_port)

        # create start namespace
        commit_url = SPARK_ARTIFACTS[self.__spark_version]["commit_url"]
        ns = self.__opends._start_instance_on_staroid(self.__cluster_name, commit_url)
        self.__opends._start_tunnel(
            self.__cluster_name,
            [
                "{}:localhost:57683".format(local_kube_api_port), # kubernetes api
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
                r = requests.get("{}/version".format(local_kube_api_addr), timeout=(3, 5))
                if r.status_code == 200:
                    established = True
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(3)

        if established:
            # create kube client
            kube_conf = client.Configuration()
            kube_conf.host = local_kube_api_addr
            kube_client = client.ApiClient(kube_conf)
            v1 = client.CoreV1Api(kube_client)

            driver_svc_name = "driver-{}".format(self.__cluster_name)
            sparkui_svc_name = "spark-ui-{}".format(self.__cluster_name)

            driver_svc_exists = False
            sparkui_svc_exists = False

            self.__ns = ns
            self.__kube_client = kube_client
            self.__sparkui_svc_name = sparkui_svc_name
            self.__driver_svc_name = driver_svc_name

            # create driver service
            service_list = v1.list_namespaced_service(namespace=ns.namespace())
            for svc in service_list.items:
                if svc.metadata.name == driver_svc_name:
                    driver_svc_exists = True
                if svc.metadata.name == sparkui_svc_name:
                    sparkui_svc_exists = True
                    
            if not driver_svc_exists:
                driver_svc = client.V1Service(
                    api_version="v1",
                    kind="Service",
                    metadata=client.V1ObjectMeta(name=driver_svc_name),
                    spec=client.V1ServiceSpec(
                        selector={"resource.staroid.com/system": "shell"},
                        ports=[
                            client.V1ServicePort(port=22321, name="driver"),
                            client.V1ServicePort(port=22322, name="blockmanager")
                        ]
                    )
                )
                v1.create_namespaced_service(namespace=ns.namespace(), body=driver_svc)

            # create spark-ui service
            if not sparkui_svc_exists:
                spark_ui_svc = client.V1Service(
                    api_version="v1",
                    kind="Service",
                    metadata=client.V1ObjectMeta(
                        name=sparkui_svc_name,
                        annotations={"service.staroid.com/link": "show"}
                    ),
                    spec=client.V1ServiceSpec(
                        selector={"resource.staroid.com/system": "shell"},
                        ports=[
                            client.V1ServicePort(port=4040, name="spark-ui")
                        ]
                    )
                )
                v1.create_namespaced_service(namespace=ns.namespace(), body=spark_ui_svc)

            return ns
        else:
            return None

    def stop(self):
        # remove services
        if hasattr(self, '__kube_client'):
            v1 = client.CoreV1Api(self.__kube_client)
            service_list = v1.list_namespaced_service(namespace=ns.namespace())
            for svc in service_list.items:
                if svc.metadata.name == self.__driver_svc_name:
                    v1.delete_namespaced_service(namespace=self.__ns.namespace(), name=self.__driver_svc_name)
                    self.__driver_svc_name = None
                if svc.metadata.name == self.__sparkui_svc_name:
                    v1.delete_namespaced_service(namespace=self.__ns.namespace(), name=self.__sparkui_svc_name)
                    self.__sparkui_svc_name = None

        self.__opends._stop_tunnel(self.__cluster_name)
        self.__opends._stop_instance_on_staroid(self.__cluster_name)

    def delete(self):
        self.stop()
        self.__opends._delete_instance_on_staroid(self.__cluster_name)

    def __spark_major_version(self):
        return int(self.__spark_version[:1])

    def session(self):
        "Get spark session"

        ns = self.start()

        executor_python_path = None
        if sys.version_info >= (3, 8) and sys.version_info < (3, 9):
            executor_python_path = SPARK_IMAGE_PYTHON_PATH["3.8"]
        elif sys.version_info >= (3, 7) and sys.version_info < (3, 8):
            executor_python_path = SPARK_IMAGE_PYTHON_PATH["3.7"]
        elif sys.version_info >= (3, 6) and sys.version_info < (3, 7):
            executor_python_path = SPARK_IMAGE_PYTHON_PATH["3.6"]
        else:
            raise Exception("Current python version is not supported. Supported versions are 3.6, 3.7, 3.8")

        os.environ["PYSPARK_PYTHON"] = executor_python_path
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        from pyspark.sql import SparkSession

        jars_packages = []
        session_builder = SparkSession.builder \
            .appName(self.__cluster_name) \
            .config("spark.master", "k8s://http://localhost:8001") \
            .config("spark.kubernetes.namespace", ns.namespace()) \
            .config("spark.kubernetes.container.image", SPARK_ARTIFACTS[self.__spark_version]["image"]) \
            .config("spark.driver.host", "driver-{}".format(self.__cluster_name)) \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.port", 22321) \
            .config("spark.blockManager.port", 22322) \
            .config("spark.scheduler.mode", "FAIR") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.dynamicAllocation.enabled ", True) \
            .config("spark.dynamicAllocation.minExecutors", 1) \
            .config("spark.dynamicAllocation.maxExecutors", 100) \
            .config("spark.dynamicAllocation.executorIdleTimeout", "600s") \
            .config("spark.dynamicAllocation.schedulerBacklogTimeout", "60s") \
            .config("spark.kubernetes.allocation.batch.size ", 20) \
            .config("spark.dynamicAllocation.initialExecutors", self.__worker_num) \
            .config("spark.executor.instances", self.__worker_num)

        if self.__spark_major_version() == 3:
            session_builder = session_builder.config("spark.dynamicAllocation.shuffleTracking.enabled", True) \
                .config("spark.sql.adaptive.enabled", True) \
                .config("spark.sql.adaptive.coalescePartitions.enabled", True)

            if self.__delta:
                session_builder = session_builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                jars_packages.append("io.delta:delta-core_2.12:0.7.0")

        executor_conf = self._gen_executor_conf(self.__worker_type, self.__worker_num, self.__worker_isolation)
        for k, v in executor_conf.items():
            session_builder = session_builder.config(k, v)

        if self.__spark_conf != None:
            for k, v in self.__spark_conf.items():
                if k == "spark.jars.packages":
                    jars_packages.append(v)
                else:
                    session_builder = session_builder.config(k, v)

        if len(jars_packages) > 0:
            session_builder = session_builder.config("spark.jars.packages", ",".join(jars_packages))

        spark = session_builder.getOrCreate()
        return spark

    def _gen_executor_conf(self, worker_type, worker_num, worker_isolation):
        cores = 1
        memory = "4g"

        if worker_type == "standard-2":
            cores = 2
            memory = "8g"
        elif worker_type == "standard-4":
            cores = 4
            memory = "16g"
        elif worker_type == "standard-8":
            cores = 8
            memory = "32g"

        conf = {
            "spark.executor.cores": cores,
            "spark.executor.memory": memory,
            "spark.kubernetes.executor.limit.cores": cores,
            "spark.kubernetes.executor.label.pod.staroid.com/disk": "ssd",
            "spark.kubernetes.executor.label.pod.staroid.com/instance-type": worker_type,
            "spark.kubernetes.executor.label.pod.staroid.com/isolation": worker_isolation,
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "20s"
        }
        return conf
