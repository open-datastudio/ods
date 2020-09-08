from .spark_cluster import SparkCluster
from staroid import Staroid

import requests
import os, stat, time
from pathlib import Path

class Ods:
    def __init__(self, staroid=None, ske=None, cache_dir=None):
        self.__ske = None

        if staroid == None:
            self._staroid = Staroid()
        else:
            self._staroid = staroid

        if cache_dir == None:
            self.__cache_dir = "{}/.ods".format(str(Path.home()))
        else:        
            self.__cache_dir = cache_dir

        # configure from env var
        if "STAROID_SKE" in os.environ:
            self.__ske = os.environ["STAROID_SKE"]

        # configure from args
        if ske != None:
            self.__ske = ske

    def create_or_get_cache_dir(self, module = ""):
        "create (if not exists) or return cache dir path for module"
        cache_dir = "{}/{}".format(self.__cache_dir, module)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        return cache_dir

    def download_chisel_if_not_exists(self):
        self._staroid.get_chisel_path()

    def _start_instance_on_staroid(self, instance_name, commit_url):
        cluster = self._staroid.cluster().get(self.__ske)
        if cluster == None:
            raise Exception("Can't get ske cluster")

        ns_api = self._staroid.namespace(cluster)
        ns = ns_api.create(instance_name, commit_url)
        if ns == None:
            raise Exception("Can't create instance")

        # if instnace is stopped, restart
        if ns.status() == "PAUSE":
            ns_api.start(instance_name)

        # wait for phase to become RUNNING
        return self.__wait_for_ns_phase(ns_api, ns, "RUNNING", 600)

    def _start_tunnel(self, instance_name, tunnels):
        cluster = self._staroid.cluster().get(self.__ske)
        if cluster == None:
            raise Exception("Can't get ske cluster")
        ns_api = self._staroid.namespace(cluster)
        ns = ns_api.get(instance_name)
        ns_api.shell_start(instance_name)
        ns_api.start_tunnel(instance_name, tunnels)

    def _stop_tunnel(self, instance_name):
        cluster = self._staroid.cluster().get(self.__ske)
        if cluster == None:
            raise Exception("Can't get ske cluster")
        ns_api = self._staroid.namespace(cluster)
        ns_api.stop_tunnel(instance_name)
        ns_api.shell_stop(instance_name)

    def _stop_instance_on_staroid(self, instance_name):
        cluster = self._staroid.cluster().get(self.__ske)
        if cluster == None:
            raise Exception("Can't get ske cluster")
        ns_api = self._staroid.namespace(cluster)
        ns = ns_api.stop(instance_name)
        ns = self.__wait_for_ns_phase(ns_api, ns, "PAUSED", 600)
        return ns
    
    def _delete_instance_on_staroid(self, instance_name):
        cluster = self._staroid.cluster().get(self.__ske)
        if cluster == None:
            raise Exception("Can't get ske cluster")
        ns_api = self._staroid.namespace(cluster)
        ns = ns_api.delete(instance_name)
        ns = self.__wait_for_ns_phase(ns_api, ns, "REMOVED", 600)
        
    def __wait_for_ns_phase(self, ns_api, ns, phase, timeout):
        start_time = time.time()
        sleep_time = 1
        max_sleep_time = 7

        while ns.phase() != phase:
            if time.time() - start_time > timeout:
                raise Exception("Timeout")

            # sleep
            time.sleep(sleep_time)
            if sleep_time < max_sleep_time:
                sleep_time += 1

            # check
            ns = ns_api.get_by_id(ns.id())
        return ns

__singleton = {}

def init(ske=None, reinit=True):
    if "instance" not in __singleton or reinit:
        __singleton["instance"] = Ods(ske=ske)
    return __singleton["instance"]

def spark(
        name,
        spark_conf=None,
        spark_version="3.0.1",
        spark_home=None,
        worker_num=1,
        worker_type="standard-4",
        worker_isolation="dedicated",
        delta=False,
        aws=True):
    init(reinit=False)

    cluster = SparkCluster(
        __singleton["instance"],
        name,
        spark_conf=spark_conf,
        spark_version=spark_version,
        spark_home=spark_home,
        worker_num=worker_num,
        worker_type=worker_type,
        worker_isolation=worker_isolation,
        delta=delta,
        aws=aws)
    return cluster
