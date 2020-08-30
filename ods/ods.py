from .spark_cluster import SparkCluster
from staroid import Staroid

import requests
import os, stat, time
from pathlib import Path
from shutil import which
import platform
import subprocess
import wget
import atexit

CHISEL_VERSION="1.6.0"
CHISEL_ARCH_MAP={
    "x86_64": "amd64",
    "i386": "386"
}

class Ods:
    def __init__(self, staroid=None, ske=None, cache_dir=None, chisel_path=None):
        self.__tunnel_processes = {}
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

        self.__chisel_path = chisel_path

    def create_or_get_cache_dir(self, module = ""):
        "create (if not exists) or return cache dir path for module"
        cache_dir = "{}/{}".format(self.__cache_dir, module)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        return cache_dir

    def __check_cmd(self, cmd):
        if which(cmd) == None:
            raise Exception("'{}' command not found".format(cmd))

    def download_chisel_if_not_exists(self):
        # check gunzip available
        self.__check_cmd("gunzip")

        if self.__chisel_path == None:
            # download chisel binary for secure tunnel if not exists
            uname = platform.uname()
            uname.system.lower()
            if uname.machine not in CHISEL_ARCH_MAP.keys():
                raise Exception("Can not download chisel automatically. Please download manually from 'https://github.com/jpillora/chisel/releases/download/v{}' and set 'chisel_path' argument".format(CHISEL_VERSION))

            download_url = "https://github.com/jpillora/chisel/releases/download/v{}/chisel_{}_{}_{}.gz".format(
                CHISEL_VERSION, CHISEL_VERSION, uname.system.lower(), CHISEL_ARCH_MAP[uname.machine])
            cache_bin = self.create_or_get_cache_dir("bin")
            chisel_path = "{}/chisel".format(cache_bin)

            if not os.path.exists(chisel_path):
                # download
                filename = wget.download(download_url, cache_bin)

                # extract
                subprocess.run(["gunzip", "-f", filename])

                # rename
                subprocess.run(["mv", filename.replace(".gz", ""), chisel_path])

                # chmod
                os.chmod(chisel_path, stat.S_IRWXU)

            self.__chisel_path = chisel_path

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

    def _is_tunnel_running(self, instance_name):
        if instance_name in self.__tunnel_processes:
            p = self.__tunnel_processes[instance_name]
            p.poll()
            return p.returncode == None
        else:
            return False

    def _start_tunnel(self, instance_name, tunnels):
        if self._is_tunnel_running(instance_name):
            return

        cluster = self._staroid.cluster().get(self.__ske)
        if cluster == None:
            raise Exception("Can't get ske cluster")
        ns_api = self._staroid.namespace(cluster)
        ns = ns_api.get(instance_name)
        ns_api.shell_start(instance_name)
        resources = ns_api.get_all_resources(instance_name)

        shell_service = None
        for s in resources["services"]["items"]:
            if "labels" in s["metadata"]:
                if "resource.staroid.com/system" in s["metadata"]["labels"]:
                    if s["metadata"]["labels"]["resource.staroid.com/system"] == "shell":
                        shell_service = s
                        break                        

        if shell_service == None:
            raise Exception("Shell service not found")

        tunnel_server = "https://p{}-{}--{}".format("57682", shell_service["metadata"]["name"], ns.url()[len("https://"):])
        cmd = [
            self.__chisel_path,
            "client",
            "--header",
            "Authorization: token {}".format(self._staroid.get_access_token()),
            "--keepalive",
            "10s",
            tunnel_server
        ]
        cmd.extend(tunnels)
        self.__tunnel_processes[instance_name]=subprocess.Popen(cmd)
        atexit.register(self.cleanup)

    def cleanup(self):
        timeout_sec = 5
        for p in self.__tunnel_processes.values(): # list of your processes
            p_sec = 0
            for second in range(timeout_sec):
                if p.poll() == None:
                    time.sleep(1)
                    p_sec += 1
            if p_sec >= timeout_sec:
                p.kill() # supported from python 2.6

    def _stop_tunnel(self, instance_name):
        if self._is_tunnel_running(instance_name):
            self.__tunnel_processes[instance_name].kill()
            del self.__tunnel_processes[instance_name]

        cluster = self._staroid.cluster().get(self.__ske)
        if cluster == None:
            raise Exception("Can't get ske cluster")
        ns_api = self._staroid.namespace(cluster)
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

def spark(name, spark_conf=None, chisel_path=None, worker_num=1, worker_type="standard-4", worker_isolation="dedicated", delta=False, aws=True):
    init(reinit=False)

    cluster = SparkCluster(__singleton["instance"], name, spark_conf=spark_conf, worker_num=worker_num, worker_type=worker_type, worker_isolation=worker_isolation, delta=delta, aws=aws)
    return cluster
