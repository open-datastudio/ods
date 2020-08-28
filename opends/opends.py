from .spark_cluster import SparkCluster
from staroid import Staroid

import requests
import os, stat
from pathlib import Path
from shutil import which
import platform
import subprocess
import wget

CHISEL_VERSION="1.6.0"
CHISEL_ARCH_MAP={
    "x86_64": "amd64",
    "i386": "386"
}

class Opends:
    def __init__(self, staroid=None, cache_dir=None, chisel_path=None):
        if staroid == None:
            self.__staroid = Staroid()
        else:
            self.__staroid = staroid

        if cache_dir == None:
            self.__cache_dir = "{}/.opends".format(str(Path.home()))
        else:        
            self.__cache_dir = cache_dir

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


__singleton = {}

def init():
    __singleton["instance"] = Opends()
    return __singleton["instance"]

def spark(name, spark_conf=None, chisel_path=None):
    cluster = SparkCluster(__singleton["instance"], name, spark_conf=spark_conf)
    cluster.install()
    cluster.create_cluster()
    cluster.start_cluster()
    cluster.open_tunnel()
    spark = cluster.create_spark_session()
    return spark
