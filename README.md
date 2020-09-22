<br />
<center>
  <img src="https://github.com/open-datastudio/datastudio/raw/master/docs/_static/open-datastudio-logo.png" width="110px"/>
</center>

# Open data studio python client

[Open data studio](https://open-datastudio.io) is a managed computing service on Staroid. Run your machine learning and large scale data processing workloads without managing clusters and servers.

This repository provides a python client library.
Currently, the following computing frameworks are supported in the library.

 - Apache Spark
 - Ray (coming soon)
 - Dask (coming soon)

Let's get started!


## Quick start

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/open-datastudio/ods/blob/master/notebook/open-data-studio.ipynb)

### Install

```
pip install ods
```

Python `3.6`, `3.7`, `3.8` are supported.

### Initialize

1. Login staroid.com and get an [access token](https://staroid.com/settings/accesstokens). And set the `STAROID_ACCESS_TOKEN` environment variable. See [here](https://github.com/staroids/staroid-python#configuration) for more detail.
2. Login staroid.com and create a SKE (Star Kubernetes engine) cluster.

```python
import ods
# 'ske' is the name of kubernetes cluster created from staroid.com.
# Alternatively, you can set the 'STAROID_SKE' environment variable.
ods.init(ske="kube-cluster-1")
```

## Spark

Quick start video

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/J43qKJnp_N8/0.jpg)](https://www.youtube.com/watch?v=J43qKJnp_N8)

### Create spark session
Create a spark session with the default configuration.
You don't need to install/configure spark manually.

```python
import ods
spark = ods.spark("spark-1").session() # 'spark-1' is name of spark-serverless instance to create.
df = spark.createDataFrame(....)
```

Configure initial number of worker nodes

```python
import ods
spark = ods.spark("spark-1", worker_num=3).session()
df = spark.createDataFrame(....)
```

`detal=True` to automatically download & configure delta lake

```python
import ods
spark = ods.spark("spark-delta", delta=True).session()
spark.read.format("delta").load(....)
```

pass `spark_conf` dictionary for additonal configuration

```python
import ods
spark = ods.spark(spark_conf = {
    "spark.hadoop.fs.s3a.access.key": "...",
    "spark.hadoop.fs.s3a.secret.key" : "..."
}).session()
```

configure spark version

```python
import ods
spark = ods.spark(spark_version = "3.0.1").session()
```

Currently, spark `3.0.1`, `3.0.0` are supported.


Check [tests/test_spark.py](https://github.com/open-datastudio/ods/blob/master/tests/test_spark.py) for complete working example.

## Ray

Coming soon 🚛

```bash
$ ray up example-full.yaml
$ ray attach example-full.yaml
$ ray down example-full.yaml
```

## Dask

Coming soon 🚛


## Get involved

Open data studio is an open source project. Please give us feedback and feel free to get involved!

 - Feedbacks, questions - [ods issue tracker](https://github.com/open-datastudio/ods/issues)
 - [Staroid public dev roadmap](https://github.com/staroids/community/projects/1)
