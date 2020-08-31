<br />
<center>
  <img src="https://github.com/open-datastudio/datastudio/raw/master/docs/_static/open-datastudio-logo.png" width="110px"/>
</center>
<br />

# Open data studio

[Open data studio](https://open-datastudio.io) is managed computing computing service on Staroid cloud. Run your machine learning and large scale data processing workload without managing cluster and servers.

Supported computing frameworks are

 - Apache Spark
 - Dask (coming soon)
 - Ray (coming soon)

Let's get started!


## Quick start

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/open-datastudio/ods/blob/master/notebook/open-data-studio.ipynb)

### Install

```
pip install ods
```

### Initialize

1. Login staroid.com and get an [access token](https://staroid.com/settings/accesstokens). And set `STAROID_ACCESS_TOKEN` environment variable. See [here](https://github.com/staroids/staroid-python#configuration) for more detail.
2. Login staroid.com and create a SKE (Star Kubernetes engine) cluster.

```python
import ods
# 'ske' is the name of kubernetes cluster created from staroid.com. Alternatively, you can export 'STAROID_SKE' environment variable.
ods.init(ske="kube-cluster-1")
```

## Spark

### Create spark session
Create spark session with default configuration

```python
import ods
spark = ods.spark("spark-1") # 'spark-1' is name of spark-serverless instance to create.
df = spark.createDataFrame(....)
```

Configurue initial number of worker nodes

```python
import ods
spark = ods.spark("spark-1", worker_num=3).session()
df = spark.createDataFrame(....)
```

`detal=True` to automatically download & configure delta lake

```python
import ods
spark = ods.spark("spark-delta", delta=True)
spark.read.format("delta").load(....)
```

pass `spark_conf` dictionary for additonal configuration

```python
import ods
spark = ods.spark(spark_conf = {
    "spark.hadoop.fs.s3a.access.key": "...",
    "spark.hadoop.fs.s3a.secret.key" : "..."
})
```

Check [tests/test_spark.py](https://github.com/open-datastudio/ods/blob/master/tests/test_spark.py) as well.

## Dask

Coming soon 🚛

```python
import ods
cluster = ods.dask("dask-1", worker_num=10)

from dask.distributed import Client
client = Client(cluster)
```

## Ray

Coming soon 🚛

```python
import ods
ods.ray(cluster_name="")
```
