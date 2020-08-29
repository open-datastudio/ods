<br />
<center>
  <img src="https://github.com/open-datastudio/datastudio/raw/master/docs/_static/open-datastudio-logo.png" width="250px"/>
</center>
<br />

# Open data studio python library


## Install

```
pip install ods
```


## Quick start

### Configuration

1. [Configure staroid](https://github.com/staroids/staroid-python#configuration)
2. Login staroid.com and create a SKE (Star Kubernetes engine) cluster.

```python
import ods
ods.init(ske="kube-cluster-1") # ske is a name of kubernetes cluster
```

### Get Spark cluster

```python
spark = ods.spark("spark-1", worker_num=3).session() # create spark session with 3 initial worker nodes

df = spark.createDataFrame(....)
```

### Get Dask cluster (Coming soon)

```python
cluster = ods.dask("dask-1", worker_num=10)

from dask.distributed import Client
client = Client(cluster)
```

### Get Ray cluster (Coming soon)

```python
ods.ray(cluster_name="")
```
