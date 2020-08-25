<br />
<center>
  <img src="https://github.com/open-datastudio/datastudio/raw/master/docs/_static/open-datastudio-logo.png" width="250px"/>
</center>
<br />

# Open data studio python library


## Install

```
pip install opends
```


## Usage

### Initialization

```python
import opends

# in cluster initialization. (or from ~/.opends/config)
opends.init()

# initialization with staroid access token
opends.init(staroid_access_token="", staroid_org_name="", staroid_cluster_name="", )
```

### Get Spark cluster

```python
from pyspark.sql import SparkSession
import pyspark

spark_conf = pyspark.SparkConf()
spark = opends.spark(
    cluster_name="",
    worker_instance_type="standard-4",
    worker_num="2:2:10", # initial:min:max
    spark_conf=spark_conf # optional spark conf
)
```

### Get Dask cluster

```python
cluster = opends.dask(
    cluster_name="",
    worker_num=10
)

from dask.distributed import Client
client = Client(cluster)
```

### Get Ray cluster

```python
opends.ray(
    cluster_name=""
)
```


## Design consideration

- Should ray, daks, pyspark be a dependency package or not?
- Standard python Convention
- Pluggable code structure
  - New cluster framework
  - New cloud infrastructure
- Support multiple python versions (3.6, 3.7, 3.8)
- How to print ui URL (spark-ui, dask-ui, ...)
- Unittest