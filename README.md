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


## Usage

### Initialization

```python
import ods

# in cluster initialization. (or from ~/.ods/config)
ods.init()

# initialization with staroid access token
ods.init(staroid_access_token="", staroid_org_name="", staroid_cluster_name="", )
```

### Get Spark cluster

```python
from pyspark.sql import SparkSession
import pyspark

spark_conf = pyspark.SparkConf()
spark = ods.spark(
    cluster_name="",
    worker_instance_type="standard-4",
    worker_num="2:2:10", # initial:min:max
    spark_conf=spark_conf # optional spark conf
)
```

### Get Dask cluster (planned)

```python
cluster = ods.dask(
    cluster_name="",
    worker_num=10
)

from dask.distributed import Client
client = Client(cluster)
```

### Get Ray cluster (planned)

```python
ods.ray(
    cluster_name=""
)
```
