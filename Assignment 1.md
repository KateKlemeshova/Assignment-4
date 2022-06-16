```python
import os
import re
import json
import socket
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import utils as pu
from pyspark.sql import functions as F
from pyspark.sql import types as pt
```

## Assignment #1 

This task rely on to 


```python
print('user:', os.environ['JUPYTERHUB_SERVICE_PREFIX'])

def uiWebUrl(self):
    from urllib.parse import urlparse
    web_url = self._jsc.sc().uiWebUrl().get()
    port = urlparse(web_url).port
    return '{}proxy/{}/jobs/'.format(os.environ['JUPYTERHUB_SERVICE_PREFIX'], port)

SparkContext.uiWebUrl = property(uiWebUrl)

conf = SparkConf().set('spark.master', 'local[*]').set('spark.driver.memory', '4g').set("spark.executor.instances", "4")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark
```

    user: /user/st095440/



    ---------------------------------------------------------------------------

    ValueError                                Traceback (most recent call last)

    <ipython-input-53-e8e11fbf1e49> in <module>
         10 
         11 conf = SparkConf().set('spark.master', 'local[*]').set('spark.driver.memory', '4g').set("spark.executor.instances", "4")
    ---> 12 sc = SparkContext(conf=conf)
         13 spark = SparkSession(sc)
         14 spark


    /opt/conda/lib/python3.8/site-packages/pyspark/context.py in __init__(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, gateway, jsc, profiler_cls)
        131                 " is not allowed as it is a security risk.")
        132 
    --> 133         SparkContext._ensure_initialized(self, gateway=gateway, conf=conf)
        134         try:
        135             self._do_init(master, appName, sparkHome, pyFiles, environment, batchSize, serializer,


    /opt/conda/lib/python3.8/site-packages/pyspark/context.py in _ensure_initialized(cls, instance, gateway, conf)
        334 
        335                     # Raise error if there is already a running Spark context
    --> 336                     raise ValueError(
        337                         "Cannot run multiple SparkContexts at once; "
        338                         "existing SparkContext(app=%s, master=%s)"


    ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=pyspark-shell, master=local[*]) created by __init__ at <ipython-input-2-e8e11fbf1e49>:12 


## Songs 

The songs. Note that data is in unicode.

- song_id
- song_length: in ms
- genre_ids: genre category. Some songs have multiple genres and they are separated by |
- artist_name
- composer
- lyricist
- language



```python
songs = spark.read.option("header", "True")\
    .csv("file:///home/jovyan/__DATA/big_data_2022/labs/kkbox-music-recommendation-challenge/songs.csv")

songs.limit(100).toPandas().head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>song_id</th>
      <th>song_length</th>
      <th>genre_ids</th>
      <th>artist_name</th>
      <th>composer</th>
      <th>lyricist</th>
      <th>language</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CXoTN1eb7AI+DntdU1vbcwGRV4SCIDxZu+YD8JP8r4E=</td>
      <td>247640</td>
      <td>465</td>
      <td>張信哲 (Jeff Chang)</td>
      <td>董貞</td>
      <td>何啟弘</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>o0kFgae9QtnYgRkVPqLJwa05zIhRlUjfF7O1tDw0ZDU=</td>
      <td>197328</td>
      <td>444</td>
      <td>BLACKPINK</td>
      <td>TEDDY|  FUTURE BOUNCE|  Bekuh BOOM</td>
      <td>TEDDY</td>
      <td>31.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>DwVvVurfpuz+XPuFvucclVQEyPqcpUkHR0ne1RQzPs0=</td>
      <td>231781</td>
      <td>465</td>
      <td>SUPER JUNIOR</td>
      <td>None</td>
      <td>None</td>
      <td>31.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>dKMBWoZyScdxSkihKG+Vf47nc18N9q4m58+b4e7dSSE=</td>
      <td>273554</td>
      <td>465</td>
      <td>S.H.E</td>
      <td>湯小康</td>
      <td>徐世珍</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>W3bqWd3T+VeHFzHAUfARgW9AvVRaF4N5Yzm4Mr6Eo/o=</td>
      <td>140329</td>
      <td>726</td>
      <td>貴族精選</td>
      <td>Traditional</td>
      <td>Traditional</td>
      <td>52.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>kKJ2JNU5h8rphyW21ovC+RZU+yEHPM+3w85J37p7vEQ=</td>
      <td>235520</td>
      <td>864|857|850|843</td>
      <td>貴族精選</td>
      <td>Joe Hisaishi</td>
      <td>Hayao Miyazaki</td>
      <td>17.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>N9vbanw7BSMoUgdfJlgX1aZPE1XZg8OS1wf88AQEcMc=</td>
      <td>226220</td>
      <td>458</td>
      <td>伍佰 &amp; China Blue</td>
      <td>Jonathan Lee</td>
      <td>None</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>GsCpr618xfveHYJdo+E5SybrpR906tsjLMeKyrCNw8s=</td>
      <td>276793</td>
      <td>465</td>
      <td>光良 (Michael Wong)</td>
      <td>光良</td>
      <td>彭資閔</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>oTi7oINPX+rxoGp+3O6llSltQTl80jDqHoULfRoLcG4=</td>
      <td>228623</td>
      <td>465</td>
      <td>林俊傑 (JJ Lin)</td>
      <td>JJ Lin</td>
      <td>Wu Qing Feng</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>btcG03OHY3GNKWccPP0auvtSbhxog/kllIIOx5grE/k=</td>
      <td>232629</td>
      <td>352|1995</td>
      <td>Kodaline</td>
      <td>Stephen Garrigan| Mark Prendergast| Vincent Ma...</td>
      <td>Stephen Garrigan| Mark Prendergast| Vincent Ma...</td>
      <td>52.0</td>
    </tr>
  </tbody>
</table>
</div>




```python

```

## "Train" data

Some data of users listen history 

- msno: user id
- song_id: song id
- source_system_tab: the name of the tab where the event was triggered. System tabs are used to categorize KKBOX mobile apps functions. For example, tab my library contains functions to manipulate the local storage, and tab search contains functions relating to search.
- source_screen_name: name of the layout a user sees.
- source_type: an entry point a user first plays music on mobile apps. An entry point could be album, online-playlist, song .. etc.
- target: this is the target variable. target=1 means there are recurring listening event(s) triggered within a month after the user’s very first observable listening event, target=0 otherwise .



```python
listen_history = spark.read.option("header", "True")\
    .csv("file:///home/jovyan/__DATA/big_data_2022/labs/kkbox-music-recommendation-challenge/train.csv")

listen_history.show()
```

    +--------------------+--------------------+-----------------+-------------------+-------------------+------+
    |                msno|             song_id|source_system_tab| source_screen_name|        source_type|target|
    +--------------------+--------------------+-----------------+-------------------+-------------------+------+
    |FGtllVqz18RPiwJj/...|BBzumQNXUHKdEBOB7...|          explore|            Explore|    online-playlist|     1|
    |Xumu+NIjS6QYVxDS4...|bhp/MpSNoqoxOIB+/...|       my library|Local playlist more|     local-playlist|     1|
    |Xumu+NIjS6QYVxDS4...|JNWfrrC7zNN7BdMps...|       my library|Local playlist more|     local-playlist|     1|
    |Xumu+NIjS6QYVxDS4...|2A87tzfnJTSWqD7gI...|       my library|Local playlist more|     local-playlist|     1|
    |FGtllVqz18RPiwJj/...|3qm6XTZ6MOCU11x8F...|          explore|            Explore|    online-playlist|     1|
    |FGtllVqz18RPiwJj/...|3Hg5kugV1S0wzEVLA...|          explore|            Explore|    online-playlist|     1|
    |Xumu+NIjS6QYVxDS4...|VkILU0H1h3NMmk9MQ...|       my library|Local playlist more|     local-playlist|     1|
    |FGtllVqz18RPiwJj/...|bPIvRTzfHxH5LgHrS...|          explore|            Explore|    online-playlist|     1|
    |uHqAtShXTRXju5GE8...|/bU6IRSK+YNlNbaTk...|       my library|Local playlist more|      local-library|     1|
    |uHqAtShXTRXju5GE8...|EbI7xoNxI+3QSsiHx...|       my library|Local playlist more|      local-library|     1|
    |uHqAtShXTRXju5GE8...|t0aT90DlS1TGncgnK...|       my library|Local playlist more|      local-library|     1|
    |uHqAtShXTRXju5GE8...|8FGjC9W+7F8WjheGZ...|       my library|Local playlist more|      local-library|     1|
    |TJU0Gfvy7FB+r89bW...|u6/Pb7X4u7KU4gXrB...|       my library|               null|top-hits-for-artist|     0|
    |3g0bC24RD7QUeALY1...|TYhx9eqWklddkLQlA...|       my library|Local playlist more|      local-library|     1|
    |TJU0Gfvy7FB+r89bW...|IgMar/mVrJQ+ODFPy...|       my library|Local playlist more|     local-playlist|     1|
    |3g0bC24RD7QUeALY1...|6HofPS0v2MVFsL10y...|       my library|Local playlist more|      local-library|     1|
    |TJU0Gfvy7FB+r89bW...|a4TbK5V15pj3YZUOG...|       my library|         My library|     local-playlist|     1|
    |TJU0Gfvy7FB+r89bW...|t95ClWf/B7Hi46sJe...|       my library|         My library|     local-playlist|     1|
    |TJU0Gfvy7FB+r89bW...|4ZISq5iNKgBGGW2Ov...|       my library|               null|top-hits-for-artist|     0|
    |uQQHTQJ1nVEkBfbXe...|9yi9yLGK5Soiz1IO3...|       my library|Local playlist more|      local-library|     1|
    +--------------------+--------------------+-----------------+-------------------+-------------------+------+
    only showing top 20 rows
    


## Assignment:

PREFER DATA FRAME API(!)

Vision of result:
- self-sufficient jupyter notebook
- not only the code but n
- published on github 
- link to your github attached here https://docs.google.com/spreadsheets/d/1F8ScpCCiBaxiyZqDd09jn6t25pykiK8oauJbRuuA8Q4/edit?usp=sharing


Task 1: 
- Find out top-20 of most popular artist's in terms of: raw listens, unique users listens;

Task 2: 
- find out top-3 languages by num of raw listens
- build histogram of songs length by language / genre. What is the longest genre? What is more valuable in terms of length: language or genre? 

Deadlines:
- soft 17.05
- hard 24.05


```python
songs = spark.read.option("header", "True")\
    .csv("file:///home/jovyan/__DATA/big_data_2022/labs/kkbox-music-recommendation-challenge/songs.csv")

songs.limit(100).toPandas().head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>song_id</th>
      <th>song_length</th>
      <th>genre_ids</th>
      <th>artist_name</th>
      <th>composer</th>
      <th>lyricist</th>
      <th>language</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CXoTN1eb7AI+DntdU1vbcwGRV4SCIDxZu+YD8JP8r4E=</td>
      <td>247640</td>
      <td>465</td>
      <td>張信哲 (Jeff Chang)</td>
      <td>董貞</td>
      <td>何啟弘</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>o0kFgae9QtnYgRkVPqLJwa05zIhRlUjfF7O1tDw0ZDU=</td>
      <td>197328</td>
      <td>444</td>
      <td>BLACKPINK</td>
      <td>TEDDY|  FUTURE BOUNCE|  Bekuh BOOM</td>
      <td>TEDDY</td>
      <td>31.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>DwVvVurfpuz+XPuFvucclVQEyPqcpUkHR0ne1RQzPs0=</td>
      <td>231781</td>
      <td>465</td>
      <td>SUPER JUNIOR</td>
      <td>None</td>
      <td>None</td>
      <td>31.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>dKMBWoZyScdxSkihKG+Vf47nc18N9q4m58+b4e7dSSE=</td>
      <td>273554</td>
      <td>465</td>
      <td>S.H.E</td>
      <td>湯小康</td>
      <td>徐世珍</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>W3bqWd3T+VeHFzHAUfARgW9AvVRaF4N5Yzm4Mr6Eo/o=</td>
      <td>140329</td>
      <td>726</td>
      <td>貴族精選</td>
      <td>Traditional</td>
      <td>Traditional</td>
      <td>52.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>kKJ2JNU5h8rphyW21ovC+RZU+yEHPM+3w85J37p7vEQ=</td>
      <td>235520</td>
      <td>864|857|850|843</td>
      <td>貴族精選</td>
      <td>Joe Hisaishi</td>
      <td>Hayao Miyazaki</td>
      <td>17.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>N9vbanw7BSMoUgdfJlgX1aZPE1XZg8OS1wf88AQEcMc=</td>
      <td>226220</td>
      <td>458</td>
      <td>伍佰 &amp; China Blue</td>
      <td>Jonathan Lee</td>
      <td>None</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>GsCpr618xfveHYJdo+E5SybrpR906tsjLMeKyrCNw8s=</td>
      <td>276793</td>
      <td>465</td>
      <td>光良 (Michael Wong)</td>
      <td>光良</td>
      <td>彭資閔</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>oTi7oINPX+rxoGp+3O6llSltQTl80jDqHoULfRoLcG4=</td>
      <td>228623</td>
      <td>465</td>
      <td>林俊傑 (JJ Lin)</td>
      <td>JJ Lin</td>
      <td>Wu Qing Feng</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>btcG03OHY3GNKWccPP0auvtSbhxog/kllIIOx5grE/k=</td>
      <td>232629</td>
      <td>352|1995</td>
      <td>Kodaline</td>
      <td>Stephen Garrigan| Mark Prendergast| Vincent Ma...</td>
      <td>Stephen Garrigan| Mark Prendergast| Vincent Ma...</td>
      <td>52.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
listen_history = spark.read.option("header", "True")\
    .csv("file:///home/jovyan/__DATA/big_data_2022/labs/kkbox-music-recommendation-challenge/train.csv")

listen_history.limit(100).toPandas().head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>msno</th>
      <th>song_id</th>
      <th>source_system_tab</th>
      <th>source_screen_name</th>
      <th>source_type</th>
      <th>target</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>FGtllVqz18RPiwJj/edr2gV78zirAiY/9SmYvia+kCg=</td>
      <td>BBzumQNXUHKdEBOB7mAJuzok+IJA1c2Ryg/yzTF6tik=</td>
      <td>explore</td>
      <td>Explore</td>
      <td>online-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Xumu+NIjS6QYVxDS4/t3SawvJ7viT9hPKXmf0RtLNx8=</td>
      <td>bhp/MpSNoqoxOIB+/l8WPqu6jldth4DIpCm3ayXnJqM=</td>
      <td>my library</td>
      <td>Local playlist more</td>
      <td>local-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Xumu+NIjS6QYVxDS4/t3SawvJ7viT9hPKXmf0RtLNx8=</td>
      <td>JNWfrrC7zNN7BdMpsISKa4Mw+xVJYNnxXh3/Epw7QgY=</td>
      <td>my library</td>
      <td>Local playlist more</td>
      <td>local-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Xumu+NIjS6QYVxDS4/t3SawvJ7viT9hPKXmf0RtLNx8=</td>
      <td>2A87tzfnJTSWqD7gIZHisolhe4DMdzkbd6LzO1KHjNs=</td>
      <td>my library</td>
      <td>Local playlist more</td>
      <td>local-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>FGtllVqz18RPiwJj/edr2gV78zirAiY/9SmYvia+kCg=</td>
      <td>3qm6XTZ6MOCU11x8FIVbAGH5l5uMkT3/ZalWG1oo2Gc=</td>
      <td>explore</td>
      <td>Explore</td>
      <td>online-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>FGtllVqz18RPiwJj/edr2gV78zirAiY/9SmYvia+kCg=</td>
      <td>3Hg5kugV1S0wzEVLAEfqjIV5UHzb7bCrdBRQlGygLvU=</td>
      <td>explore</td>
      <td>Explore</td>
      <td>online-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Xumu+NIjS6QYVxDS4/t3SawvJ7viT9hPKXmf0RtLNx8=</td>
      <td>VkILU0H1h3NMmk9MQrXouNudGk5n8Ls5cqRRuBxeTh4=</td>
      <td>my library</td>
      <td>Local playlist more</td>
      <td>local-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>FGtllVqz18RPiwJj/edr2gV78zirAiY/9SmYvia+kCg=</td>
      <td>bPIvRTzfHxH5LgHrStll+tYwSQNVV8PySgA3M1PfTgc=</td>
      <td>explore</td>
      <td>Explore</td>
      <td>online-playlist</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>uHqAtShXTRXju5GE8ri3ITsVFepPf8jUoCF7ffNOuqE=</td>
      <td>/bU6IRSK+YNlNbaTkxo7bhsb2EDLPrnksdX3ggcZNhI=</td>
      <td>my library</td>
      <td>Local playlist more</td>
      <td>local-library</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>uHqAtShXTRXju5GE8ri3ITsVFepPf8jUoCF7ffNOuqE=</td>
      <td>EbI7xoNxI+3QSsiHxL13zBdgHIJOwa3srHd7cDcnJ0g=</td>
      <td>my library</td>
      <td>Local playlist more</td>
      <td>local-library</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
members = spark.read.option("header", "True")\
    .csv("file:///home/jovyan/__DATA/big_data_2022/labs/kkbox-music-recommendation-challenge/members.csv")

members.limit(100).toPandas().head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>msno</th>
      <th>city</th>
      <th>bd</th>
      <th>gender</th>
      <th>registered_via</th>
      <th>registration_init_time</th>
      <th>expiration_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>XQxgAYj3klVKjR3oxPPXYYFp4soD4TuBghkhMTD4oTw=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>7</td>
      <td>20110820</td>
      <td>20170920</td>
    </tr>
    <tr>
      <th>1</th>
      <td>UizsfmJb9mV54qE9hCYyU07Va97c0lCRLEQX3ae+ztM=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>7</td>
      <td>20150628</td>
      <td>20170622</td>
    </tr>
    <tr>
      <th>2</th>
      <td>D8nEhsIOBSoE6VthTaqDX8U6lqjJ7dLdr72mOyLya2A=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>4</td>
      <td>20160411</td>
      <td>20170712</td>
    </tr>
    <tr>
      <th>3</th>
      <td>mCuD+tZ1hERA/o5GPqk38e041J8ZsBaLcu7nGoIIvhI=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>9</td>
      <td>20150906</td>
      <td>20150907</td>
    </tr>
    <tr>
      <th>4</th>
      <td>q4HRBfVSssAFS9iRfxWrohxuk9kCYMKjHOEagUMV6rQ=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>4</td>
      <td>20170126</td>
      <td>20170613</td>
    </tr>
    <tr>
      <th>5</th>
      <td>zgPOEyUn5a/Fvuzb3m69ajzxjkbblVtObglW89FzLdo=</td>
      <td>13</td>
      <td>43</td>
      <td>female</td>
      <td>9</td>
      <td>20120703</td>
      <td>20171006</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Sw9AT8QoR4wWiNUqHZUH6g5ahzGUx4lo1g+Y3xE2f2M=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>4</td>
      <td>20160326</td>
      <td>20160329</td>
    </tr>
    <tr>
      <th>7</th>
      <td>pg6bT2XZkSP1TDBy4qn3HBPY/HffKQ/bg8WIISQYBSY=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>7</td>
      <td>20160130</td>
      <td>20170930</td>
    </tr>
    <tr>
      <th>8</th>
      <td>kfk1AdTNH2dNqF5LzIs4e0vwGPejw2jrnFjJlcYnEgk=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>7</td>
      <td>20110111</td>
      <td>20170930</td>
    </tr>
    <tr>
      <th>9</th>
      <td>tscijwx4dbEp0NXGl+iFtHJ8zrj+TkcMrduOQk9t+gE=</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>7</td>
      <td>20160217</td>
      <td>20170613</td>
    </tr>
  </tbody>
</table>
</div>



## Task 1


### Top-20 of most popular artist's in terms of raw listens using the approach from lecrure 4
First of all, we need to join two dataset on `"song_id"` using `join`.
Secondly, we need to group our data on `"artist_name"`, then find the sum of `"raw listeners"`.
Finally, we sort it and find TOP-20


```python
joined = listen_history.join(songs, ["song_id"], 'inner')

joined.groupBy("artist_name")\
.agg(F.count(F.col("msno")).alias("raw listens")).orderBy(F.col("raw listens").desc())\
.limit(100).toPandas().head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_name</th>
      <th>raw listens</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Various Artists</td>
      <td>303617</td>
    </tr>
    <tr>
      <th>1</th>
      <td>周杰倫 (Jay Chou)</td>
      <td>186776</td>
    </tr>
    <tr>
      <th>2</th>
      <td>五月天 (Mayday)</td>
      <td>182088</td>
    </tr>
    <tr>
      <th>3</th>
      <td>林俊傑 (JJ Lin)</td>
      <td>115325</td>
    </tr>
    <tr>
      <th>4</th>
      <td>田馥甄 (Hebe)</td>
      <td>104946</td>
    </tr>
    <tr>
      <th>5</th>
      <td>aMEI (張惠妹)</td>
      <td>82799</td>
    </tr>
    <tr>
      <th>6</th>
      <td>陳奕迅 (Eason Chan)</td>
      <td>76035</td>
    </tr>
    <tr>
      <th>7</th>
      <td>玖壹壹</td>
      <td>70445</td>
    </tr>
    <tr>
      <th>8</th>
      <td>G.E.M.鄧紫棋</td>
      <td>67297</td>
    </tr>
    <tr>
      <th>9</th>
      <td>BIGBANG</td>
      <td>61927</td>
    </tr>
    <tr>
      <th>10</th>
      <td>謝和弦 (R-chord)</td>
      <td>57040</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Maroon 5</td>
      <td>55151</td>
    </tr>
    <tr>
      <th>12</th>
      <td>A-Lin</td>
      <td>52913</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Eric 周興哲</td>
      <td>49426</td>
    </tr>
    <tr>
      <th>14</th>
      <td>蔡依林 (Jolin Tsai)</td>
      <td>49055</td>
    </tr>
    <tr>
      <th>15</th>
      <td>蘇打綠 (Sodagreen)</td>
      <td>47177</td>
    </tr>
    <tr>
      <th>16</th>
      <td>楊丞琳 (Rainie Yang)</td>
      <td>46006</td>
    </tr>
    <tr>
      <th>17</th>
      <td>丁噹 (Della)</td>
      <td>45762</td>
    </tr>
    <tr>
      <th>18</th>
      <td>梁靜茹 (Fish Leong)</td>
      <td>44290</td>
    </tr>
    <tr>
      <th>19</th>
      <td>The Chainsmokers</td>
      <td>44215</td>
    </tr>
  </tbody>
</table>
</div>



### Using the approach from the previous task, we will find Top-20 "unique users listens"


```python
joined.groupBy("artist_name")\
.agg(F.countDistinct(F.col("msno")).alias("unique users listens")).orderBy(F.col("unique users listens").desc())\
.limit(100).toPandas().head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist_name</th>
      <th>unique users listens</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Various Artists</td>
      <td>22256</td>
    </tr>
    <tr>
      <th>1</th>
      <td>田馥甄 (Hebe)</td>
      <td>18771</td>
    </tr>
    <tr>
      <th>2</th>
      <td>周杰倫 (Jay Chou)</td>
      <td>18727</td>
    </tr>
    <tr>
      <th>3</th>
      <td>五月天 (Mayday)</td>
      <td>18183</td>
    </tr>
    <tr>
      <th>4</th>
      <td>林俊傑 (JJ Lin)</td>
      <td>17160</td>
    </tr>
    <tr>
      <th>5</th>
      <td>陳奕迅 (Eason Chan)</td>
      <td>16290</td>
    </tr>
    <tr>
      <th>6</th>
      <td>G.E.M.鄧紫棋</td>
      <td>16064</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Eric 周興哲</td>
      <td>15672</td>
    </tr>
    <tr>
      <th>8</th>
      <td>謝和弦 (R-chord)</td>
      <td>15313</td>
    </tr>
    <tr>
      <th>9</th>
      <td>aMEI (張惠妹)</td>
      <td>15169</td>
    </tr>
    <tr>
      <th>10</th>
      <td>周湯豪 (NICKTHEREAL)</td>
      <td>14827</td>
    </tr>
    <tr>
      <th>11</th>
      <td>丁噹 (Della)</td>
      <td>12837</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Alan Walker</td>
      <td>12830</td>
    </tr>
    <tr>
      <th>13</th>
      <td>玖壹壹</td>
      <td>12487</td>
    </tr>
    <tr>
      <th>14</th>
      <td>The Chainsmokers</td>
      <td>12477</td>
    </tr>
    <tr>
      <th>15</th>
      <td>吳克群 (Kenji Wu)</td>
      <td>12468</td>
    </tr>
    <tr>
      <th>16</th>
      <td>陳勢安 (Andrew Tan)</td>
      <td>12378</td>
    </tr>
    <tr>
      <th>17</th>
      <td>楊丞琳 (Rainie Yang)</td>
      <td>11976</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Maroon 5</td>
      <td>11963</td>
    </tr>
    <tr>
      <th>19</th>
      <td>A-Lin</td>
      <td>11874</td>
    </tr>
  </tbody>
</table>
</div>



## Task 2
### Find out top-3 languages by num of raw listens


```python
joined.groupBy("language")\
.agg(F.count(F.col("msno")).alias("raw listens")).orderBy(F.col("raw listens").desc())\
.limit(100).toPandas().head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>language</th>
      <th>raw listens</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3.0</td>
      <td>4044643</td>
    </tr>
    <tr>
      <th>1</th>
      <td>52.0</td>
      <td>1864788</td>
    </tr>
    <tr>
      <th>2</th>
      <td>31.0</td>
      <td>656623</td>
    </tr>
  </tbody>
</table>
</div>



### Build histogram of songs length by language / genre. What is the longest genre? What is more valuable in terms of length: language or genre?


```python
songs_lang_len = songs.groupby(F.col("language"))\
.agg(F.sum("song_length").alias("song_length_sum")).orderBy(F.col("song_length_sum").desc())\
.limit(100).toPandas()
```


```python
songs_lang_len.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>language</th>
      <th>song_length_sum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>52.0</td>
      <td>3.134235e+11</td>
    </tr>
    <tr>
      <th>1</th>
      <td>-1.0</td>
      <td>1.764514e+11</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3.0</td>
      <td>2.594856e+10</td>
    </tr>
    <tr>
      <th>3</th>
      <td>17.0</td>
      <td>2.316844e+10</td>
    </tr>
    <tr>
      <th>4</th>
      <td>24.0</td>
      <td>9.963970e+09</td>
    </tr>
    <tr>
      <th>5</th>
      <td>31.0</td>
      <td>8.582794e+09</td>
    </tr>
    <tr>
      <th>6</th>
      <td>10.0</td>
      <td>3.770904e+09</td>
    </tr>
    <tr>
      <th>7</th>
      <td>45.0</td>
      <td>3.480345e+09</td>
    </tr>
    <tr>
      <th>8</th>
      <td>59.0</td>
      <td>1.811680e+09</td>
    </tr>
    <tr>
      <th>9</th>
      <td>38.0</td>
      <td>5.746941e+08</td>
    </tr>
  </tbody>
</table>
</div>




```python
#convert in ms into thousands of hour
songs_lang_len['song_length_sum_in_th_hours'] = songs_lang_len['song_length_sum']/60/60/60/1000
```


```python
songs_lang_len.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>language</th>
      <th>song_length_sum</th>
      <th>song_length_sum_in_th_hours</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>52.0</td>
      <td>3.134235e+11</td>
      <td>1451.034816</td>
    </tr>
    <tr>
      <th>1</th>
      <td>-1.0</td>
      <td>1.764514e+11</td>
      <td>816.904645</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3.0</td>
      <td>2.594856e+10</td>
      <td>120.132206</td>
    </tr>
    <tr>
      <th>3</th>
      <td>17.0</td>
      <td>2.316844e+10</td>
      <td>107.261303</td>
    </tr>
    <tr>
      <th>4</th>
      <td>24.0</td>
      <td>9.963970e+09</td>
      <td>46.129490</td>
    </tr>
    <tr>
      <th>5</th>
      <td>31.0</td>
      <td>8.582794e+09</td>
      <td>39.735159</td>
    </tr>
    <tr>
      <th>6</th>
      <td>10.0</td>
      <td>3.770904e+09</td>
      <td>17.457890</td>
    </tr>
    <tr>
      <th>7</th>
      <td>45.0</td>
      <td>3.480345e+09</td>
      <td>16.112709</td>
    </tr>
    <tr>
      <th>8</th>
      <td>59.0</td>
      <td>1.811680e+09</td>
      <td>8.387407</td>
    </tr>
    <tr>
      <th>9</th>
      <td>38.0</td>
      <td>5.746941e+08</td>
      <td>2.660621</td>
    </tr>
  </tbody>
</table>
</div>




```python
fig, ax = plt.subplots(figsize=(24, 8))
sns.barplot(x='language', y='song_length_sum_in_th_hours', data=songs_lang_len.head(20), ax=ax)
```




    <AxesSubplot:xlabel='language', ylabel='song_length_sum_in_th_hours'>




    
![png](output_24_1.png)
    



```python
genre_song_length = songs.select("genre_ids", "song_length")\
.groupby(F.col("genre_ids"))\
.agg(F.sum("song_length").alias("song_length_sum"))\
.toPandas()
```


```python
genre_song_length.genre_ids.describe()
```




    count     1046
    unique    1046
    top        921
    freq         1
    Name: genre_ids, dtype: object




```python
genre_song_length.head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>genre_ids</th>
      <th>song_length_sum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>829</td>
      <td>2.644641e+09</td>
    </tr>
    <tr>
      <th>1</th>
      <td>691</td>
      <td>3.710705e+09</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1572</td>
      <td>3.748923e+08</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1152|465|1180</td>
      <td>5.705852e+06</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1152|2022</td>
      <td>1.396196e+07</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1944|310</td>
      <td>2.118175e+07</td>
    </tr>
    <tr>
      <th>6</th>
      <td>423|1047</td>
      <td>2.223643e+06</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2022|430</td>
      <td>1.250166e+07</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1633|1955</td>
      <td>6.148940e+05</td>
    </tr>
    <tr>
      <th>9</th>
      <td>451</td>
      <td>3.193254e+09</td>
    </tr>
    <tr>
      <th>10</th>
      <td>465|1259</td>
      <td>1.068185e+08</td>
    </tr>
    <tr>
      <th>11</th>
      <td>465|139</td>
      <td>4.450986e+08</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2022|1955</td>
      <td>6.290436e+07</td>
    </tr>
    <tr>
      <th>13</th>
      <td>1609|726</td>
      <td>2.694790e+05</td>
    </tr>
    <tr>
      <th>14</th>
      <td>465|1609|94</td>
      <td>8.144610e+05</td>
    </tr>
    <tr>
      <th>15</th>
      <td>1187|1259</td>
      <td>2.081430e+05</td>
    </tr>
    <tr>
      <th>16</th>
      <td>786|947</td>
      <td>6.728666e+08</td>
    </tr>
    <tr>
      <th>17</th>
      <td>1280</td>
      <td>3.568119e+07</td>
    </tr>
    <tr>
      <th>18</th>
      <td>451|2189|367</td>
      <td>9.959030e+05</td>
    </tr>
    <tr>
      <th>19</th>
      <td>829|423</td>
      <td>8.995240e+05</td>
    </tr>
  </tbody>
</table>
</div>




```python
#convert in ms into thousands of hour
genre_song_length['genre_song_length_in_th_hours'] = genre_song_length['song_length_sum']/60/60/60/1000
```


```python
fig, ax = plt.subplots(figsize=(24, 8))
plt.ticklabel_format(style='plain', axis='y')
sns.barplot(x='genre_ids', y='genre_song_length_in_th_hours', data=genre_song_length.head(20), ax=ax)
```




    <AxesSubplot:xlabel='genre_ids', ylabel='genre_song_length_in_th_hours'>




    
![png](output_29_1.png)
    


## Answers
What is the longest genre? 829
What is more valuable in terms of length: language or genre? from my standpoint, Language is more important, as genres has more than 1000 unique values, and can be combined in different ways. In turn, language is more 'stable feature'
