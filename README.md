# Hadoop-MapReduce
Hadoop Map-Reduce

1. ssh parth5494@systemt.datascientistworkbench.com
-------------Before run---------------
hdfs dfs -mkdir Input1
hdfs dfs -put Input1 Input1

hdfs dfs -rm hdfs://iop-bi-master.imdemocloud.com:8020/user/hdfs/Input1/*
hdfs dfs -rmdir hdfs://iop-bi-master.imdemocloud.com:8020/user/hdfs/Input1/

hdfs dfs -ls hdfs://iop-bi-master.imdemocloud.com:8020/user/hdfs/Input1

export HADOOP_USER_NAME=hdfs
hdfs dfs -chown -R parth5494 hdfs://iop-bi-master.imdemocloud.com:8020/data/

--------------Run the code---------------------------------

/usr/iop/current/hadoop-client/bin/hadoop jar Assignment1.jar hadoop.WordCount /user/hdfs/Input1/ /Output1

-------------Viewing the output--------------

hdfs dfs -ls hdfs://iop-bi-master.imdemocloud.com:8020/Output1
hdfs dfs -cat hdfs://iop-bi-master.imdemocloud.com:8020/Output1/part-r-00000

agriculture     8
education       11
politics        10
sports  21

--------------After each run----------------

hdfs dfs -rm hdfs://iop-bi-master.imdemocloud.com:8020/data/intermediate/*
hdfs dfs -rmdir hdfs://iop-bi-master.imdemocloud.com:8020/data/intermediate/
hdfs dfs -rm hdfs://iop-bi-master.imdemocloud.com:8020/Output1/*
hdfs dfs -rmdir hdfs://iop-bi-master.imdemocloud.com:8020/Output1

-------------------------------------------------------------


2. ssh parth5494@systemt.datascientistworkbench.com
-------------Before run---------------
hdfs dfs -mkdir Input1
hdfs dfs -put Input1 Input1

export HADOOP_USER_NAME=hdfs
hdfs dfs -chown -R parth5494 hdfs://iop-bi-master.imdemocloud.com:8020/data/

--------------Run the code---------------------------------

/usr/iop/current/hadoop-client/bin/hadoop jar Assignment1.jar hadoop.WordRank /user/hdfs/Input1/ /Output2

-------------Viewing the output--------------

hdfs dfs -ls hdfs://iop-bi-master.imdemocloud.com:8020/Output2
hdfs dfs -cat hdfs://iop-bi-master.imdemocloud.com:8020/Output2/part-r-00000


agriculture-education-sports-politics   Iowa, Louisiana
agriculture-politics    Wyoming
agriculture-politics-education  Alaska, North_Dakota
agriculture-politics-sports     Nebraska, Delaware
agriculture-politics-sports-education   South_Dakota
education-agriculture-politics  Mississippi, Washington
education-agriculture-politics-sports   Ohio, Arkansas, New_York
education-politics      West_Virginia, Tennessee, South_Carolina
education-politics-sports-agriculture   Alabama
education-sports-politics       Maryland, Oklahoma
politics        Maine
politics-agriculture-education-sports   Kentucky
politics-agriculture-sports     Pennsylvania
politics-agriculture-sports-education   Wisconsin
politics-education      Hawaii, Georgia
politics-education-sports       Arizona, New_Mexico
politics-sports Idaho
politics-sports-agriculture     Massachusetts
sports  Rhode_Island, Indiana
sports-agriculture-politics-education   Kansas
sports-education-agriculture-politics   Missouri, New_Jersey, Texas, Virginia
sports-education-politics-agriculture   California
sports-politics Utah, Montana, Connecticut, New_Hampshire
sports-politics-agriculture     Nevada
sports-politics-agriculture-education   Florida
sports-politics-education       Michigan, Illinois, Minnesota, Oregon
sports-politics-education-agriculture   Vermont, Colorado, North_Carolina


--------------After each run----------------

hdfs dfs -rm hdfs://iop-bi-master.imdemocloud.com:8020/data/intermediate/*
hdfs dfs -rmdir hdfs://iop-bi-master.imdemocloud.com:8020/data/intermediate/
hdfs dfs -rm hdfs://iop-bi-master.imdemocloud.com:8020/Output2/*
hdfs dfs -rmdir hdfs://iop-bi-master.imdemocloud.com:8020/Output2

-------------------------------------------------------------