cd ~/Downloads

wget http://stat-computing.org/dataexpo/2009/2003.csv.bz2
wget http://stat-computing.org/dataexpo/2009/2004.csv.bz2
wget http://stat-computing.org/dataexpo/2009/2005.csv.bz2
wget http://stat-computing.org/dataexpo/2009/2006.csv.bz2
wget http://stat-computing.org/dataexpo/2009/2007.csv.bz2
wget http://stat-computing.org/dataexpo/2009/2008.csv.bz2
wget http://stat-computing.org/dataexpo/2009/carriers.csv
wget http://stat-computing.org/dataexpo/2009/plane-data.csv
wget http://stat-computing.org/dataexpo/2009/airports.csv

hdfs dfs -mkdir -p /user/cloudera/rawdata/airline/flight
hdfs dfs -mkdir -p /user/cloudera/rawdata/airline/airport
hdfs dfs -mkdir -p /user/cloudera/rawdata/airline/carrier
hdfs dfs -mkdir -p /user/cloudera/rawdata/airline/planeInfo

bzip2 -d 2003.csv.bz2
bzip2 -d 2004.csv.bz2
bzip2 -d 2005.csv.bz2
bzip2 -d 2006.csv.bz2
bzip2 -d 2007.csv.bz2
bzip2 -d 2008.csv.bz2


hdfs dfs -moveFromLocal 2003.csv /user/cloudera/rawdata/airline/flight
hdfs dfs -moveFromLocal 2004.csv /user/cloudera/rawdata/airline/flight
hdfs dfs -moveFromLocal 2005.csv /user/cloudera/rawdata/airline/flight
hdfs dfs -moveFromLocal 2006.csv /user/cloudera/rawdata/airline/flight
hdfs dfs -moveFromLocal 2007.csv /user/cloudera/rawdata/airline/flight
hdfs dfs -moveFromLocal 2008.csv /user/cloudera/rawdata/airline/flight

hdfs dfs -moveFromLocal plane-data.csv /user/cloudera/rawdata/airline/planeInfo
hdfs dfs -moveFromLocal airports.csv /user/cloudera/rawdata/airline/airport
hdfs dfs -moveFromLocal carriers.csv /user/cloudera/rawdata/airline/carrier

