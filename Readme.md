# Full dataset:
Dataset is upload on google drive due to the fize size on 2GB can be download from below link :   zvv

### Input : 
### Output : 

      
# Usefull Commands



### build docker containers for hadoop
docker-compose build 

### start docker containers for hadoop
docker-compose up -d

### Shutdown the container
docker-compose down

### Connect to container
docker exec -it namenode bash

### Check if the csv exist on namenode
docker exec namenode ls -lh /data/input | grep csv

### Copy the file to hadoop bda input directory
docker exec namenode_bda hdfs dfs -put -f /data/input/export_cleaned_masked.csv /user/bda/input/

### Check if the input file copy successfully
docker exec namenode_bda hdfs dfs -ls -h /user/bda/input/

### Check if the output file generated
docker exec namenode_bda hdfs dfs -ls -h /user/bda/input/

### Check HDFS block distribution
docker exec namenode_bda hdfs fsck /user/bda/input/export_cleaned_masked.csv -files -blocks -locations

### Execte map reduce on hadoop
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-input /user/bda/input/export_cleaned_masked.csv \
-output /user/bda/output/mapreduce_results \
-mapper /data/mapreduce/mapper_big.py \
-reducer /data/mapreduce/reducer_big.py \
-file /data/mapreduce/mapper_big.py \
-file /data/mapreduce/reducer_big.py

### Remove input or output directory
hdfs dfs -rm -r -f /user/bda/output/mapreduce_results

### Copy the output to data mount directory for analysis 
hdfs dfs -get /user/bda/output/mapreduce_results/part-00000 /data/output/mapreduce_results_hadoop.csv