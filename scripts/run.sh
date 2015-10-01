./bin/spark-submit \
  --master local[6] \
  --executor-memory 1G \
  --packages org.elasticsearch:elasticsearch-spark_2.10:2.2.0-m1 \
  --conf spark.es.nodes=localhost:9200 \
  /home/damien/project/spark-1.5.0-bin-hadoop2.6/ngas-log-importer_2.10-1.0.jar \
  file:///home/damien/project/ngaslogs-fe1
