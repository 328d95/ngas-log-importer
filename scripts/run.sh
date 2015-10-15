<path to spark>/bin/spark-submit \
  --master local[6] \
  --executor-memory 1G \
  --packages org.elasticsearch:elasticsearch-spark_2.10:2.2.0-m1 \
  --conf spark.es.nodes=localhost:9200 \
  <path to executable jar> \
  <path to log files> \
  #$(date +%s -d "-1 day") # process files with mod date > date
