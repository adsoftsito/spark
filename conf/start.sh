start-dfs.sh
echo "hdfs started... "
start-yarn.sh
echo "yarn started... "

echo "hadoop running : http://34.125.132.131:8088/"
$SPARK_HOME/sbin/start-history-server.sh
echo "spark running : http://34.125.132.131:18080/"

