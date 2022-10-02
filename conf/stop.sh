$SPARK_HOME/sbin/stop-history-server.sh
echo "spark stopped..."
stop-yarn.sh
echo "yarn stopped "
stop-dfs.sh
echo "dfs stopped... "
