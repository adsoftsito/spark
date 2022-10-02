# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi
PATH=/home/hadoop/hadoop/bin:/home/hadoop/hadoop/sbin:/home/hadoop/spark/bin:$PATH
# User specific environment and startup programs

export HADOOP_CONF_DIR=/home/hadoop/hadoop/etc/hadoop
export SPARK_HOME=/home/hadoop/spark
export LD_LIBRARY_PATH=/home/hadoop/hadoop/lib/native:$LD_LIBRARY_PATH
