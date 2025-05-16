
# This script is used to set up a local Hadoop and Tez environment for running a simple word count example.
# Prerequisites
# 1. java is installed and JAVA_HOME is set
# 2. ssh localhost works without password

# configure this if needed, by default it will use the latest stable versions in the current directory
export TEZ_VERSION=$(curl -s "https://downloads.apache.org/tez/" | grep -oP '\K[0-9]+\.[0-9]+\.[0-9]+(?=/)' | sort -V | tail -1) # e.g. 0.10.4
export HADOOP_VERSION=$(curl -s "https://downloads.apache.org/hadoop/common/" | grep -oP 'hadoop-\K[0-9]+\.[0-9]+\.[0-9]+(?=/)' | sort -V | tail -1) # e.g. 3.4.1
export HADOOP_STACK_HOME=$PWD

echo "Demo script is running in $HADOOP_STACK_HOME with TEZ version $TEZ_VERSION and HADOOP version $HADOOP_VERSION"

cd $HADOOP_STACK_HOME
wget -nc https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
wget -nc https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz

if [ ! -d "hadoop-$HADOOP_VERSION" ]; then
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz
fi

if [ ! -d "apache-tez-$TEZ_VERSION-bin" ]; then
    tar -xzf apache-tez-$TEZ_VERSION-bin.tar.gz
fi

ln -s hadoop-$HADOOP_VERSION hadoop
ln -s apache-tez-$TEZ_VERSION-bin tez

export HADOOP_HOME=$HADOOP_STACK_HOME/hadoop
export TEZ_HOME=$HADOOP_STACK_HOME/tez
export HADOOP_CLASSPATH=$TEZ_HOME/*:$TEZ_HOME/lib/*:$TEZ_HOME/conf

export PATH=$PATH:$HADOOP_HOME/bin

# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
cat <<EOF > $HADOOP_HOME/etc/hadoop/hdfs-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
      <name>dfs.replication</name>
      <value>1</value>
  </property>
</configuration>
EOF

cat <<EOF > $HADOOP_HOME/etc/hadoop/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
      <name>fs.defaultFS</name>
      <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

cat <<EOF > $HADOOP_HOME/etc/hadoop/yarn-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
EOF

# optionally stop previous clusters if any
#$HADOOP_HOME/sbin/stop-dfs.sh
#$HADOOP_HOME/sbin/stop-yarn.sh

hdfs namenode -format

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

hadoop fs -mkdir /apps/
hadoop fs -mkdir /apps/tez-$TEZ_VERSION
hadoop fs -copyFromLocal $TEZ_HOME/share/tez.tar.gz /apps/tez-$TEZ_VERSION

# create a simple tez-site.xml
cat <<EOF > $TEZ_HOME/conf/tez-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
      <name>tez.lib.uris</name>
      <value>/apps/tez-$TEZ_VERSION/tez.tar.gz</value>
  </property>
</configuration>
EOF

# create a simple input file
cat <<EOF > ./words.txt
Apple
Banana
Car
Apple
Banana
Car
Dog
Elephant
Friend
Game
EOF

hadoop fs -copyFromLocal words.txt /words.txt

# finally run the example
hadoop jar $TEZ_HOME/tez-examples-$TEZ_VERSION.jar orderedwordcount /words.txt /words_out

# check the output
hadoop fs -ls /words_out
hadoop fs -text /words_out/part-v002-o000-r-00000