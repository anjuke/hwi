# Hwi

A New Hive Web Interface

## Features

 * Seamless intergration with existing hive deployment
 * Save user queries in database
 * Save hive query result in hdfs
 * More information about hive query:related job, cpu time, total time
 * Callback when hive query is completed
 * View hive query result summary in web interface 
 * Support download full result according Hwi

## How to use

    $ git clone git@github.com:anjuke/hwi.git
    $ cd hwi 
    $ mvn package
    $ cp target/hive-hwi-1.0*.jar ${HIVE_HOME}/lib/
    $ cp target/hive-hwi-1.0*.war ${HIVE_HOME}/lib/
    $ mv ${HIVE_HOME}/lib/hive-hwi*jar ${HIVE_HOME}/lib/hive-hwi-origin.jar.bak #back up original hwi jar
    $ mv ${HIVE_HOME}/lib/hive-hwi*war ${HIVE_HOME}/lib/hive-hwi-origin.war.bak #back up original hwi war
    $ nohup hive --service hwi > /dev/null 2>&1 &

