# HWI

A New Hive Web Interface

## Features

**Query**

 * Seamless intergration with existing hive deployment
 * Save user queries in database
 * Save hive query result in hdfs
 * Show progress of queries
 * Summary info, such as cpu time, total time, saved time
 * Callback when hive query is completed
 * Download full result

**Crontab**

 * Create crontabs to run queries automatically

## How to use

    $ git clone git@github.com:anjuke/hwi.git
    $ cd hwi
    $ mvn clean package
    $ cp target/hive-hwi-1.0*.war ${HIVE_HOME}/lib/
    $ hive --service hwi
