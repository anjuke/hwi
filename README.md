# Hwi

A New Hive Web Interface

## Features

 * Save user queries in database
 * More information about hive query:related job, cpu time, total time
 * Support callback when hive query completed.

## How to use
    $ git clone git@github.com:anjuke/hwi.git
    $ cd hwi 
    $ mvn package
    $ cp target/hive-hwi-1.0*.jar ${HIVE_HOME}/lib/
    $ cp target/hive-hwi-1.0*.war ${HIVE_HOME}/lib/
    $ nohup hive --service hwi > /dev/null 2>&1 &

