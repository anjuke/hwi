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

**Download**

    $ git clone git@github.com:anjuke/hwi.git
  
**Package**

    $ cd hwi
    $ mvn clean package

**Deploy**

    $ cp target/hive-hwi-1.0*.war ${HIVE_HOME}/lib/

    **Pay attention!** Maybe you prefer not to simply overide conf/hive-site.xml
  
    $ cp src/main/resources/hive-site.xml.template ${HIVE_HOME}/conf/hive-site.xml
  
**Run**

    $ hive --service hwi
