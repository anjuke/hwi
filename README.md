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

## Implemention

Following frameworks are used:

* [Twitter Bootstrap](http://twitter.github.com/bootstrap/) for html, css, js
* [Velocity](http://velocity.apache.org/) for view templating
* [Jersey](http://jersey.java.net/) for url routing
* [Quartz](http://quartz-scheduler.org/) for queries scheduling

## How to use

**Download**

    $ git clone git@github.com:anjuke/hwi.git
  
**Package**

    $ cd hwi
    $ mvn clean package war:war

**Deploy**

    $ mv ${HIVE_HOME}/lib/hive-hwi-x.jar ${HIVE_HOME}/lib/hive-hwi-x.jar.bak
    $ mv ${HIVE_HOME}/lib/hive-hwi-x.war ${HIVE_HOME}/lib/hive-hwi-x.war.bak

    $ cp target/hive-hwi-1.0.jar ${HIVE_HOME}/lib/hive-hwi-1.0.jar
    $ cp target/hive-hwi-1.0.war ${HIVE_HOME}/lib/hive-hwi-1.0.war

    **Pay attention!** Maybe you prefer not to simply overide conf/hive-site.xml
  
    $ cp src/main/resources/hive-site.xml.template ${HIVE_HOME}/conf/hive-site.xml
  
**Run**

    $ hive --service hwi

## Develop

**Create Database**

    $ create database hive
    $ create user 'hive'@'localhost' identified by password 'hive'
    $ grant all on hive.* to 'hive'@'localhost'
    $ flush privileges

**config file**

```
cp src/main/resources/hive-site.xml.template src/main/resources/hive-site.xml
vim src/main/resources/hive-site.xml

cp src/main/resources/hive-site.xml {$HIVE_HOME}/conf/
cp ~/.m2/repository/mysql/mysql-connector-java/5.1.22/mysql-connector-java-5.1.22.jar {$HIVE_HOME}/lib/
```


**Install DataNucleus**
    
* install url is `http://www.datanucleus.org/downloads/eclipse-update/`

**Create Eclipse Configuration**

    $ mvn eclipse:eclipse

**Import Hwi Maven Project**

1. eclipse import maven project
2. please use java environment
3. hwi project right click, DataNuclens => Add DataNuclens Support 
4. hwi project right click, DataNuclens => Enable Auto-Enhancement

**run project**

1. hadoop fs -mkdir /user/hive/result
2. use TestRunHWIServer.java to run as java application
3. you can open your browser, input "http://localhost:9999/hwi/"
