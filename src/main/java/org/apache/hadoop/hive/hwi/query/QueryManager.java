/**
 * Copyright (C) [2013] [Anjuke Inc]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.hwi.query;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.query.RunningRunner.Running;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QueryManager {
    protected static final Log l4j = LogFactory.getLog(QueryManager.class
            .getName());

    private static QueryManager instance;

    private Scheduler scheduler;
    private RunningRunner runner;

    private QueryManager() {
    }

    public static QueryManager getInstance() {
        if (instance == null) {
            synchronized (QueryManager.class) {
                if (instance == null) {
                    instance = new QueryManager();
                }
            }
        }
        return instance;
    }

    public void start() {
        try {
            l4j.info("QueryManager starting.");
            startScheduler();
            loadCrontabs();
            startRunner();
            l4j.info("QueryManager started.");
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager failed to start.");
        }
    }

    protected void startScheduler() throws SchedulerException {
        Properties props = new Properties();
        props.setProperty("org.quartz.threadPool.threadCount", "30");

        StdSchedulerFactory ssf = new StdSchedulerFactory(props);
        scheduler = ssf.getScheduler();
        scheduler.start();
    }

    protected void loadCrontabs() {
        List<MCrontab> crontabs = QueryStore.getInstance().runningCrontabs();
        for (MCrontab crontab : crontabs) {
            schedule(crontab);
        }
        l4j.info("Crontabs loaded.");
    }

    protected void startRunner() {
        runner = new RunningRunner();
        runner.start();
    }

    public boolean submit(MQuery mquery) {
        if (scheduler == null)
            return false;

        if (mquery.getId() == null)
            return false;

        JobDetail job = JobBuilder.newJob(QueryRunner.class)
                .withIdentity(mquery.getId().toString(), mquery.getUserId())
                .build();

        JobDataMap map = job.getJobDataMap();
        map.put("mqueryId", mquery.getId());

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(mquery.getId().toString(), mquery.getUserId())
                .startNow().build();

        try {
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager failed to schedule.");
            return false;
        }

        return true;
    }

    public boolean schedule(MCrontab crontab) {
        if (scheduler == null)
            return false;

        if (crontab.getId() == null)
            return false;

        JobDetail job = JobBuilder.newJob(QueryCrontab.class)
                .withIdentity(crontab.getId().toString(), crontab.getUserId())
                .build();

        JobDataMap map = job.getJobDataMap();
        map.put("crontabId", crontab.getId());

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity(crontab.getId().toString(), crontab.getUserId())
                .withSchedule(
                        CronScheduleBuilder.cronSchedule(crontab.getCrontab()))
                .build();

        try {
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager failed to schedule.");
            return false;
        }

        return true;
    }

    public boolean unschedule(MCrontab crontab) {
        if (scheduler == null)
            return false;

        if (crontab.getId() == null)
            return false;

        try {
            scheduler.deleteJob(new JobKey(crontab.getId().toString(), crontab
                    .getUserId()));
            return true;
        } catch (SchedulerException e) {
            e.printStackTrace();
            l4j.error("QueryManager unschedule failed.");
            return false;
        }
    }

    public boolean monitor(Running running) {
        if (runner == null)
            return false;

        runner.add(running);
        return true;
    }

    public void shutdown() {
        l4j.info("QueryManager shutting down.");

        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                e.printStackTrace();
                l4j.error("scheduler failed to shutdown.");
            }
        }

        if (runner != null)
            runner.shutdown();

        l4j.info("QueryManager shutdown complete.");
    }

}
