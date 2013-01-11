package org.apache.hadoop.hive.hwi.query;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QueryCrontab implements Job {
    protected static final Log l4j = LogFactory.getLog(QueryManager.class
            .getName());

    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        JobDataMap map = context.getJobDetail().getJobDataMap();

        int crontabId = map.getIntValue("crontabId");

        MCrontab crontab = QueryStore.getInstance().getCrontabById(crontabId);

        Date created = Calendar.getInstance(TimeZone.getDefault()).getTime();
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        String name = "[" + crontab.getName() + "] " + sf.format(created);

        MQuery query = new MQuery(name, crontab.getQuery(),
                crontab.getCallback(), crontab.getUserId(),
                crontab.getGroupId());

        query.setCrontabId(crontabId);
        QueryStore.getInstance().insertQuery(query);

        QueryManager.getInstance().submit(query);
    }
}
