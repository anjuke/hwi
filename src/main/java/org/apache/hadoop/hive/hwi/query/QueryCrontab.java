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
