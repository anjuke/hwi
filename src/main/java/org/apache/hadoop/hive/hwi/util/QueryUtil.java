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
package org.apache.hadoop.hive.hwi.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.history.HiveHistory.TaskInfo;

public class QueryUtil {

    protected static final Log l4j = LogFactory.getLog(QueryUtil.class
            .getName());

    public static HWIHiveHistoryViewer getHiveHistoryViewer(String historyFile) {
        if (historyFile == null) {
            return null;
        }

        try {
            HWIHiveHistoryViewer hv = new HWIHiveHistoryViewer(historyFile);
            return hv;
        } catch (Exception e) {
            e.printStackTrace();
            l4j.error(e.getMessage());
            return null;
        }
    }

    public static String getJobId(HWIHiveHistoryViewer hv) {
        if (hv == null) {
            return null;
        }

        String jobId = "";

        for (String taskKey : hv.getTaskInfoMap().keySet()) {
            TaskInfo ti = hv.getTaskInfoMap().get(taskKey);
            for (String tiKey : ti.hm.keySet()) {
                l4j.debug(tiKey + ":" + ti.hm.get(tiKey));

                if (tiKey.equalsIgnoreCase("TASK_HADOOP_ID")) {
                    String tid = ti.hm.get(tiKey);
                    if (!jobId.contains(tid)) {
                        jobId = jobId + tid + ";";
                    }
                }
            }
        }

        return jobId;
    }

    public static Integer getCpuTime(HWIHiveHistoryViewer hv) {
        if (hv == null) {
            return null;
        }

        int cpuTime = 0;

        Pattern pattern = Pattern
                .compile("Map-Reduce Framework.CPU time spent \\(ms\\):(\\d+),");

        for (String taskKey : hv.getTaskInfoMap().keySet()) {
            TaskInfo ti = hv.getTaskInfoMap().get(taskKey);
            for (String tiKey : ti.hm.keySet()) {
                if (tiKey.equalsIgnoreCase("TASK_COUNTERS")) {
                    l4j.debug(tiKey + ":" + ti.hm.get(tiKey));

                    Matcher matcher = pattern.matcher(ti.hm.get(tiKey));
                    if (matcher.find()) {
                        try {
                            cpuTime += Integer.parseInt(matcher.group(1));
                        } catch (NumberFormatException e) {
                            l4j.error(matcher.group(1) + " is not int");
                        }
                    }
                }
            }
        }

        return cpuTime;
    }

    public static String getSafeQuery(String query) {
        query = query.replaceAll("\r|\n", " ");
        return query;
    }

}
