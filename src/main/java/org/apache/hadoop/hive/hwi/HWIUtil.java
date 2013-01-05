package org.apache.hadoop.hive.hwi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.history.HiveHistory.TaskInfo;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HWIUtil {

    protected static final Log l4j = LogFactory.getLog(HWIUtil.class.getName());

    public static String getJobTrackerURL(String jobid) {
        HiveConf conf = new HiveConf(SessionState.class);
        String jt = conf.get("mapred.job.tracker");
        String jth = conf.get("mapred.job.tracker.http.address");
        String[] jtparts = null;
        String[] jthttpParts = null;
        if (jt.equalsIgnoreCase("local")) {
            jtparts = new String[2];
            jtparts[0] = "local";
            jtparts[1] = "";
        } else {
            jtparts = jt.split(":");
        }
        if (jth.contains(":")) {
            jthttpParts = jth.split(":");
        } else {
            jthttpParts = new String[2];
            jthttpParts[0] = jth;
            jthttpParts[1] = "";
        }
        return "http://" + jtparts[0] + ":" + jthttpParts[1]
                + "/jobdetails.jsp?jobid=" + jobid + "&refresh=30";
    }

    /*
     * incorrect, datanode can't be random
     */
    public static String getDataNodeURL(String path) {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-default.xml");
        conf.addResource("hdfs-site.xml");

        String nnHttp = conf.get("dfs.http.address");
        String dnHttp = conf.get("dfs.datanode.http.address");

        String host = "";
        try {
            ClassLoader classLoader = Thread.currentThread()
                    .getContextClassLoader();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    classLoader.getResource("slaves").openStream()));
            host = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            l4j.error(e.getMessage());
        }

        String nnPort = "";
        if (nnHttp.contains(":")) {
            nnPort = nnHttp.split(":")[1];
        }

        String dnPort = "";
        if (dnHttp.contains(":")) {
            dnPort = dnHttp.split(":")[1];
        }

        return "http://" + host + ":" + dnPort
                + "/browseDirectory.jsp?namenodeInfoPort=" + nnPort + "&dir="
                + path;
    }

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
