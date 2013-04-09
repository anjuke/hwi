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

import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.MQuery.Status;
import org.apache.hadoop.hive.hwi.query.RunningRunner.Progress;
import org.apache.hadoop.hive.hwi.query.RunningRunner.Running;
import org.apache.hadoop.hive.hwi.util.HWIHiveHistoryViewer;
import org.apache.hadoop.hive.hwi.util.QueryUtil;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QueryRunner implements Job, Running {
    protected static final Log l4j = LogFactory.getLog(QueryRunner.class
            .getName());

    private JobExecutionContext context;

    private MQuery mquery;

    private HiveConf hiveConf;

    private QueryStore qs;

    private String historyFile;

    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        this.context = context;

        if (init()) {
            QueryManager.getInstance().monitor(this);

            runQuery();
            finish();
        }
    }

    protected boolean init() {
        hiveConf = new HiveConf(SessionState.class);

        SessionState.start(hiveConf);
        historyFile = SessionState.get().getHiveHistory().getHistFileName();

        qs = QueryStore.getInstance();

        JobDataMap map = context.getJobDetail().getJobDataMap();

        int mqueryId = map.getIntValue("mqueryId");

        mquery = QueryStore.getInstance().getById(mqueryId);

        if (mquery == null) {
            l4j.error("MQuery<" + mqueryId + "> is missing");
            return false;
        }

        return true;
    }

    /**
     * run user input queries
     */
    public void runQuery() {
        String result = hiveConf.get("hive.hwi.result", "/user/hive/result");
        mquery.setResultLocation(result + "/" + mquery.getId() + "/");
        mquery.setStatus(Status.RUNNING);
        qs.updateQuery(mquery);

        ArrayList<String> cmds = queryToCmds(mquery);

        long start_time = System.currentTimeMillis();
        for (String cmd : cmds) {
            try {
                CommandProcessorResponse resp = runCmd(cmd);
                mquery.setErrorMsg(resp.getErrorMessage());
                mquery.setErrorCode(resp.getResponseCode());
            } catch (Exception e) {
                mquery.setErrorMsg(e.getMessage());
                mquery.setErrorCode(500);
                break;
            }
        }
        long end_time = System.currentTimeMillis();
        mquery.setTotalTime((int) (end_time - start_time));

        qs.updateQuery(mquery);
    }

    protected ArrayList<String> queryToCmds(MQuery query) {
        
        ArrayList<String> cmds = new ArrayList<String>();
        String resultLocation = query.getResultLocation();

        // query is not safe ! safe it !
        String safeQuery = QueryUtil.getSafeQuery(query.getQuery());
        
        // set map reduce job name
        cmds.add("set mapred.job.name=HWI Query #" + query.getId() + " (" + query.getName() + ")");
        
        // check user date settings
        Pattern setTimePattern = Pattern.compile(
                "set(\\s+)date(\\s)*=(\\s)*(\\+|\\-)?\\s*(\\d+)\\s*(day|hour)(s?)", 
                Pattern.CASE_INSENSITIVE);
        String timeDiffUnit = null;
        int timeDiffValue = 0;
        
        for (String cmd : safeQuery.split(";")) {
            cmd = cmd.trim();
            if (cmd.equals("")) {
                continue;
            }
            
            String prefix = cmd.split("\\s+")[0];
            if ("select".equalsIgnoreCase(prefix)) {
                cmd = "INSERT OVERWRITE DIRECTORY '" + resultLocation + "' "
                        + cmd;
            } else if ("set".equalsIgnoreCase(prefix)) {
                Matcher m = setTimePattern.matcher(cmd);
                if (m.matches()) {
                    if (m.group(4) != null && !"".equals(m.group(4))) {
                        timeDiffValue = Integer.parseInt(m.group(4) + m.group(5));
                    } else {
                        timeDiffValue = Integer.parseInt(m.group(5));
                    }
                    
                    timeDiffUnit = m.group(6);
                }
            }
            
            cmds.add(cmd);
        }
        
        if (safeQuery.contains("hiveconf")) {
            
            Calendar gc = Calendar.getInstance();
            if ("day".equalsIgnoreCase(timeDiffUnit)) {
                gc.add(Calendar.DATE, timeDiffValue);
            } else if("hour".equalsIgnoreCase(timeDiffUnit)) {
                gc.add(Calendar.HOUR, timeDiffValue);
            }
            
            SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddHHmmss");
            String dateStr = ft.format(gc.getTime());
            
            cmds.add(0, "set year=" + dateStr.substring(0, 4));
            cmds.add(0, "set month=" + dateStr.substring(4, 6));
            cmds.add(0, "set day=" + dateStr.substring(6, 8));
            cmds.add(0, "set hour=" + dateStr.substring(8, 10));
            cmds.add(0, "set minute=" + dateStr.substring(10, 12));
            cmds.add(0, "set second=" + dateStr.substring(12, 14));
        }


        return cmds;
    }

    protected CommandProcessorResponse runCmd(String cmd)
            throws RuntimeException, CommandNeedRetryException {
        String[] tokens = cmd.split("\\s+");

        CommandProcessor proc = CommandProcessorFactory
                .get(tokens[0], hiveConf);
        if (proc == null)
            throw new RuntimeException("CommandProcessor for " + tokens[0]
                    + " was not found");

        CommandProcessorResponse resp;

        if (proc instanceof Driver) {
            Driver qp = (Driver) proc;
            qp.setTryCount(Integer.MAX_VALUE);

            try {
                resp = qp.run(cmd);
            } catch (CommandNeedRetryException e) {
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            } finally {
                qp.close();
            }
        } else {
            try {
                resp = proc.run(cmd.substring(tokens[0].length()).trim());
            } catch (CommandNeedRetryException e) {
                throw e;
            }
        }

        return resp;
    }

    /**
     * query finished
     * 
     */
    private void finish() {

        HWIHiveHistoryViewer hv = QueryUtil.getHiveHistoryViewer(historyFile);

        String jobId = QueryUtil.getJobId(hv);

        if (jobId != null && !jobId.equals("")
                && !jobId.equals(mquery.getJobId())) {
            mquery.setJobId(jobId);
        }

        Integer cpuTime = QueryUtil.getCpuTime(hv);

        if (cpuTime != null && cpuTime > 0) {
            mquery.setCpuTime(cpuTime);
        }

        if (mquery.getErrorCode() == null || mquery.getErrorCode() == 0) {
            mquery.setStatus(Status.FINISHED);
        } else {
            mquery.setStatus(Status.FAILED);
        }

        callback();

        qs.updateQuery(mquery);
    }

    /**
     * when query is finished, callback is invoked.
     */
    private void callback() {
        String callback = this.mquery.getCallback();
        if (callback != null && !"".equals(callback)) {
            try {

                String errorCode = "0";
                if (this.mquery.getErrorCode() != null) {
                    errorCode = URLEncoder.encode(this.mquery.getErrorCode()
                            .toString(), "UTF-8");
                }

                String errorMsg = "";
                if (this.mquery.getErrorMsg() != null) {
                    errorMsg = URLEncoder.encode(this.mquery.getErrorMsg(),
                            "UTF-8");
                }

                String postData = "id="
                        + URLEncoder.encode(this.mquery.getId().toString(),
                                "UTF-8")
                        + "&status="
                        + URLEncoder.encode(this.mquery.getStatus().toString(),
                                "UTF-8")
                        + "&error_code="
                        + errorCode
                        + "&error_msg="
                        + errorMsg
                        + "&result_location="
                        + URLEncoder.encode(this.mquery.getResultLocation(),
                                "UTF-8")
                        + "&result_location_url="
                        + URLEncoder.encode(
                                "/hwi/query_result.jsp?action=download&id="
                                        + this.mquery.getId(), "UTF-8");

                int trycallbacktimes = 0;
                do {
                    URL callbackUrl = new URL(callback);

                    HttpURLConnection urlConn = (HttpURLConnection) callbackUrl
                            .openConnection();
                    urlConn.setDoOutput(true);
                    urlConn.connect();

                    OutputStreamWriter out = new OutputStreamWriter(
                            urlConn.getOutputStream(), "UTF-8");
                    out.write(postData);
                    out.close();

                    int responseCode = urlConn.getResponseCode();

                    if (responseCode == 200) {
                        break;
                    }
                } while (++trycallbacktimes < 3);

                /*
                 * l4j.debug(urlConn.getResponseMessage());
                 * l4j.debug(urlConn.getResponseCode()); BufferedReader bin =
                 * new BufferedReader(new
                 * InputStreamReader(urlConn.getInputStream(), "UTF-8")); String
                 * temp; while ((temp = bin.readLine()) != null) {
                 * System.out.println(temp); }
                 */

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        l4j.debug(this.mquery.getName() + " state is now FINISHED");
    }

    public Progress running() {
        switch (mquery.getStatus()) {
        case INITED:
            return Progress.CONTINUE;
        case RUNNING:
            HWIHiveHistoryViewer hv = QueryUtil
                    .getHiveHistoryViewer(historyFile);

            String jobId = QueryUtil.getJobId(hv);

            if (jobId != null && !jobId.equals("")
                    && !jobId.equals(mquery.getJobId())) {
                mquery.setJobId(jobId);
                QueryStore.getInstance().copyAndUpdateQuery(mquery);
            }
            return Progress.CONTINUE;
        default:
            return Progress.EXIT;
        }
    }

}