package org.apache.hadoop.hive.hwi.query;

import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.HWIUtil;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.MQuery.Status;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.history.HiveHistoryViewer;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class QueryWorker implements Runnable {

	protected static final Log l4j = LogFactory.getLog(QueryWorker.class
			.getName());

	private MQuery mquery;

	private HiveConf hiveConf;

	private QueryStore qs;

	private String historyFile;

	public QueryWorker(MQuery mquery) {
		this.mquery = mquery;
	}

	public void run() {
		init();
		runQueries();
		finish();
	}

	protected Status getStatus() {
		return mquery.getStatus();
	}

	private void init() {
		hiveConf = new HiveConf(SessionState.class);

		SessionState.start(hiveConf);
		historyFile = SessionState.get().getHiveHistory().getHistFileName();

		qs = QueryStore.getInstance();
	}

	/**
	 * run user input queries
	 */
	public void runQueries() {
		mquery.setResultLocation("/user/hive/result/" + mquery.getId() + "/");
		mquery.setStatus(MQuery.Status.RUNNING);
		qs.updateQuery(mquery);

		ArrayList<String> queries = new ArrayList<String>();

		// query is not safe ! safe it !
		String queryStr = HWIUtil.getSafeQuery(mquery.getQuery());

		if (queryStr.contains("hiveconf")) {
			Date d = new Date();
			SimpleDateFormat ft = new SimpleDateFormat("yyyy");
			queries.add("set year=" + ft.format(d));
			ft = new SimpleDateFormat("MM");
			queries.add("set month=" + ft.format(d));
			ft = new SimpleDateFormat("dd");
			queries.add("set day=" + ft.format(d));
			ft = new SimpleDateFormat("HH");
			queries.add("set hour=" + ft.format(d));
			ft = new SimpleDateFormat("mm");
			queries.add("set minute=" + ft.format(d));
			ft = new SimpleDateFormat("ss");
			queries.add("set second=" + ft.format(d));
		}
		
		queries.addAll(Arrays.asList(queryStr.split(";")));

		long start_time = System.currentTimeMillis();
		for (String query : queries) {
			if ("".equals(query))
				continue;
			try {
				CommandProcessorResponse resp = runQuery(query,
						mquery.getResultLocation());
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

	protected CommandProcessorResponse runQuery(String cmd,
			String resultLocation) throws RuntimeException,
			CommandNeedRetryException {
		String cmd_trimmed = cmd.trim();
		String[] tokens = cmd_trimmed.split("\\s+");

		if ("select".equalsIgnoreCase(tokens[0])) {
			cmd_trimmed = "INSERT OVERWRITE DIRECTORY '" + resultLocation
					+ "' " + cmd_trimmed;
		}

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
				resp = qp.run(cmd_trimmed);
			} catch (CommandNeedRetryException e) {
				throw e;
			} finally {
				qp.close();
			}
		} else {
			try {
				String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
				resp = proc.run(cmd_1);
			} catch (CommandNeedRetryException e) {
				throw e;
			}
		}

		return resp;
	}

	protected void running() {
		HiveHistoryViewer hv = HWIUtil.getHiveHistoryViewer(historyFile);

		String jobId = HWIUtil.getJobId(hv);

		if (jobId != null && !jobId.equals("")
				&& !jobId.equals(mquery.getJobId())) {
			mquery.setJobId(jobId);
			QueryStore.getInstance().updateQuery(mquery);
		}

	}

	/**
	 * query finished
	 * 
	 */
	private void finish() {

		HiveHistoryViewer hv = HWIUtil.getHiveHistoryViewer(historyFile);

		String jobId = HWIUtil.getJobId(hv);

		if (jobId != null && !jobId.equals("")
				&& !jobId.equals(mquery.getJobId())) {
			mquery.setJobId(jobId);
		}

		Integer cpuTime = HWIUtil.getCpuTime(hv);

		if (cpuTime != null && cpuTime > 0) {
			mquery.setCpuTime(cpuTime);
		}

		if (mquery.getErrorCode() == null || mquery.getErrorCode() == 0) {
			mquery.setStatus(MQuery.Status.FINISHED);
		} else {
			mquery.setStatus(MQuery.Status.FAILED);
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

}
