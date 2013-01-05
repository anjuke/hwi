package org.apache.hadoop.hive.hwi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

import javax.jdo.Query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.query.QueryCron;
import org.apache.hadoop.hive.hwi.query.QueryManager;
import org.apache.hadoop.hive.hwi.query.QueryStore;
import org.apache.hadoop.hive.ql.session.SessionState;

public class TestEverything {

	public static void testHistoryFile() throws IOException {
		HWIUtil.getHiveHistoryViewer("/tmp/hadoop/hive_job_log_hadoop_201212280953_1889961092.txt");
	}

	public static String readFile(String path) throws IOException {
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(path));
		StringBuffer str = new StringBuffer();
		String line = br.readLine();
		while (line != null) {
			str.append(line);
			str.append("\n");
			line = br.readLine();
		}

		return str.toString();
	}

	public static void testGetJobTrackerURL() {
		System.out.println(HWIUtil.getJobTrackerURL("aa"));
	}
	
	public static void testGetDataNodeURL() {
		System.out.println(HWIUtil.getDataNodeURL("/tmp"));
	}
	
	public static void testHiveConf(){
		HiveConf conf = new HiveConf(SessionState.class);
		Properties p = conf.getAllProperties();
		for(Object k : p.keySet()){
			System.out.print((String)k + ",");
			System.out.println(p.get(k));
		}
	}
	
	public static void testHadoopConf() throws IOException{
		Configuration conf = new Configuration();
		conf.addResource("hdfs-default.xml");
		conf.addResource("hdfs-site.xml");
		Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
	}
	
	public static void testSlaves() throws IOException{
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		BufferedReader in = new BufferedReader(new InputStreamReader(classLoader.getResource("slaves").openStream()));
		String line = in.readLine();
		System.out.println(line);
	}
	
	public static void testQueryCron() throws InterruptedException{
		QueryManager.getInstance();
		MCrontab ct = new MCrontab("test-query", "select * from test", "", "*/10 * * * * ?", "hadoop");
		QueryStore.getInstance().insertCrontab(ct);
		QueryCron.getInstance().schedule(ct);
		Thread.sleep(30000);
		QueryCron.getInstance().shutdown();
		QueryManager.getInstance().shutdown();
	}
	
	public static void testInt(){
		System.out.println(new Integer(10).toString());
	}
	
	public static void testQuery(){
		QueryStore qs = QueryStore.getInstance();
		Query query = qs.getPM().newQuery(MQuery.class);
		query.setOrdering("id DESC");
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("crontabId", 3);
		query.setResult("COUNT(id)");
		query.executeWithMap(null);
		//Pagination<MQuery> pagination = qs.paginate(query, map, 1, 2);
		//System.out.println(pagination.getTotal());
	}
	
	public static void testCalendar(){
		Calendar c = Calendar.getInstance();
		System.out.println(c.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.CHINA));
	}
	
	public static void testDate(){
		Date d = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(ft.format(d));
	}
	
	public static void testConcurrent(){
		ArrayList<String> l = new ArrayList<String>();
		l.add("1");
		for(String s : l){
			l.remove(s);
		}
	}
	
	public static void main(String[] args) throws Exception {
		testConcurrent();
	}
}
