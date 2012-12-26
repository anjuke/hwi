package org.apache.hadoop.hive.hwi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

public class TestEverything {

	public static void testHistoryFile() throws IOException {
		String s = "1\r2\n3";
		System.out.println(s.replace('\r', ' ').replace('\n', ' '));
	}

	public static String readFile(String path) throws IOException {
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
	
	public static void main(String[] args) throws Exception {
		testGetDataNodeURL();
	}
}
