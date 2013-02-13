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

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.Pagination;
import org.apache.hadoop.hive.hwi.query.QueryManager;
import org.apache.hadoop.hive.hwi.query.QueryStore;
import org.apache.hadoop.hive.hwi.util.HadoopUtil;
import org.apache.hadoop.hive.hwi.util.QueryUtil;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.net.URI;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class TestEverything {
	
    public static void testAPI() {
        Client client = Client.create();
		 
        WebResource webResource = client .resource(getBaseURI());
 
        MultivaluedMap<String,String> formData = new MultivaluedMapImpl();
        formData.add("name", "wanghuida");
        formData.add("query", "select count(1) from pokes");
        formData.add("callback", "http://localhost/abc.php");
        ClientResponse response = webResource.path("queries").path("create").path("api").accept("application/json")
                   .post(ClientResponse.class, formData);
 
        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
        }
 
        String output = response.getEntity(String.class);
 
        System.out.println("Output from Server .... \n");
        System.out.println(output);
    }
	
    private static URI getBaseURI() {
        return UriBuilder.fromUri("http://localhost:9999/hwi").build();
    }

    public static void testHistoryFile() throws IOException {
        QueryUtil
                .getHiveHistoryViewer("/tmp/hadoop/hive_job_log_hadoop_201212280953_1889961092.txt");
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
        System.out.println(HadoopUtil.getJobTrackerURL("aa"));
    }

    public static void testGetDataNodeURL() {
        System.out.println(HadoopUtil.getDataNodeURL("/tmp"));
    }

    public static void testHiveConf() {
        HiveConf conf = new HiveConf(SessionState.class);
        Properties p = conf.getAllProperties();
        for (Object k : p.keySet()) {
            System.out.print((String) k + ",");
            System.out.println(p.get(k));
        }
    }

    public static void testHadoopConf() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-default.xml");
        conf.addResource("hdfs-site.xml");
        Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
    }

    public static void testSlaves() throws IOException {
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                classLoader.getResource("slaves").openStream()));
        String line = in.readLine();
        System.out.println(line);
    }

    public static void testQueryCron() throws InterruptedException {
        QueryManager.getInstance();
        MCrontab ct = new MCrontab("test-query", "select * from test", "",
                "*/10 * * * * ?", "hadoop", "hadoop");
        QueryStore.getInstance().insertCrontab(ct);
        QueryManager.getInstance().schedule(ct);
        Thread.sleep(30000);
        QueryManager.getInstance().shutdown();
        QueryManager.getInstance().shutdown();
    }

    public static void testInt() {
        System.out.println(new Integer(10).toString());
    }

    public static void testQuery() {
        QueryStore qs = QueryStore.getInstance();
        Query query = qs.getPM().newQuery(MQuery.class);
        query.setOrdering("id DESC");
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("crontabId", 3);
        query.setResult("COUNT(id)");
        query.executeWithMap(null);
        // Pagination<MQuery> pagination = qs.paginate(query, map, 1, 2);
        // System.out.println(pagination.getTotal());
    }

    public static void testCalendar() {
        Calendar c = Calendar.getInstance();
        System.out.println(c.getDisplayName(Calendar.MONTH, Calendar.SHORT,
                Locale.CHINA));
    }

    public static void testDate() {
        Date d = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(ft.format(d));
    }

    public static void testConcurrent() {
        ArrayList<String> l = new ArrayList<String>();
        l.add("1");
        for (String s : l) {
            l.remove(s);
        }
    }

    public static void testPersistenceManager() {
        PersistenceManagerFactory pmf = QueryStore.getInstance().getPMF();

        PersistenceManager pm1 = pmf.getPersistenceManager();
        System.out.println(pm1);

        MQuery mquery = pm1.getObjectById(MQuery.class, 204);
        mquery.setCallback("1");

        Transaction tx1 = pm1.currentTransaction();
        tx1.begin();
        pm1.makePersistent(mquery);
        // tx1.commit();

        PersistenceManager pm2 = pmf.getPersistenceManager();
        System.out.println(pm2);

        mquery.setCallback("2");
        MQuery mquery1 = pm2.getObjectById(MQuery.class, mquery.getId());
        mquery1.copy(mquery);
        Transaction tx2 = pm2.currentTransaction();
        tx2.begin();
        pm2.makePersistent(mquery1);
        tx2.commit();

        PersistenceManager pm3 = pmf.getPersistenceManager();
        System.out.println(pm3);
        Query query = pm3.newQuery(MQuery.class);
        query.setOrdering("id DESC");
        Pagination<MQuery> p = new Pagination<MQuery>(query, null, 1, 1);
        System.out.println(p.getItems().get(0).getId());
    }

    public static void testValidateQuery() {
        try {
            // QueryUtil.validateQuery("add jar 1.jar;select * from test;select * from xx;");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testStartStop() {
        QueryManager.getInstance().start();
        QueryManager.getInstance().shutdown();
    }

    public static void testProp() {
        Properties props = new Properties();

        props.put("org.quartz.threadPool.threadCount", "20");
        System.out.println(props
                .getProperty("org.quartz.threadPool.threadCount"));
        props.setProperty("org.quartz.threadPool.threadCount", "20");
        System.out.println(props
                .getProperty("org.quartz.threadPool.threadCount"));
    }

    public static void main(String[] args) throws Exception {
        //testProp();
        testAPI();
    }
}
