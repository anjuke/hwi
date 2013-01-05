package org.apache.hadoop.hive.hwi.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.Pagination;
import org.apache.hadoop.hive.ql.session.SessionState;

public class QueryStore {
    private static final Log l4j = LogFactory
            .getLog(QueryStore.class.getName());

    private static QueryStore instance;

    private PersistenceManagerFactory pmf;

    private ThreadLocal<PersistenceManager> pm = new ThreadLocal<PersistenceManager>();

    private QueryStore() {
        HiveConf hiveConf = new HiveConf(SessionState.class);
        Properties props = getDataSourceProps(hiveConf);
        pmf = JDOHelper.getPersistenceManagerFactory(props);
    }

    public static QueryStore getInstance() {
        if (instance == null) {
            synchronized (QueryStore.class) {
                if (instance == null) {
                    instance = new QueryStore();
                }
            }
        }
        return instance;
    }

    /**
     * Properties specified in hive-default.xml override the properties
     * specified in jpox.properties.
     */
    private static Properties getDataSourceProps(Configuration conf) {
        Properties prop = new Properties();

        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> e = iter.next();
            if (e.getKey().contains("datanucleus")
                    || e.getKey().contains("jdo")) {
                Object prevVal = prop.setProperty(e.getKey(),
                        conf.get(e.getKey()));
                if (l4j.isDebugEnabled()
                        && !e.getKey().equals(
                                HiveConf.ConfVars.METASTOREPWD.varname)) {
                    l4j.debug("Overriding " + e.getKey() + " value " + prevVal
                            + " from  jpox.properties with " + e.getValue());
                }
            }
        }

        if (l4j.isDebugEnabled()) {
            for (Entry<Object, Object> e : prop.entrySet()) {
                if (!e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
                    l4j.debug(e.getKey() + " = " + e.getValue());
                }
            }
        }
        return prop;
    }

    public PersistenceManagerFactory getPMF() {
        return pmf;
    }

    public PersistenceManager getPM() {
        if (pm.get() == null || pm.get().isClosed()) {
            pm.set(getPMF().getPersistenceManager());
        }
        return pm.get();
    }

    public void shutdown() {
        if (pm.get() != null) {
            pm.get().close();
        }
    }

    /**
     * new query
     * 
     * @param mquery
     */
    public void insertQuery(MQuery mquery) {
        Transaction tx = getPM().currentTransaction();
        tx.begin();
        getPM().makePersistent(mquery);
        tx.commit();
    }

    /**
     * 
     * @param mquery
     */
    public void updateQuery(MQuery mquery) {
        Transaction tx = getPM().currentTransaction();
        MQuery query = getPM().getObjectById(MQuery.class, mquery.getId());
        query.copy(mquery);
        tx.begin();
        getPM().makePersistent(query);
        tx.commit();
    }

    /**
     * paginate by page and pageSize
     * 
     * @param page
     * @param pageSize
     * @return
     */
    public Pagination<MQuery> paginate(int page, int pageSize) {
        Query query = getPM().newQuery(MQuery.class);
        query.setOrdering("id DESC");
        return paginate(query, null, page, pageSize);
    }

    public Pagination<MQuery> paginate(Query query, Map<String, Object> map,
            int page, int pageSize) {
        return new Pagination<MQuery>(query, map, page, pageSize);
    }

    /**
     * 
     * @param queryId
     * @return
     */
    public MQuery getById(Integer queryId) {
        Query query = getPM().newQuery(MQuery.class, "id == :id ");
        query.setUnique(true);
        return (MQuery) query.execute(queryId);
    }

    public void insertCrontab(MCrontab crontab) {
        Transaction tx = getPM().currentTransaction();
        tx.begin();
        getPM().makePersistent(crontab);
        tx.commit();
    }

    public MCrontab getCrontabById(Integer crontabId) {
        Query query = getPM().newQuery(MCrontab.class, "id == :id ");
        query.setUnique(true);
        return (MCrontab) query.execute(crontabId);
    }

    public Pagination<MCrontab> crontabPaginate(int page, int pageSize) {
        Query query = getPM().newQuery(MCrontab.class);
        query.setOrdering("id DESC");
        return crontabPaginate(query, null, page, pageSize);
    }

    public Pagination<MCrontab> crontabPaginate(Query query,
            Map<String, Object> map, int page, int pageSize) {
        return new Pagination<MCrontab>(query, map, page, pageSize);
    }

    @SuppressWarnings("unchecked")
    public List<MCrontab> runningCrontabs() {
        Query query = getPM().newQuery(MCrontab.class);
        query.setFilter("status == :status");
        query.setOrdering("id DESC");
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("status", MCrontab.Status.RUNNING);
        return (List<MCrontab>) query.executeWithMap(map);
    }

    public void updateCrontab(MCrontab mcrontab) {
        Transaction tx = getPM().currentTransaction();
        MCrontab crontab = getPM().getObjectById(MCrontab.class,
                mcrontab.getId());
        crontab.copy(mcrontab);
        tx.begin();
        getPM().makePersistent(crontab);
        tx.commit();
    }
}
