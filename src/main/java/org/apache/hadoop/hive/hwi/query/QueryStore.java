package org.apache.hadoop.hive.hwi.query;

import java.util.Collections;
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
import javax.jdo.datastore.DataStoreCache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hwi.model.MCrontab;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.Pagination;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.ql.session.SessionState;

public class QueryStore {
    private static final Log LOG = LogFactory
            .getLog(QueryStore.class.getName());

    private static QueryStore instance;

    private PersistenceManagerFactory pmf;

    private ThreadLocal<PersistenceManager> pm = new ThreadLocal<PersistenceManager>();

    @SuppressWarnings("rawtypes")
    private static final Map<String, Class> PINCLASSMAP;
    static {
        @SuppressWarnings("rawtypes")
        Map<String, Class> map = new HashMap<String, Class>();
        map.put("table", MTable.class);
        map.put("storagedescriptor", MStorageDescriptor.class);
        map.put("serdeinfo", MSerDeInfo.class);
        map.put("partition", MPartition.class);
        map.put("database", MDatabase.class);
        map.put("type", MType.class);
        map.put("fieldschema", MFieldSchema.class);
        map.put("order", MOrder.class);
        PINCLASSMAP = Collections.unmodifiableMap(map);
    }

    private QueryStore() {
        pmf = getPMF();
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
    @SuppressWarnings("nls")
    private static Properties getDataSourceProps(Configuration conf) {
        Properties prop = new Properties();

        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> e = iter.next();
            if (e.getKey().contains("datanucleus")
                    || e.getKey().contains("jdo")) {
                Object prevVal = prop.setProperty(e.getKey(),
                        conf.get(e.getKey()));
                if (LOG.isDebugEnabled()
                        && !e.getKey().equals(
                                HiveConf.ConfVars.METASTOREPWD.varname)) {
                    LOG.debug("Overriding " + e.getKey() + " value " + prevVal
                            + " from  jpox.properties with " + e.getValue());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            for (Entry<Object, Object> e : prop.entrySet()) {
                if (!e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
                    LOG.debug(e.getKey() + " = " + e.getValue());
                }
            }
        }
        return prop;
    }

    private PersistenceManagerFactory getPMF() {
        HiveConf hiveConf = new HiveConf(SessionState.class);
        Properties props = getDataSourceProps(hiveConf);

        PersistenceManagerFactory pmf = JDOHelper
                .getPersistenceManagerFactory(props);

        DataStoreCache dsc = pmf.getDataStoreCache();
        if (dsc != null) {
            HiveConf conf = new HiveConf(ObjectStore.class);
            String objTypes = HiveConf.getVar(conf,
                    HiveConf.ConfVars.METASTORE_CACHE_PINOBJTYPES);
            LOG.info("Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes=\""
                    + objTypes + "\"");
            if (objTypes != null && objTypes.length() > 0) {
                objTypes = objTypes.toLowerCase();
                String[] typeTokens = objTypes.split(",");
                for (String type : typeTokens) {
                    type = type.trim();
                    if (PINCLASSMAP.containsKey(type)) {
                        dsc.pinAll(true, PINCLASSMAP.get(type));
                    } else {
                        LOG.warn(type
                                + " is not one of the pinnable object types: "
                                + org.apache.commons.lang.StringUtils.join(
                                        PINCLASSMAP.keySet(), " "));
                    }
                }
            }
        } else {
            LOG.warn("PersistenceManagerFactory returned null DataStoreCache object. Unable to initialize object pin types defined by hive.metastore.cache.pinobjtypes");
        }

        return pmf;
    }

    public PersistenceManager getPM() {
        if (pm.get() == null) {
            pm.set(pmf.getPersistenceManager());
        }
        return pm.get();
    }

    public void close() {
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
        tx.begin();
        getPM().makePersistent(mquery);
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

    public void updateCrontab(MCrontab crontab) {
        Transaction tx = getPM().currentTransaction();
        tx.begin();
        getPM().makePersistent(crontab);
        tx.commit();
    }
}
