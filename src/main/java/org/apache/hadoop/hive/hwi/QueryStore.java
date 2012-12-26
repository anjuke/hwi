package org.apache.hadoop.hive.hwi;

import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;

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
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MType;

public class QueryStore {

  private static Properties prop = null;

  private PersistenceManager pm = null;

  private static PersistenceManagerFactory pmf = null;

  private boolean isInitialized = false;

  private static final Log LOG = LogFactory.getLog(QueryStore.class.getName());

  private static final Map<String, Class> PINCLASSMAP;
  static {
    Map<String, Class> map = new HashMap();
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


  public QueryStore(Configuration conf) {
    prop = getDataSourceProps(conf);
    initialize(prop);
  }

  private void initialize(Properties dsProps) {
    prop = dsProps;
    pm = getPersistenceManager();
    isInitialized = pm != null;
    return;
  }

  public PersistenceManager getPersistenceManager() {
    return getPMF().getPersistenceManager();
  }

  /**
   * Properties specified in hive-default.xml override the properties specified
   * in jpox.properties.
   */
  @SuppressWarnings("nls")
  private static Properties getDataSourceProps(Configuration conf) {
    Properties prop = new Properties();

    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      if (e.getKey().contains("datanucleus") || e.getKey().contains("jdo")) {
        Object prevVal = prop.setProperty(e.getKey(), conf.get(e.getKey()));
        if (LOG.isDebugEnabled()
            && !e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
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

  private static PersistenceManagerFactory getPMF() {
    if (pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if (dsc != null) {
        HiveConf conf = new HiveConf(ObjectStore.class);
        String objTypes = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CACHE_PINOBJTYPES);
        LOG.info("Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes=\"" + objTypes + "\"");
        if (objTypes != null && objTypes.length() > 0) {
          objTypes = objTypes.toLowerCase();
          String[] typeTokens = objTypes.split(",");
          for (String type : typeTokens) {
            type = type.trim();
            if (PINCLASSMAP.containsKey(type)) {
              dsc.pinAll(true, PINCLASSMAP.get(type));
            }
            else {
              LOG.warn(type + " is not one of the pinnable object types: " + org.apache.commons.lang.StringUtils.join(PINCLASSMAP.keySet(), " "));
            }
          }
        }
      } else {
        LOG.warn("PersistenceManagerFactory returned null DataStoreCache object. Unable to initialize object pin types defined by hive.metastore.cache.pinobjtypes");
      }
    }
    return pmf;
  }

  /**
   * new query
   * @param mquery
   */
  public void insertQuery(MQuery mquery) {
    Transaction tx = pm.currentTransaction();
    tx.begin();
    pm.makePersistent(mquery);
    tx.commit();
  }

  /**
   *
   * @param mquery
   */
  public void updateQuery(MQuery mquery) {
    Transaction tx = pm.currentTransaction();
    tx.begin();
    MQuery nmquery = getById(mquery.getId());
    nmquery.setCallback(mquery.getCallback());
    nmquery.setDescription(mquery.getDescription());
    nmquery.setErrorCode(mquery.getErrorCode());
    nmquery.setErrorMsg(mquery.getErrorMsg());
    nmquery.setJobId(mquery.getJobId());
    nmquery.setName(mquery.getName());
    nmquery.setQuery(mquery.getQuery());
    nmquery.setResultLocation(mquery.getResultLocation());
    nmquery.setStatus(mquery.getStatus());
    nmquery.setUpdated(Calendar.getInstance(TimeZone.getDefault()).getTime());
    nmquery.setCpuTime(mquery.getCpuTime());
    nmquery.setTotalTime(mquery.getTotalTime());
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
    Query query = pm.newQuery(MQuery.class);
    query.setOrdering("id DESC");
    return new Pagination<MQuery>(query, page, pageSize);
  }
  
  /**
   *
   * @param queryId
   * @return
   */
  public MQuery getById(Integer queryId) {

    Query query = pm.newQuery(MQuery.class, "id == :id ");
    query.setUnique(true);

    return (MQuery) query.execute(queryId);
  }

  public static void main(String[] args) {
    try {
      Properties properties = new Properties();
      properties.put("com.sun.jdori.option.ConnectionCreate", "true");
      PersistenceManagerFactory pmf =
              JDOHelper.getPersistenceManagerFactory(properties);
      PersistenceManager pm = pmf.getPersistenceManager();
      Transaction tx = pm.currentTransaction();
      tx.begin();
      MQuery mquery = new MQuery();
      mquery.setName("xxx");
      pm.makePersistent(mquery);
      tx.commit();
    } catch (Exception e) {
        System.out.println("Problem creating database");
        e.printStackTrace();
    }

  }

}
