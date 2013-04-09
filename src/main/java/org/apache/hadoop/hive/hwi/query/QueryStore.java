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
        
        try {
            tx.begin();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        try {
            getPM().makePersistent(mquery);
        } catch (Exception e) {
            e.printStackTrace();
        }
    
        try {
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            tx.rollback();
        }
    }

    /**
     * 
     * @param mquery
     */
    public void copyAndUpdateQuery(MQuery mquery) {
            
        try {
            l4j.debug("mquery classloader " + mquery.getClass().getClassLoader());
            
            Transaction tx = getPM().currentTransaction();
            MQuery query = getPM().getObjectById(MQuery.class, mquery.getId());
            
            l4j.debug("query classloader " + query.getClass().getClassLoader());
            
            query.copy(mquery);
            
            try {
                tx.begin();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            
            try {
                getPM().makePersistent(query);
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            try {
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateQuery(MQuery mquery) {
        Transaction tx = null;
        try {
            tx = getPM().currentTransaction();
            tx.begin();
        } catch (Exception e) {
            e.printStackTrace();
            return ;
        }
        
        try {
            getPM().makePersistent(mquery);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }
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
        
        Object obj = query.execute(queryId);
        
        l4j.debug("---- getById start ----");
        
        l4j.debug("object class loader: " + obj.getClass().getClassLoader());
        l4j.debug("Query  class loader:" + Query.class.getClassLoader());
        l4j.debug("query  class loader:" + query.getClass().getClassLoader());
        l4j.debug("MQuery class loader: " + MQuery.class.getClassLoader());
        
        l4j.debug("---- getById   end ----");
        
        return (MQuery) obj;
    }

    public void insertCrontab(MCrontab crontab) {
        
        Transaction tx = getPM().currentTransaction();
        
        try {
            tx.begin();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        try {
            getPM().makePersistent(crontab);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }
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
        
        try {
            tx.begin();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        try {
            getPM().makePersistent(crontab);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }
    }
}
