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
package org.apache.hadoop.hive.hwi.model;

import java.util.List;
import java.util.Map;

import javax.jdo.Query;

public class Pagination<T> {

    private final Query query;
    private final Map<String, Object> map;
    private final int page;
    private final int pageSize;
    private Long total;
    private List<T> items;

    public Pagination(Query query, Map<String, Object> map, int page,
            int pageSize) {
        this.query = query;
        this.map = map;
        this.page = page;
        this.pageSize = pageSize;
    }

    public Query getQuery() {
        return query;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Long getTotal() {
        if (total == null) {
            Query newQuery = query.getPersistenceManager().newQuery(query);
            newQuery.setOrdering(null);
            newQuery.setResult("COUNT(id)");
            total = (Long) newQuery.executeWithMap(map);
        }
        return total;
    }

    @SuppressWarnings("unchecked")
    public List<T> getItems() {
        if (items == null) {
            Query newQuery = query.getPersistenceManager().newQuery(query);
            int offset = (page - 1) * pageSize;
            newQuery.setRange(offset, offset + pageSize);
            items = (List<T>) newQuery.executeWithMap(map);
        }
        return items;
    }

    public int getPages() {
        return (int) Math.ceil((double) getTotal() / pageSize);
    }
}
