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
	
	public Pagination(Query query, Map<String, Object> map, int page, int pageSize){
		this.query = query;
		this.map = map;
		this.page = page;
		this.pageSize = pageSize;
	}

	public Query getQuery(){
		return query;
	}
	
	public Map<String, Object> getMap() {
		return map;
	}

	public int getPage(){
		return page;
	}
	
	public int getPageSize(){
		return pageSize;
	}
	
	public Long getTotal(){
		if(total == null){
			Query newQuery = query.getPersistenceManager().newQuery(query);
			newQuery.setResult("COUNT(id)");
			total = (Long)newQuery.executeWithMap(map);
		}
		return total;
	}
	
	@SuppressWarnings("unchecked")
	public List<T> getItems(){
		if(items == null){
			Query newQuery = query.getPersistenceManager().newQuery(query);
			int offset = (page - 1) * pageSize;
			newQuery.setRange(offset, offset + pageSize);
			items = (List<T>) newQuery.executeWithMap(map);
		}
		return items;
	}

	public int getPages(){
		return (int) Math.ceil(getTotal() / pageSize);
	}
}
