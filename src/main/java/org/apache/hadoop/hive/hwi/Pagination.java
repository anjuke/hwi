package org.apache.hadoop.hive.hwi;

import java.util.List;

import javax.jdo.Query;

public class Pagination<T> {

	private final Query query;
	private final int page;
	private final int pageSize;
	private Long total;
	private List<T> items;
	
	public Pagination(Query query, int page, int pageSize){
		this.query = query;
		this.page = page;
		this.pageSize = pageSize;
	}

	public Query getQuery(){
		return query;
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
			total = (Long)newQuery.execute();
		}
		return total;
	}
	
	@SuppressWarnings("unchecked")
	public List<T> getItems(){
		if(items == null){
			Query newQuery = query.getPersistenceManager().newQuery(query);
			int offset = (page - 1) * pageSize;
			newQuery.setRange(offset, offset + pageSize);
			items = (List<T>) newQuery.execute();
		}
		return items;
	}

	public int getPages(){
		return (int) Math.ceil(getTotal() / pageSize);
	}
}
