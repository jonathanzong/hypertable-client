package com.didactic.htclient.accessor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hypertable.thriftgen.Cell;

/**
 * Class representing a single row result
 * returned by a Hypertable scan.
 *
 */
public class Result {

	private String rowkey;
	private List<Cell> cells;

	/**
	 * Construct a scan result object
	 * 
	 * @param rowkey
	 * @param cells
	 */
	public Result(String rowkey, List<Cell> cells){
		this.rowkey = rowkey;
		this.cells = cells;
	}

	/**
	 * Get the rowkey associated with the result
	 * 
	 * @return
	 */
	public String getRowkey(){
		return this.rowkey;
	}

	/**
	 * Check if the Result object contains
	 * a value for the specified column
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	public boolean containsColumn(String cfam, String cqual){
		return getValue(cfam, cqual) != null;
	}

	public boolean containsFamily(String cfam){
		return getFamily(cfam) != null;
	}

	/**
	 * Returns the most recent value for the specified column
	 * or null if it is not found.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	public String getValue(String cfam, String cqual){
		Cell c = getCell(cfam, cqual);
		return c == null ? null : new String(c.getValue());
	}

	/**
	 * Returns all revisions of a value for the specified column
	 * or null if it is not found.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	public List<String> getValueRevisions(String cfam, String cqual){
		List<Cell> cs = getCellRevisions(cfam, cqual);
		if(cs == null) return null;
		List<String> list = new LinkedList<String>();
		for(Cell c:cs)
			list.add(new String(c.getValue()));
		return list;
	}

	/**
	 * Returns the most recent timestamp for the
	 * specified column, or null if it is not found
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	public Long getTimestamp(String cfam, String cqual){
		Cell c = getCell(cfam, cqual);
		return c == null ? null : c.getKey().getTimestamp();
	}

	/**
	 * Returns timestamps for all revisions of the specified
	 * column, or null if it is not found.
	 *  
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	public List<Long> getTimestampRevisions(String cfam, String cqual){
		List<Cell> cs = getCellRevisions(cfam, cqual);
		if(cs == null) return null;
		List<Long> list = new LinkedList<Long>();
		for(Cell c:cs)
			list.add(c.getKey().getTimestamp());
		return list;
	}

	/**
	 * Performs a linear search to find the most recent
	 * cell for a specified column. Returns null if it is
	 * not found. Operates on the contract that cells are returned
	 * sorted by most recent first.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	private Cell getCell(String cfam, String cqual){
		for(Cell c:cells)
			if(c.getKey().getColumn_family().equals(cfam) && c.getKey().getColumn_qualifier().equals(cqual))
				return c;
		return null;
	}

	/**
	 * Performs a linear search to return all revisions
	 * of a specified column, or null if it is not found.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	private List<Cell> getCellRevisions(String cfam, String cqual){
		List<Cell> list = new LinkedList<Cell>();
		for(Cell c:cells)
			if(c.getKey().getColumn_family().equals(cfam) && c.getKey().getColumn_qualifier().equals(cqual))
				list.add(c);
		return list.isEmpty() ? null : list;
	}
	
	public Map<String,String> getFamily(String cfam){
		List<Cell> cf = getCellFamily(cfam);
		if(cf == null) return null;
		Map<String,String> map = new HashMap<String,String>();
		for(Cell c:cf){
			map.put(c.getKey().getColumn_qualifier(), new String(c.getValue()));
		}
		return map;
	}
	
	private List<Cell> getCellFamily(String cfam){
		List<Cell> revs = getCellFamilyRevisions(cfam);
		if(revs == null) return null;
		List<Cell> list = new LinkedList<Cell>();
		list.add(revs.get(0));
		for(int idx=1;idx<revs.size();idx++){
			if(!revs.get(idx).getKey().getColumn_qualifier()
					.equals(revs.get(idx-1).getKey().getColumn_qualifier()))
				list.add(revs.get(idx));
		}
		return list;		
	}

	private List<Cell> getCellFamilyRevisions(String cfam){
		List<Cell> list = new LinkedList<Cell>();
		for(Cell c:cells)
			if(c.getKey().getColumn_family().equals(cfam))
				list.add(c);
		return list.isEmpty() ? null : list;
	}

	@Override
	public String toString(){
		String s = rowkey+" : {";
		for(Cell c:cells)
			s+="'"+c.getKey().getColumn_family()+":"+c.getKey().getColumn_qualifier()+"' : '"+new String(c.getValue())+"', ";
		return s.substring(0,s.length()-2) + "}";
	}

}
