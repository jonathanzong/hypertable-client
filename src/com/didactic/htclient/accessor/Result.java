package com.didactic.htclient.accessor;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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
	 * Performs a binary search to find the most recent
	 * cell for a specified column. Returns null if it is
	 * not found.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	private Cell getCell(String cfam, String cqual){
		int lo = 0;
		int hi = cells.size() - 1;
		while(lo <= hi){
			int mid = lo + (hi - lo) / 2;
			if(cfam.compareTo(cells.get(mid).getKey().getColumn_family()) < 0)
				hi = mid - 1;
			else if(cfam.compareTo(cells.get(mid).getKey().getColumn_family()) > 0)
				lo = mid + 1;
			else if(cfam.compareTo(cells.get(mid).getKey().getColumn_family()) == 0){
				if(cqual.compareTo(cells.get(mid).getKey().getColumn_qualifier()) < 0)
					hi = mid - 1;
				else if(cqual.compareTo(cells.get(mid).getKey().getColumn_qualifier()) > 0)
					lo = mid + 1;
				else if(cells.get(mid).getKey().getColumn_qualifier().equals(cqual)){
					while(mid>-1 && cells.get(mid).getKey().getColumn_family().equals(cfam) &&
							cells.get(mid).getKey().getColumn_qualifier().equals(cqual)){
						mid--;
					}
					return cells.get(mid+1);
				}
			}            
		}
		return null;
	}

	/**
	 * Performs a binary search to return all revisions
	 * of a specified column, or null if it is not found.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	private List<Cell> getCellRevisions(String cfam, String cqual){
		int lo = 0;
		int hi = cells.size() - 1;
		while(lo <= hi){
			int mid = lo + (hi - lo) / 2;
			if(cfam.compareTo(cells.get(mid).getKey().getColumn_family()) < 0)
				hi = mid - 1;
			else if(cfam.compareTo(cells.get(mid).getKey().getColumn_family()) > 0)
				lo = mid + 1;
			else if(cfam.compareTo(cells.get(mid).getKey().getColumn_family()) == 0){
				if(cqual.compareTo(cells.get(mid).getKey().getColumn_qualifier()) < 0)
					hi = mid - 1;
				else if(cqual.compareTo(cells.get(mid).getKey().getColumn_qualifier()) > 0)
					lo = mid + 1;
				else if(cells.get(mid).getKey().getColumn_qualifier().equals(cqual)){
					while(mid>-1 && cells.get(mid).getKey().getColumn_family().equals(cfam) &&
							cells.get(mid).getKey().getColumn_qualifier().equals(cqual)){
						mid--;
					}
					List<Cell> vals = new LinkedList<Cell>();
					for(int idx=mid+1;idx<cells.size()-1;idx++){
						if(!(cells.get(idx).getKey().getColumn_family().equals(cfam) &&
								cells.get(idx).getKey().getColumn_qualifier().equals(cqual))){
							break;
						}
						vals.add(cells.get(idx));
					}
					return vals;
				}
			}            
		}
		return null;
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder(rowkey+" : [");
		for(Cell c:cells){
			sb.append('{');
			sb.append(c.getKey().getColumn_family());
			sb.append(':');
			sb.append(c.getKey().getColumn_qualifier());
			sb.append(" = ");
			sb.append(new String(c.getValue()));
			sb.append(" R");
			sb.append(c.getKey().getRevision());
			sb.append("}, ");
		}
		return sb.toString().substring(0,sb.length()-2)+"]";
	}

}
