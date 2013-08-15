package com.didactic.htclient.accessor;

import java.util.LinkedList;
import java.util.List;

import org.hypertable.thriftgen.RowInterval;
import org.hypertable.thriftgen.ScanSpec;

/**
 * Class representing a Scan operation to
 * perform a read from Hypertable.
 *
 */
public class Scan {
	
	protected ScanSpec scanSpec;
	protected List<RowInterval> intervals;
	
	/**
	 * Constructs an empty scan object.
	 * Row intervals must be added manually.
	 */
	public Scan(){
		scanSpec = new ScanSpec();
		intervals = new LinkedList<RowInterval>();
	}

	/**
	 * Constructs a scan over a single row.
	 * Analogous to a "Get" operation unless
	 * more intervals are manually added.
	 * 
	 * @param rowkey
	 */
	public Scan(String rowkey){
		this();
		addRowInterval(rowkey,rowkey);
	}
	
	/**
	 * Constructs a scan over a single row interval.
	 * 
	 * @param startRow
	 * @param endRow
	 */
	public Scan(String startRow, String endRow){
		this();
		addRowInterval(startRow,endRow);
	}
	
	public Scan addRow(String rowkey){
		RowInterval ri = new RowInterval();
		ri.setStart_row(rowkey);
		ri.setEnd_row(rowkey);
		intervals.add(ri);
		scanSpec.setRow_intervals(intervals);
		return this;
	}
	
	/**
	 * Adds a row interval to the scan specification.
	 * 
	 * @param startRow
	 * @param endRow
	 * @return
	 */
	public Scan addRowInterval(String startRow, String endRow){
		RowInterval ri = new RowInterval();
		ri.setStart_row(startRow);
		ri.setEnd_row(endRow);
		intervals.add(ri);
		scanSpec.setRow_intervals(intervals);
		return this;
	}
	
	/**
	 * Adds a column family to return as results.
	 * By default, a scan returns all information
	 * unless one of these add methods are called.
	 * 
	 * @param cfam
	 * @return
	 */
	public Scan addFamily(String cfam){
		return addColumn(cfam, null);
	}
	
	/**
	 * Adds a column to return as results.
	 * By default, a scan returns all information
	 * unless one of these add methods are called.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
	public Scan addColumn(String cfam, String cqual){
		if(cfam != null){
			String col = cfam;
			if(cqual != null){
				col += ":"+cqual;
			}
			scanSpec.addToColumns(col);			
		}
		return this;
	}
	
	/**
	 * Sets the number of revisions of each column to return.
	 * For example, passing a parameter of 1 will return only
	 * the latest revision of a column.
	 * 
	 * @param revisions
	 * @return
	 */
	public Scan setRevisions(Integer revisions){
		scanSpec.setVersions(revisions+1);
		return this;
	}
	
	/**
	 * Sets a start time to the scan specification.
	 * 
	 * @param nanosSinceEpoch
	 * @return
	 */
	public Scan setStartTime(Long nanosSinceEpoch){
		scanSpec.setStart_time(nanosSinceEpoch);
		return this;
	}
	
	/**
	 * Sets an end time to the scan specification.
	 * 
	 * @param nanosSinceEpoch
	 * @return
	 */
	public Scan setEndTime(Long nanosSinceEpoch){
		scanSpec.setEnd_time(nanosSinceEpoch);
		return this;
	}
	
	/**
	 * Sets a regular expression to match on rowkeys
	 * during the scan.
	 * 
	 * @param regex
	 * @return
	 */
	public Scan setRowkeyRegex(String regex){
		scanSpec.setRow_regexp(regex);
		return this;
	}

	/**
	 * Sets a regular expression to match on values
	 * during the scan.
	 * 
	 * @param regex
	 * @return
	 */
	public Scan setValueRegex(String regex){
		scanSpec.setValue_regexp(regex);
		return this;
	}
	
	public ScanSpec _getScanSpec(){
		return this.scanSpec;
	}
	
}
