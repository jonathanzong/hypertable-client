package com.didactic.htclient.accessor;

import java.util.LinkedList;
import java.util.List;

import org.hypertable.thriftgen.RowInterval;
import org.hypertable.thriftgen.ScanSpec;

public class Scan {
	
	protected ScanSpec scanSpec;
	protected List<RowInterval> intervals;
	
	public Scan(){
		scanSpec = new ScanSpec();
		intervals = new LinkedList<RowInterval>();
	}

	public Scan(String rowkey){
		this();
		addRowInterval(rowkey,rowkey);
	}
	
	public Scan(String startRow, String endRow){
		this();
		addRowInterval(startRow,endRow);
	}
	
	public Scan addRowInterval(String startRow, String endRow){
		RowInterval ri = new RowInterval();
		ri.setStart_row(startRow);
		ri.setEnd_row(endRow);
		intervals.add(ri);
		scanSpec.setRow_intervals(intervals);
		return this;
	}
	
	public Scan addFamily(String cfam){
		return addColumn(cfam, null);
	}
	
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
	
	public Scan setRevisions(Integer revisions){
		scanSpec.setVersions(revisions);
		return this;
	}
	
	public Scan setStartTime(Long nanosSinceEpoch){
		scanSpec.setStart_time(nanosSinceEpoch);
		return this;
	}
	
	public Scan setEndTime(Long nanosSinceEpoch){
		scanSpec.setEnd_time(nanosSinceEpoch);
		return this;
	}
	
	public Scan setRowkeyRegex(String regex){
		scanSpec.setRow_regexp(regex);
		return this;
	}

	public Scan setValueRegex(String regex){
		scanSpec.setValue_regexp(regex);
		return this;
	}
	
	public ScanSpec _getScanSpec(){
		return this.scanSpec;
	}
	
}
