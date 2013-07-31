package com.didactic.htclient.mutator;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.KeyFlag;

public class Delete{

	private String rowkey;
	private List<Cell> cells;

	public Delete(String rowkey){
		this.rowkey = rowkey;
		this.cells = new LinkedList<Cell>();
	}
	
	public Delete deleteColumnNthLatest(String cfam, String cqual, Integer revisions){
		Cell cell = new Cell();

		Key key = new Key();
		key.setFlag(KeyFlag.DELETE_CELL_VERSION);
		key.setRow(this.rowkey);
		
		if(cfam != null)
			key.setColumn_family(cfam);

		if(cqual != null)
			key.setColumn_qualifier(cqual);
		
		if(revisions != null)
			key.setRevision(revisions);

		cell.setKey(key);
		
		this.cells.add(cell);

		return this;
	}
	
	public Delete deleteColumn(String cfam, String cqual, Long timestamp){
		Cell cell = new Cell();

		Key key = new Key();
		key.setFlag(KeyFlag.DELETE_CELL_VERSION);
		key.setRow(this.rowkey);
		
		if(cfam != null)
			key.setColumn_family(cfam);

		if(cqual != null)
			key.setColumn_qualifier(cqual);
		
		if(timestamp != null)
			key.setTimestamp(timestamp);

		cell.setKey(key);
		
		this.cells.add(cell);

		return this;		
	}
	
	public Delete deleteFamilyRevisions(String cfam){
		return deleteColumnRevisions(cfam, null);
	}

	public Delete deleteColumnRevisions(String cfam, String cqual){
		Cell cell = new Cell();

		Key key = new Key();
		key.setFlag(KeyFlag.DELETE_CELL);
		key.setRow(this.rowkey);

		if(cfam != null)
			key.setColumn_family(cfam);

		if(cqual != null)
			key.setColumn_qualifier(cqual);

		cell.setKey(key);
		
		this.cells.add(cell);

		return this;		
	}

	public List<Cell> _getCells() {
		return this.cells;
	}

	public String getRowkey() {
		return this.rowkey;
	}
	
}
