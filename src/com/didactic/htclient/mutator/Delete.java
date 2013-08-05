package com.didactic.htclient.mutator;

import java.util.LinkedList;
import java.util.List;

import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.KeyFlag;

/**
 * Class representing a Delete operation
 * on Hypertable. Deletes are implemented
 * by Hypertable as Puts that insert
 * tombstone values.
 *
 */
public class Delete{

	private String rowkey;
	private List<Cell> cells;

	/**
	 * Constructs a Delete object for a specified row.
	 * 
	 * @param rowkey
	 */
	public Delete(String rowkey){
		this.rowkey = rowkey;
		this.cells = new LinkedList<Cell>();
	}
	
	/**
	 * Deletes the N latest revisions of a specified column.
	 * For example, passing 3 as the Integer parameter will
	 * delete the 3 most recent updates to the column.
	 * 
	 * N is held in the revisions field of the Key
	 * (which actually means something else) and retrieved
	 * by the client object calling method.
	 * 
	 * @param cfam
	 * @param cqual
	 * @param revisions
	 * @return
	 */
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
	
	/**
	 * Deletes a revision of a specified column by timestamp.
	 * 
	 * @param cfam
	 * @param cqual
	 * @param timestamp
	 * @return
	 */
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
	
	/**
	 * Deletes all revisions of all columns in
	 * the specified column family.
	 * 
	 * @param cfam
	 * @return
	 */
	public Delete deleteFamilyRevisions(String cfam){
		return deleteColumnRevisions(cfam, null);
	}

	/**
	 * Deletes all revisions of a specified column.
	 * 
	 * @param cfam
	 * @param cqual
	 * @return
	 */
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
