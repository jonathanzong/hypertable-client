package com.didactic.htclient.mutator;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.Key;

/**
 * Class that represents a Put operation into Hypertable.
 *
 */
public class Put {

	private String rowkey;
	private List<Cell> cells;

	/**
	 * Constructs a Put object for a specified rowkey.
	 * 
	 * @param rowkey
	 */
	public Put(String rowkey){
		this.rowkey = rowkey;
		this.cells = new LinkedList<Cell>();
	}

	/**
	 * Adds a value to be inserted into the specified column.
	 * 
	 * @param cfam
	 * @param cqual
	 * @param value
	 * @return
	 */
	public Put add(String cfam, String cqual, String value){
		Cell cell = new Cell();

		Key key = new Key();
		key.setRow(this.getRow());

		if(cfam != null)
			key.setColumn_family(cfam);

		if(cqual != null)
			key.setColumn_qualifier(cqual);

		cell.setKey(key);

		cell.setValue( ByteBuffer.wrap(value.getBytes()) );
		
		this.cells.add(cell);

		return this;		
	}

	public String getRow() {
		return rowkey;
	}

	public List<Cell> _getCells(){
		return this.cells;
	}
}
