package com.didactic.htclient.mutator;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.Key;

public class Put {

	private String rowkey;
	private List<Cell> cells;

	public Put(String rowkey){
		this.rowkey = rowkey;
		this.cells = new LinkedList<Cell>();
	}

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
