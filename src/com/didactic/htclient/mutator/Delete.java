package com.didactic.htclient.mutator;

import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.KeyFlag;

public class Delete extends Put{

	public Delete(String rowkey) {
		super(rowkey);
	}

	public Delete add(String cfam, String cqual){
		Cell cell = new Cell();

		Key key = new Key();
		key.setFlag(KeyFlag.DELETE_CELL);
		key.setRow(this.getRow());

		if(cfam != null)
			key.setColumn_family(cfam);

		if(cqual != null)
			key.setColumn_qualifier(cqual);

		cell.setKey(key);
		
		_getCells().add(cell);

		return this;		
	}
	
}
