package com.didactic.htclient;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;

import com.didactic.htclient.accessor.Result;
import com.didactic.htclient.accessor.Scan;
import com.didactic.htclient.mutator.Delete;
import com.didactic.htclient.mutator.Put;

public class HypertableClient {

	private ThriftClient client = null;
	private long ns = -1;

	public HypertableClient() throws TTransportException, TException, ClientException{
		client = ThriftClient.create("localhost", 38080);
		ns = client.namespace_open("/");
	}

	public HypertableClient useNamespace(String namespace) throws ClientException, TException{
		return useNamespace(namespace,false);
	}

	public HypertableClient useNamespace(String namespace, boolean createIfNotExists) throws ClientException, TException{
		if(ns > -1){
			client.namespace_close(ns);
			ns = -1;
		}

		if(client.namespace_exists(namespace))
			ns = client.namespace_open(namespace);
		else if(createIfNotExists){
			client.create_namespace(namespace);
			ns = client.namespace_open(namespace);
		}
		return this;
	}

	public void put(String tableName, Put put) throws ClientException, TException{
		long mutator = client.mutator_open(ns, tableName, 0, 0);

		try {
			if(put._getCells().size() > 1)
				client.mutator_set_cells(mutator, put._getCells());
			else if(put._getCells().size() == 1)
				client.mutator_set_cell(mutator, put._getCells().get(0));
		}
		finally {
			client.mutator_close(mutator);
		}
	}

	public void put(String tableName, List<Put> puts) throws ClientException, TException{
		long mutator = client.mutator_open(ns, tableName, 0, 0);

		List<Cell> cells = new LinkedList<Cell>();

		for(Put put:puts)
			cells.addAll(put._getCells());

		try {
			if(cells.size() > 1)
				client.mutator_set_cells(mutator, cells);
			else if(cells.size() == 1)
				client.mutator_set_cell(mutator, cells.get(0));
		}
		finally {
			client.mutator_close(mutator);
		}
	}

	public void delete(String tableName, Delete delete) throws ClientException, TException{
		long mutator = client.mutator_open(ns, tableName, 0, 0);

		for(int i=0;i<delete._getCells().size();i++){
			Cell c = delete._getCells().get(i);
			if(c.getKey().isSetRevision()){
				int revs = (int) c.getKey().getRevision();
				System.out.println(revs);
				String cfam = c.getKey().getColumn_family();
				String cqual = c.getKey().getColumn_qualifier();
				c.getKey().setRevisionIsSet(false);
				List<Result> res = this.scan(tableName, new Scan(delete.getRowkey()).addColumn(cfam, cqual).setRevisions(revs));
				if(!res.isEmpty()){
					System.out.println(res.get(0).getValueRevisions(cfam, cqual));
					for(Long ts:res.get(0).getTimestampRevisions(cfam, cqual))
						delete.deleteColumn(cfam, cqual, ts);
				}
				delete._getCells().remove(i--);
			}
		}

		try {
			if(delete._getCells().size() > 1)
				client.mutator_set_cells(mutator, delete._getCells());
			else if(delete._getCells().size() == 1)
				client.mutator_set_cell(mutator, delete._getCells().get(0));
		}
		finally {
			client.mutator_close(mutator);
		}
	}

	public void delete(String tableName, List<Delete> deletes) throws ClientException, TException{
		long mutator = client.mutator_open(ns, tableName, 0, 0);

		List<Cell> cells = new LinkedList<Cell>();

		for(Delete delete:deletes)
			cells.addAll(delete._getCells());

		try {
			if(cells.size() > 1)
				client.mutator_set_cells(mutator, cells);
			else if(cells.size() == 1)
				client.mutator_set_cell(mutator, cells.get(0));
		}
		finally {
			client.mutator_close(mutator);
		}
	}

	public List<Result> scan(String tablename, Scan scan) throws ClientException, TException{
		long scanner = client.scanner_open(ns, tablename, scan._getScanSpec());
		HashMap<String, List<Cell>> buckets = new HashMap<String, List<Cell>>();
		try {
			List<Cell> cells = client.scanner_get_cells(scanner);

			while (cells.size() > 0) {
				for (Cell cell : cells) {
					String row = cell.getKey().getRow();
					if(buckets.get(row) == null)
						buckets.put(row,new LinkedList<Cell>());
					buckets.get(row).add(cell);
				}
				cells = client.scanner_get_cells(scanner);
			}
		}
		finally {
			client.scanner_close(scanner);
		}
		List<Result> reslist = new LinkedList<Result>();
		for(String row: buckets.keySet()){
			reslist.add(new Result(row, buckets.get(row)));
		}
		return reslist;
	}
}
