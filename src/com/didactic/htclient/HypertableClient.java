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
import com.didactic.htclient.mutator.TableSchema;

/**
 * Class used to communicate with Hypertable. Wraps
 * the ThriftClient and provides methods to provide
 * namespacing, create and access tables, and 
 * perform database operations.
 */
public class HypertableClient {

	private ThriftClient client = null;
	private long ns = -1;

	/**
	 * Creates an object to access Hypertable.
	 * 
	 * @param host
	 * @param port
	 * @throws TTransportException
	 * @throws TException
	 * @throws ClientException
	 */
	public HypertableClient(String host, int port) throws TTransportException, TException, ClientException{
		client = ThriftClient.create(host, port);
		ns = client.namespace_open("/");
	}

	/**
	 * Convenience method for useNamespace(namespace,false);
	 * 
	 * @param namespace
	 * @return
	 * @throws ClientException
	 * @throws TException
	 * @see useNamespace(String namespace, boolean createIfNotExists)
	 */
	public HypertableClient useNamespace(String namespace) throws ClientException, TException{
		return useNamespace(namespace,false);
	}

	/**
	 * Specifies a namespace for the client to use.
	 * Default namespace is root '/'
	 * 
	 * @param namespace
	 * @param createIfNotExists
	 * @return
	 * @throws ClientException
	 * @throws TException
	 */
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

	/**
	 * Method to execute a Put mutation operation
	 * on Hypertable.
	 * 
	 * @param tableName
	 * @param put
	 * @throws ClientException
	 * @throws TException
	 * @see Put
	 */
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

	/**
	 * Method to execute a batch Put mutation
	 * operation on Hypertable. 
	 * 
	 * @param tableName
	 * @param puts
	 * @throws ClientException
	 * @throws TException
	 */
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

	/**
	 * Method to execute a Delete mutation
	 * operation on Hypertable. A delete
	 * is implemented by Hypertable as a
	 * put that inserts tombstone values.
	 * 
	 * @param tableName
	 * @param delete
	 * @throws ClientException
	 * @throws TException
	 */
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

	/**
	 * Method to execute a batch Delete 
	 * mutation operation on Hypertable.
	 * A delete is implemented by
	 * Hypertable as a put that inserts
	 * tombstone values.
	 * 
	 * @param tableName
	 * @param deletes
	 * @throws ClientException
	 * @throws TException
	 */
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

	/**
	 * Method to execute a Scan
	 * operation on Hypertable.
	 * 
	 * @param tablename
	 * @param scan
	 * @return
	 * @throws ClientException
	 * @throws TException
	 */
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
	
	/**
	 * Method to execute an operation
	 * to create a table based on a schema object.
	 * 
	 * @param tableName
	 * @param tableschema
	 * @throws ClientException
	 * @throws TException
	 */
	public void createTable(String tableName, TableSchema tableschema) throws ClientException, TException{
		client.create_table(ns, tableName, tableschema.toString());
	}
	
	/**
	 * Method to execute an operation
	 * to create a table based on a 
	 * schema object only if a table
	 * with that name does not already
	 * exist. Returns true if a table
	 * was created.
	 * 
	 * @param tableName
	 * @param tableschema
	 * @throws ClientException
	 * @throws TException
	 */
	public boolean createTableIfNotExists(String tableName, TableSchema tableschema) throws ClientException, TException{
		if(!client.table_exists(ns, tableName)){
			createTable(tableName, tableschema);
			return true;
		}
		return false;
	}
}
