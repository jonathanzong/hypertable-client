package com.didactic.htclient;

import java.util.List;

import com.didactic.htclient.accessor.Result;
import com.didactic.htclient.accessor.Scan;
import com.didactic.htclient.mutator.Delete;
import com.didactic.htclient.mutator.Put;
import com.didactic.htclient.mutator.TableSchema;

public class Example {

	public static void main(String[] args) throws Exception{
		HypertableClient htc = new HypertableClient("localhost", 38080);

		// Create a table
		TableSchema table = new TableSchema()
		.addColumnFamily("cfam");

		htc.createTableIfNotExists("htc_test", table);

		// Put some values
		Put put = new Put("42")
		.add("cfam", "cqual", "1")
		.add("cfam", "cqual", "2")
		.add("cfam", "cqual", "3")
		.add("cfam", "cqual", "4")
		.add("cfam", "cqual", "5")
		.add("cfam", "cqual2", "differentQualifier")
		.add("cfam", "cqual2", "differentQualifierRev2");

		htc.put("htc_test", put);

		// Scan the whole table
		List<Result> res = htc.scan("htc_test", new Scan("42"));

		// each result represents a single row
		for(Result r: res){
			// print the latest revision of the column
			sopl(r.getValue("cfam", "cqual"));
			// print all revisions of the column
			sopl(r.getValueRevisions("cfam", "cqual"));
			// print the latest revision of every column in a family
			sopl(r.getFamily("cfam"));
		}
		
		// get the most recent timestamp
		Long ts = res.get(0).getTimestamp("cfam", "cqual");

		// Delete a revision by timestamp
		Delete del = new Delete("42")
		.deleteColumn("cfam", "cqual", ts);

		htc.delete("htc_test", del);

		sopl(htc.scan("htc_test", new Scan("42")).get(0).getValueRevisions("cfam", "cqual"));
		
		// Delete the 2 latest revisions of the column
		del.deleteColumnNthLatest("cfam", "cqual", 2);
		htc.delete("htc_test", del);

		sopl(htc.scan("htc_test", new Scan("42")).get(0).getValueRevisions("cfam", "cqual"));

		// Delete all revisions
		del.deleteColumnRevisions("cfam", "cqual");
		htc.delete("htc_test", del);

		sopl(htc.scan("htc_test", new Scan("42")).get(0).getValueRevisions("cfam", "cqual"));
	}

	static void sopl(Object o){
		System.out.println(o);
	}
}
