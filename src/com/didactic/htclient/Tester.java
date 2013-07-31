package com.didactic.htclient;

import java.util.List;

import com.didactic.htclient.accessor.Result;
import com.didactic.htclient.accessor.Scan;
import com.didactic.htclient.mutator.Delete;
import com.didactic.htclient.mutator.Put;

public class Tester {

	public static void main(String[] args) throws Exception{
		HypertableClient htc = new HypertableClient();

		Put put = new Put("4128");
		for(int i=0;i<10;i++)
			put.add("val", "lol", ""+i);

		htc.put("test", put);
		
		// Scan
		List<Result> res = htc.scan("test", new Scan("4128"));

		Long ts = res.get(0).getTimestamp("val", "lol");
		System.out.println(ts);

		Delete del = new Delete("4128");
		del.deleteColumn("val", "lol", ts);

		htc.delete("test", del);


		System.out.println(htc.scan("test", new Scan("4128")).get(0).getValueRevisions("val","lol")+" !!");

		del = new Delete("4128").deleteColumnNthLatest("val", "lol", 3);
		htc.delete("test", del);
		
		System.out.println(htc.scan("test", new Scan("4128")).get(0).getValueRevisions("val","lol")+" ~~!!");
		

		del.deleteColumnRevisions("val", "lol");
		
		htc.delete("test", del);
		
		System.out.println(htc.scan("test", new Scan("4128"))+" !!");

		//		List<String> revs = (htc.scan("Users", new Scan("99","99")).get(0).getValueRevisions("dynamic","rerank"));
		//		String recent = (htc.scan("Users", new Scan("99","99")).get(0).getValue("dynamic","rerank"));
		//		System.out.println(revs.indexOf(recent));
		//		System.out.println(revs.size());
		//		System.out.println(recent);
	}

}
