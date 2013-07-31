package com.didactic.htclient;

import java.util.List;

import com.didactic.htclient.accessor.Scan;
import com.didactic.htclient.mutator.Delete;
import com.didactic.htclient.mutator.Put;

public class Main {
	
	public static void main(String[] args) throws Exception{
		HypertableClient htc = new HypertableClient();
		System.out.println("hi");
		
		Put put = new Put("4128");
		put.add("val", "lol", "9");
		
		htc.put("test", put);
		System.out.println("hi");
		List<String> revs = htc.scan("test", new Scan("4128","4128")).get(0).getValueRevisions("val","lol");
		System.out.println("hi");
		String recent = htc.scan("test", new Scan("4128","4128")).get(0).getValue("val","lol");

		System.out.println(revs);
		System.out.println(recent);

		Delete del = new Delete("4128");
		del.add("val", "lol");
		
		htc.put("test", del);
		

		System.out.println(htc.scan("test", new Scan("4128","4128"))+" !!");
		
//		List<String> revs = (htc.scan("Users", new Scan("99","99")).get(0).getValueRevisions("dynamic","rerank"));
//		String recent = (htc.scan("Users", new Scan("99","99")).get(0).getValue("dynamic","rerank"));
//		System.out.println(revs.indexOf(recent));
//		System.out.println(revs.size());
//		System.out.println(recent);
	}

}
