hypertable-client
=================

A nicer Java Client API for Hypertable, using row-oriented operations in the style of HBase.

What's Wrong with Thrift?
------------------------

```
client.table_exists(ns, name);
client.exists_table(ns, name);
```

Among other things, the Thrift API isn't stylistically consistent with Java conventions (camel case instead of underscores, etc.) and in some cases needlessly verbose.

Performing operations from a row-oriented perspective makes certain tasks conceptually easier. The Java client wrapper also provides operations that are either unavailable or annoying in the Thrift API.

Code Example
------------

```
HypertableClient htc = new HypertableClient("localhost", 38080);
```

### Create a table
```
TableSchema table = new TableSchema().addColumnFamily("cfam");
htc.createTable("htc_test", table);
```

### Put some values
```
Put put = new Put("42")
.add("cfam", "cqual1", "val1")
.add("cfam", "cqual2", "val2");

htc.put("htc_test", put);
```

### Scan the table
```
List<Result> res = htc.scan("htc_test", new Scan("42"));
```

### Access some data
```
for(Result r: res){
	sopl(r.getFamily("cfam"));
	sopl(r.getValue("cfam", "cqual1"));
}
```

Highlighted Features
----------------------------------

- Create table using a Schema string in XML without HQL (thrift does not expose the function: https://groups.google.com/forum/#!msg/hypertable-user/QnMGE1LgpAI/JMWDCUWe8wUJ)
- Delete n most recent revisions of cell
- Access data as Result objects encapsulating all columns associated with a single rowkey
- Access column families as qualifier-value maps

Contribute
--------------------------------

- Fork the project
- Increase awesomeness of project
- Test new awesomeness
- Commit and send a pull request