hypertable-client
=================

A nicer Java Client API for Hypertable, using row-oriented operations in the style of HBase.

Documentation and examples forthcoming.

Features not present in thrift API:
- Delete n most recent revisions of cell
- Create table using a Schema string in XML without HQL (thrift does not expose the function: https://groups.google.com/forum/#!msg/hypertable-user/QnMGE1LgpAI/JMWDCUWe8wUJ)