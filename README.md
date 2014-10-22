# Pig Scripts on Apache Drill
This is an initial work on supporting Pig scripts on Drill. It extends the PigServer to get a Pig logical plan for a input pig script. It then converts the Pig logical plan to Drill logical plan.

Review Board:
https://reviews.apache.org/r/26769/

Operators Supported:
LOAD, STORE, FILTER, UNION, JOIN, DISTINCT, LIMIT.
Future work:
FOREACH and GROUP is not supported yet.


TestCases:
org.apache.drill.exec.pigparser.TestPigLatinOperators
Pig Scripts can be tested on Drill's web interface as well (localhost:8047/query).

Fact check:
 * LOAD: Supports delimited text files only. Picks delimeter provided in PigStorage(). Default \t. Reads data from Local Filesystem currently. (pig -x local)
 * STORE: Only dumps on --SCREEN-- for now.
 * JOIN: Inner, LeftOuter, RightOuter, FullOuter (not supported by drill currently though). Only supports alias based joins not index based($0 etc).

# Apache Drill

Apache Drill is a distributed MPP query layer that supports SQL and alternative query languages against NoSQL and Hadoop data storage systems.  It was inspired in part by [Google's Dremel](http://research.google.com/pubs/pub36632.html).  It is currently incubating under the Apache Foundation.

## Quickstart

Please read INSTALL.md for setting up and running Apache Drill.

## More Information
Please see the [Apache Drill Website](http://incubator.apache.org/drill/) or the [Apache Drill Wiki](https://cwiki.apache.org/confluence/display/DRILL/Apache+Drill+Wiki) for more information including:

 * Remote Execution Installation Instructions
 * Information about how to submit logical and distributed physical plans
 * More example queries and sample data
 * Find out ways to be involved or disuss Drill


## Join the community!
Apache Drill is an Apache Foundation project and is seeking all types of contributions.  Please say hello on the Apache Drill mailing list or join our Google Hangouts for more information.  (More information can be found at the Apache Drill website).

## Disclaimer
Apache Drill is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
