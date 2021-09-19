[![Java CI - deploy](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml/badge.svg)](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d40cc721f39b49eb81408307960f145b)](https://app.codacy.com/gh/ArcadeData/arcadedb?utm_source=github.com&utm_medium=referral&utm_content=ArcadeData/arcadedb&utm_campaign=Badge_Grade_Settings)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/1f971260db1e46638bd3fd91e3ebf668)](https://www.codacy.com/gh/ArcadeData/arcadedb/dashboard?utm_source=github.com&utm_medium=referral&utm_content=ArcadeData/arcadedb&utm_campaign=Badge_Coverage)
[![security status](https://www.meterian.io/badge/gh/ArcadeData/arcadedb/security?branch=main)](https://www.meterian.io/report/gh/ArcadeData/arcadedb)
[![stability status](https://www.meterian.io/badge/gh/ArcadeData/arcadedb/stability?branch=main)](https://www.meterian.io/report/gh/ArcadeData/arcadedb)

![ArcadeDB Logo](https://arcadedb.com/assets/images/arcadedb-logo.png)

ArcadeDB is a Multi-Model DBMS (created originally as a fork from [OrientDB Open Source project](https://github.com/orientechnologies/orientdb) after the acquisition by SAP) with
a brand new engine made of Alien Technology, able to crunch millions of records per second on common hardware with the usage of
minimal resources. ArcadeDB is written in LLJ: Low Level Java. It's still Java8+ but only using low level API to use advanced
mechanical sympathy techniques and a reduced pressure of the Garbage Collector.

ArcadeDB supports the following models:
- [Graph Database](https://docs.arcadedb.com#Graph-Model) (compatible with Neo4j Cypher, Apache Tinkerpop Gremlin and OrientDB SQL)
- [Document Database](https://docs.arcadedb.com#Document-Model) (compatible with the MongoDB driver + MongoDB queries and OrientDB SQL)
- [Key/Value](https://docs.arcadedb.com#KeyValue-Model) (compatible with the Redis driver)
- [Time Series](https://docs.arcadedb.com#TimeSeries-Model)

ArcadeDB understands multiple languages:
- [SQL](https://docs.arcadedb.com#SQL) (from OrientDB SQL)
- Neo4j [Cypher (Open Cypher)](https://docs.arcadedb.com#Cypher)
- [Apache Gremlin (Apache Tinkerpop v3.4.x)](https://docs.arcadedb.com#Gremlin-API)
- [MongoDB Query Language](https://docs.arcadedb.com#MongoDB-API)

ArcadeDB can be used as:
- Embedded from any language on top of the Java Virtual Machine
- Remotely by using [HTTP/JSON](https://docs.arcadedb.com#HTTP-API)
- Remotely by using a [Postgres driver](https://docs.arcadedb.com#Postgres-Driver) (ArcadeDB implements Postgres Wire protocol)
- Remotely by using a [MongoDB driver](https://docs.arcadedb.com#MongoDB-API) (only a subset of the operations are implemented)
- Remotely by using a [Redis driver](https://docs.arcadedb.com#Redis-API) (only a subset of the operations are implemented)

ArcadeDB is Free for any usage and licensed under the liberal [Open Source Apache 2 license](LICENSE). To contribute to the project check [CONTRIBUTING](CONTRIBUTING.md). If you need commercial support or you need to have an issue fixed ASAP, check our [GitHub Sponsor page](https://github.com/sponsors/ArcadeData) on both Recurrent and One-Time tiers. All the sponsorship received will be distributed to the active contributors of this project.

Have fun with data!

The ArcadeDB Team


[![Java CI - deploy](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml/badge.svg)](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml)
