[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![REUSE status](https://api.reuse.software/badge/github.com/ArcadeData/arcadedb)](https://api.reuse.software/info/github.com/ArcadeData/arcadedb)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.arcadedb/arcadedb-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.arcadedb/arcadedb-parent)

[![Java CI - deploy](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml/badge.svg)](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/1f971260db1e46638bd3fd91e3ebf668)](https://www.codacy.com/gh/ArcadeData/arcadedb/dashboard?utm_source=github.com&utm_medium=referral&utm_content=ArcadeData/arcadedb&utm_campaign=Badge_Coverage)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/ArcadeData/arcadedb.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/ArcadeData/arcadedb/context:java)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d40cc721f39b49eb81408307960f145b)](https://app.codacy.com/gh/ArcadeData/arcadedb?utm_source=github.com&utm_medium=referral&utm_content=ArcadeData/arcadedb&utm_campaign=Badge_Grade_Settings)
[![security status](https://www.meterian.io/badge/gh/ArcadeData/arcadedb/security?branch=main)](https://www.meterian.io/report/gh/ArcadeData/arcadedb)
[![stability status](https://www.meterian.io/badge/gh/ArcadeData/arcadedb/stability?branch=main)](https://www.meterian.io/report/gh/ArcadeData/arcadedb)

![ArcadeDB Logo](https://arcadedb.com/assets/images/arcadedb-logo.png)

ArcadeDB is a Multi-Model DBMS (created originally as a fork from [OrientDB Open Source project](https://github.com/orientechnologies/orientdb) after the acquisition by SAP) with a brand-new engine made of Alien Technology, able to crunch millions of records per second on common hardware with the usage of
minimal resources. ArcadeDB is written in LLJ: Low Level Java. It's still Java11+ but only using low level API to use advanced
mechanical sympathy techniques and a reduced pressure of the Garbage Collector.

ArcadeDB supports the following models:
- [Graph Database](https://docs.arcadedb.com#Graph-Model) (compatible with Neo4j Cypher, Apache Tinkerpop Gremlin and OrientDB SQL)
- [Document Database](https://docs.arcadedb.com#Document-Model) (compatible with the MongoDB driver + MongoDB queries and OrientDB SQL)
- [Key/Value](https://docs.arcadedb.com#KeyValue-Model) (compatible with the Redis driver)
- [Time Series](https://docs.arcadedb.com#TimeSeries-Model)

ArcadeDB understands multiple languages:
- [SQL](https://docs.arcadedb.com#SQL) (from OrientDB SQL)
- Neo4j [Cypher (Open Cypher)](https://docs.arcadedb.com#Cypher)
- [Apache Gremlin (Apache Tinkerpop v3.5.x)](https://docs.arcadedb.com#Gremlin-API)
- [GraphQL Language](https://docs.arcadedb.com#GraphQL)
- [MongoDB Query Language](https://docs.arcadedb.com#MongoDB-API)

ArcadeDB can be used as:
- Embedded from any language on top of the Java Virtual Machine
- Remotely by using [HTTP/JSON](https://docs.arcadedb.com#HTTP-API)
- Remotely by using a [Postgres driver](https://docs.arcadedb.com#Postgres-Driver) (ArcadeDB implements Postgres Wire protocol)
- Remotely by using a [Redis driver](https://docs.arcadedb.com#Redis-API) (only a subset of the operations are implemented)
- Remotely by using a [MongoDB driver](https://docs.arcadedb.com#MongoDB-API) (only a subset of the operations are implemented)

ArcadeDB is Free for any usage and licensed under the liberal [Open Source Apache 2 license](LICENSE). To contribute to the project check [CONTRIBUTING](CONTRIBUTING.md). If you need commercial support, or you need to have an issue fixed ASAP, check our [GitHub Sponsor page](https://github.com/sponsors/ArcadeData) on both Recurrent and One-Time tiers. All the sponsorship received will be distributed to the active contributors of this project.


## Getting started in 5 minuted

Start ArcadeDB Server with Docker:

```
docker run --rm -p 2480:2480 -p 2424:2424
           -e arcadedb.server.rootPassword=playwithdata
           -e "arcadedb.server.defaultDatabases=OpenBeer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}"
           arcadedata/arcadedb:latest
```

Now open your browser on http://localhost:2480 and play with [ArcadeDB Studio](https://docs.arcadedb.com/#_studio) and the imported `OpenBeer` database to find your favorite beer.

![ArcadeDB Studio](https://arcadedb.com/assets/images/openbeer-demo-graph.png)


Have fun with data!

The ArcadeDB Team
