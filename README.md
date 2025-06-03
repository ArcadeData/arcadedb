# ![ArcadeDB](https://arcadedb.com/assets/images/arcadedb-logo.png)

<h2 align="center">Multi Model DBMS Built for Extreme Performance</h2>

<p align="center">
  <a href="https://github.com/ArcadeData/arcadedb/releases"><img src="https://img.shields.io/github/v/release/arcadedata/arcadedb?color=%23ff00a0&include_prereleases&label=version&sort=semver"></a>
  &nbsp;
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-green.svg"></a>
  &nbsp;
  <a href="https://docs.oracle.org/en/java/21/"><img src="https://img.shields.io/badge/Java-%3D21-green.svg"></a>
  &nbsp;
  <a href="https://docs.oracle.org/en/java/17/"><img src="https://img.shields.io/badge/Java-%3D17-green.svg"></a>
  &nbsp;
  <a href="https://api.reuse.software/info/github.com/ArcadeData/arcadedb"><img src="https://api.reuse.software/badge/github.com/ArcadeData/arcadedb"></a>
  &nbsp;
  <a href="https://maven-badges.herokuapp.com/maven-central/com.arcadedb/arcadedb-parent"><img src="https://maven-badges.herokuapp.com/maven-central/com.arcadedb/arcadedb-parent/badge.svg"></a>
  &nbsp;
  <a href="https://hub.docker.com/repository/docker/arcadedata/arcadedb/general"><img src="https://img.shields.io/docker/pulls/arcadedata/arcadedb"></a>
</p>

<p align="center">
  <a href="https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml"><img src="https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml/badge.svg"></a>
  &nbsp;
  <a href="https://www.codacy.com/gh/ArcadeData/arcadedb/dashboard?utm_source=github.com&utm_medium=referral&utm_content=ArcadeData/arcadedb&utm_campaign=Badge_Coverage"><img src="https://app.codacy.com/project/badge/Coverage/1f971260db1e46638bd3fd91e3ebf668"></a>
  &nbsp;
  <a href="https://app.codacy.com/gh/ArcadeData/arcadedb?utm_source=github.com&utm_medium=referral&utm_content=ArcadeData/arcadedb&utm_campaign=Badge_Grade_Settings"><img src="https://api.codacy.com/project/badge/Grade/d40cc721f39b49eb81408307960f145b"></a>
  &nbsp;
  <a href="https://www.meterian.io/report/gh/ArcadeData/arcadedb"><img src="https://www.meterian.io/badge/gh/ArcadeData/arcadedb/security?branch=main"></a>
  &nbsp;
  <a href="https://www.meterian.io/report/gh/ArcadeData/arcadedb"><img src="https://www.meterian.io/badge/gh/ArcadeData/arcadedb/stability?branch=main"></a>
</p>

<p align="center">
  <a href="https://discord.gg/w2Npx2B7hZ"><img width="208" height="97" src="https://arcadedb.com/assets/images/discord_button.png" alt="Join Discord"></a>
</p>

<p align="center">
	<a href="https://github.com/arcadedata/arcadedb"><img height="25" src="studio/src/main/resources/static/images/social/github.svg" alt="Github"></a>
	&nbsp;
  <a href="https://www.linkedin.com/company/arcadedb/"><img height="25" src="studio/src/main/resources/static/images/social/linkedin.svg" alt="LinkedIn"></a>
  &nbsp;
  <a href="https://bsky.app/profile/arcadedb.bsky.social"><img height="25" src="studio/src/main/resources/static/images/social/bluesky.svg" alt="Bluesky"></a>
  &nbsp;
  <a href="https://twitter.com/arcade_db"><img height="25" src="studio/src/main/resources/static/images/social/twitter.svg" alt="Twitter"></a>
  &nbsp;
  <a href="https://www.youtube.com/@ArcadeDB"><img height="25" src="studio/src/main/resources/static/images/social/youtube.svg" alt="Youtube"></a>
  &nbsp;
  <a href="https://discord.gg/w2Npx2B7hZ"><img height="25" src="studio/src/main/resources/static/images/social/discord.svg" alt="Discord"></a>
  &nbsp;
  <a href="https://stackoverflow.com/questions/tagged/arcadedb"><img height="25" src="studio/src/main/resources/static/images/social/stack-overflow.svg" alt="StackOverflow"></a>
	&nbsp;
	<a href="https://blog.arcadedb.com/"><img height="25" src="studio/src/main/resources/static/images/social/blog.svg" alt="Blog"></a>
</p>

ArcadeDB is a Multi-Model DBMS (created originally as a fork
from [OrientDB Open Source project](https://github.com/orientechnologies/orientdb) after the acquisition by SAP) with a brand-new
engine made of Alien Technology, able to crunch millions of records per second on common hardware with the usage of
minimal resources. ArcadeDB is written in LLJ: Low Level Java. It's still Java21+ but only using low level API to use advanced
mechanical sympathy techniques and a reduced pressure of the Garbage Collector. It's highly optimized for extreme performance. Runs
from a Raspberry Pi to multiple servers on the cloud.

ArcadeDB is fully transactional DBMS with support for ACID transactions, structured and unstructured data, native graph engine (no
joins but links between records), full-text indexing, geospatial querying, and advanced security.

ArcadeDB supports the following models:

- [Graph Database](https://docs.arcadedb.com#Graph-Model) (compatible with Neo4j Cypher, Apache Tinkerpop Gremlin and OrientDB SQL)
- [Document Database](https://docs.arcadedb.com#Document-Model) (compatible with the MongoDB driver + MongoDB queries and OrientDB
  SQL)
- [Key/Value](https://docs.arcadedb.com#KeyValue-Model) (compatible with the Redis driver)
- [Search Engine](https://docs.arcadedb.com/#SearchEngine-Model)
- [Time Series](https://docs.arcadedb.com#TimeSeries-Model)
- [Vector Embedding](https://docs.arcadedb.com/#VectorEmbedding-Model)

ArcadeDB understands multiple languages:

- [SQL](https://docs.arcadedb.com#SQL) (from OrientDB SQL)
- Neo4j [Cypher (Open Cypher)](https://docs.arcadedb.com#Cypher)
- [Apache Gremlin (Apache Tinkerpop v3.7.x)](https://docs.arcadedb.com#Gremlin-API)
- [GraphQL Language](https://docs.arcadedb.com#GraphQL)
- [MongoDB Query Language](https://docs.arcadedb.com#MongoDB-API)

ArcadeDB can be used as:

- Embedded from any language on top of the Java Virtual Machine
- Remotely by using [HTTP/JSON](https://docs.arcadedb.com#HTTP-API)
- Remotely by using a [Postgres driver](https://docs.arcadedb.com#Postgres-Driver) (ArcadeDB implements Postgres Wire protocol)
- Remotely by using a [Redis driver](https://docs.arcadedb.com#Redis-API) (only a subset of the operations are implemented)
- Remotely by using a [MongoDB driver](https://docs.arcadedb.com#MongoDB-API) (only a subset of the operations are implemented)

For more information, see the [documentation](https://docs.arcadedb.com).

### Getting started in 5 minutes

Start ArcadeDB Server with Docker:

```
docker run --rm -p 2480:2480 -p 2424:2424 \
           -e JAVA_OPTS="-Darcadedb.server.rootPassword=playwithdata -Darcadedb.server.defaultDatabases=Imported[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}" \
           arcadedata/arcadedb:latest
```

Now open your browser on http://localhost:2480 and play with [ArcadeDB Studio](https://docs.arcadedb.com/#_studio) and the
imported `OpenBeer` database to find your favorite beer.

![ArcadeDB Studio](https://arcadedb.com/assets/images/openbeer-demo-graph.png)

ArcadeDB is cloud-ready with [Docker](https://docs.arcadedb.com/#Docker) and [Kubernetes](https://docs.arcadedb.com/#Kubernetes) support.

You can also [download the latest release](https://github.com/ArcadeData/arcadedb/releases), unpack it on your local hard drive and start the server with `bin/server.sh` or `bin/server.bat` for Windows.

### Releases

There are three variants of (about monthly) releases:

- `full` - this is the complete package of including all modules
- `minimal` - this package excludes the `gremlin`, `redisw`, `mongodbw`, `graphql` modules
- `headless` - this package excludes the `gremlin`, `redisw`, `mongodbw`, `graphql`, `studio` modules

The nightly builds of the repository head can be found [here](https://central.sonatype.com/service/rest/repository/browse/maven-snapshots/com/arcadedb/arcadedb-package/).

### Java Versions

Starting from ArcadeDB 24.4.1 code is compatible with Java 21.

Java 21 packages are available on [Maven central](https://repo.maven.apache.org/maven2/com/arcadedb/) and docker images on [Docker Hub](https://hub.docker.com/r/arcadedata/arcadedb).

We also support Java 17 on a separate branch `java17` for those who cannot upgrade to Java 21 yet through GitHub packages.

To use Java 17 inside your project, add the repository to your `pom.xml` and reference dependencies as follows:

```xml
    <repositories>
        <repository>
            <name>github</name>
            <id>github</id>
            <url>https://maven.pkg.github.com/ArcadeData/arcadedb</url>
        </repository>
    </repositories>
    <dependencies>
      <dependency>
          <groupId>com.arcadedb</groupId>
          <artifactId>arcadedb-engine</artifactId>
          <version>25.5.1-java17</version>
      </dependency>
    </dependencies>
```

Docker images are available on ghcr.io too:

```shell
docker pull ghcr.io/arcadedata/arcadedb:25.5.1-java17
```

### Community

Join our growing community around the world, for ideas, discussions and help regarding ArcadeDB.

- Chat live with us on [Discord](https://discord.gg/w2Npx2B7hZ)
- Follow us on [Twitter](https://twitter.com/arcade_db)
- or on [Bluesky](https://bsky.app/profile/arcadedb.bsky.social)
- Connect with us on [LinkedIn](https://www.linkedin.com/products/arcadedb)
- or on [Facebook](https://www.facebook.com/arcadedb)
- Questions tagged `#arcadedb` on [Stack Overflow](https://stackoverflow.com/questions/tagged/arcadedb)
- View our official [Blog](https://blog.arcadedb.com/)

### Security

For security issues kindly email us at support@arcadedb.com instead of posting a public issue on GitHub.

### License

ArcadeDB is Free for any usage and licensed under the liberal [Open Source Apache 2 license](LICENSE). If you need commercial
support, or you need to have an issue fixed ASAP, check our [GitHub Sponsor page](https://github.com/sponsors/ArcadeData) on both
Recurrent and One-Time tiers. All the sponsorship received will be distributed to the active contributors of this project.

### Thanks To

<a href="https://www.yourkit.com"><img src="https://www.yourkit.com/images/yklogo.png"></a> for providing YourKit Profiler to our committers.

### Contributing

We would love for you to get involved with ArcadeDB project.
If you wish to help, you can learn more about how you can contribute to this project in the [contribution guide](CONTRIBUTING.md).

Have fun with data!

The ArcadeDB Team

---

#### TL;DR: [ArcadeDB](https://arcadedb.com) is an open-source multi-model NoSQL database systems.
