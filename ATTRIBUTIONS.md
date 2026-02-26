# Third-Party Attributions

This document provides detailed attribution information for third-party software, libraries, and content included in or used by ArcadeDB.

## Table of Contents

1. [Source Code Origins](#source-code-origins)
2. [Derived and Included Code](#derived-and-included-code)
3. [Runtime Dependencies](#runtime-dependencies)
4. [Test Dependencies](#test-dependencies)
5. [Build-Time Dependencies](#build-time-dependencies)
6. [License References](#license-references)

---

## Source Code Origins

### OrientDB

**ArcadeDB includes components originally developed for OrientDB**, an Open Source Multi-Model NoSQL Database.

- **Original Copyright:** 2010-2020 OrientDB LTD
- **License:** Apache License 2.0
- **Website:** https://orientdb.com
- **Repository:** https://github.com/orientechnologies/orientdb

**History:** ArcadeDB was created in 2021 by Luca Garulli, the same founder of OrientDB, after SAP's acquisition of OrientDB LTD. ArcadeDB was written from scratch with a new engine architecture, but reuses OrientDB's SQL engine (now heavily modified) and some utility classes. ArcadeDB maintains compatibility with OrientDB's SQL dialect.

**Components Reused:**
- SQL query engine (heavily modified)
- Various utility classes

**Note:** ArcadeDB is not a fork - it is a new codebase that selectively incorporates and builds upon specific OrientDB components.

---

## Derived and Included Code

### 1. Neo4j openCypher Grammar and TCK

**openCypher** is an open source implementation of the Cypher query language, originally created by Neo4j.

- **Copyright:** Neo4j Sweden AB
- **License:** Apache License 2.0
- **Website:** https://opencypher.org/
- **Trademark Notice:** Cypher® is a registered trademark of Neo4j Inc.

**Included Components:**

| Component | Type | Location | Count |
|-----------|------|----------|-------|
| Cypher25Parser.g4 | ANTLR4 Grammar | `engine/src/main/antlr4/com/arcadedb/query/opencypher/grammar/` | 1 file |
| Cypher25Lexer.g4 | ANTLR4 Grammar | `engine/src/main/antlr4/com/arcadedb/query/opencypher/grammar/` | 1 file |
| TCK Test Features | Cucumber/Gherkin Tests | `engine/src/test/resources/opencypher/tck/features/` | 220 files |

**Attribution Notice from TCK License:**

> "This work was created by the collective efforts of the openCypher community.
> Without limiting the terms of Section 6, any Derivative Work that is not
> approved by the public consensus process of the openCypher Implementers Group
> should not be described as 'Cypher' (and Cypher® is a registered trademark of
> Neo4j Inc.) or as 'openCypher'. Extensions by implementers or prototypes or
> proposals for change that have been documented or implemented should only be
> described as 'implementation extensions to Cypher' or as 'proposed changes to
> Cypher that are not yet approved by the openCypher community'."

**License Files:**
- Grammar files: Include full Apache 2.0 header with Neo4j copyright
- TCK feature files: Include full Apache 2.0 header with attribution notice

---

### 2. OpenCypher-Gremlin - StringTranslationUtils

**OpenCypher-Gremlin** (also known as Cypher for Gremlin) enables Cypher queries over Gremlin-enabled graph databases.

- **Copyright:** 2018-2019 Neueda
- **License:** Apache License 2.0
- **Repository:** https://github.com/opencypher/cypher-for-gremlin
- **File:** `gremlin/src/main/java/org/opencypher/gremlin/translation/groovy/StringTranslationUtils.java`

**Modification Note:** This file is a security-patched override of the original OpenCypher-Gremlin implementation. The upstream project is not actively maintained and was unwilling to merge the security fix, necessitating this local override.

**Original Author:** @ExtReMLapin (as noted in code comments)

---

## Runtime Dependencies

The following table lists runtime dependencies bundled with ArcadeDB distributions:

### Core Dependencies

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.slf4j | slf4j-api | 2.0.17 | MIT | https://www.slf4j.org/ |
| org.slf4j | slf4j-jdk14 | 2.0.17 | MIT | https://www.slf4j.org/ |
| ch.qos.logback | logback-classic | 1.5.27 | EPL 1.0 / LGPL 2.1 | https://logback.qos.ch/ |
| ch.qos.logback | logback-core | 1.5.27 | EPL 1.0 / LGPL 2.1 | https://logback.qos.ch/ |
| com.conversantmedia | disruptor | 1.2.21 | Apache 2.0 | https://github.com/conversant/disruptor |
| at.yawk.lz4 | lz4-java | 1.10.3 | Apache 2.0 | https://github.com/yawkat/lz4-java |
| org.antlr | antlr4-runtime | 4.9.1 | BSD 3-Clause | https://www.antlr.org/ |
| com.google.code.gson | gson | 2.13.2 | Apache 2.0 | https://github.com/google/gson |
| org.yaml | snakeyaml | 2.4 | Apache 2.0 | https://bitbucket.org/snakeyaml/snakeyaml |

### Apache Lucene (Full-Text Search)

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.apache.lucene | lucene-core | 10.3.2 | Apache 2.0 | https://lucene.apache.org/ |
| org.apache.lucene | lucene-analysis-common | 10.3.2 | Apache 2.0 | https://lucene.apache.org/ |
| org.apache.lucene | lucene-queryparser | 10.3.2 | Apache 2.0 | https://lucene.apache.org/ |
| org.apache.lucene | lucene-queries | 10.3.2 | Apache 2.0 | https://lucene.apache.org/ |
| org.apache.lucene | lucene-sandbox | 10.3.2 | Apache 2.0 | https://lucene.apache.org/ |
| org.apache.lucene | lucene-facet | 10.3.2 | Apache 2.0 | https://lucene.apache.org/ |
| org.apache.lucene | lucene-spatial-extras | 10.3.2 | Apache 2.0 | https://lucene.apache.org/ |

**Apache Lucene Notice:** Lucene is a registered trademark of The Apache Software Foundation. See the NOTICE file for Lucene's own third-party attributions.

### Spatial and GIS Libraries

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.locationtech.spatial4j | spatial4j | 0.8 | Apache 2.0 | https://github.com/locationtech/spatial4j |
| org.locationtech.jts | jts-core | 1.20.0 | Eclipse Distribution License 1.0 | https://github.com/locationtech/jts |

### Vector Embeddings

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| io.github.jbellis | jvector | 4.0.0-rc.7 | Apache 2.0 | https://github.com/jbellis/jvector |
| org.apache.commons | commons-math3 | 3.6.1 | Apache 2.0 | https://commons.apache.org/proper/commons-math/ |
| org.agrona | agrona | 1.20.0 | Apache 2.0 | https://github.com/real-logic/agrona |

### GraalVM (JavaScript Support)

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.graalvm.sdk | graal-sdk | 25.0.2 | UPL 1.0 | https://www.graalvm.org/ |
| org.graalvm.polyglot | polyglot | 25.0.2 | UPL 1.0 | https://www.graalvm.org/ |
| org.graalvm.polyglot | js | 25.0.2 | UPL 1.0 | https://www.graalvm.org/ |
| org.graalvm.js | js-language | 25.0.2 | UPL 1.0 | https://www.graalvm.org/ |
| org.graalvm.regex | regex | 25.0.2 | UPL 1.0 | https://www.graalvm.org/ |
| org.graalvm.truffle | truffle-api | 25.0.2 | UPL 1.0 | https://www.graalvm.org/ |
| org.graalvm.truffle | truffle-runtime | 25.0.2 | UPL 1.0 | https://www.graalvm.org/ |

**License Note:** UPL = Universal Permissive License 1.0

### Studio Frontend Libraries

| Package | Version | License | Homepage |
|---------|---------|---------|----------|
| marked | 15.x | MIT | https://github.com/markedjs/marked |

### Compression

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.xerial.snappy | snappy-java | 1.1.10.7 | Apache 2.0 | https://github.com/xerial/snappy-java |

### Server and Networking

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| io.undertow | undertow-core | ~2.3.x | Apache 2.0 | https://undertow.io/ |
| io.netty | netty-* | ~4.1.x | Apache 2.0 | https://netty.io/ |

### Apache TinkerPop / Gremlin (Optional Module)

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.apache.tinkerpop | gremlin-core | 3.8.x | Apache 2.0 | https://tinkerpop.apache.org/ |
| org.apache.tinkerpop | gremlin-driver | 3.8.x | Apache 2.0 | https://tinkerpop.apache.org/ |
| org.apache.tinkerpop | gremlin-server | 3.8.x | Apache 2.0 | https://tinkerpop.apache.org/ |
| org.apache.tinkerpop | gremlin-util | 3.8.x | Apache 2.0 | https://tinkerpop.apache.org/ |
| org.apache.tinkerpop | gremlin-groovy | 3.8.x | Apache 2.0 | https://tinkerpop.apache.org/ |
| org.apache.groovy | groovy | ~4.0.x | Apache 2.0 | https://groovy-lang.org/ |

**Note:** Gremlin support is an optional module. These dependencies are only included when the Gremlin module is enabled.

### gRPC (Optional Module)

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| io.grpc | grpc-netty-shaded | 1.79.0 | Apache 2.0 | https://grpc.io/ |
| io.grpc | grpc-protobuf | 1.79.0 | Apache 2.0 | https://grpc.io/ |
| io.grpc | grpc-stub | 1.79.0 | Apache 2.0 | https://grpc.io/ |
| io.grpc | grpc-services | 1.79.0 | Apache 2.0 | https://grpc.io/ |
| com.google.protobuf | protobuf-java | 4.33.5 | BSD 3-Clause | https://protobuf.dev/ |

### Google Core Libraries

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| com.google.guava | guava | 33.5.0-android | Apache 2.0 | https://github.com/google/guava |
| com.google.code.findbugs | jsr305 | 3.0.2 | Apache 2.0 | https://code.google.com/archive/p/jsr-305/ |
| com.google.errorprone | error_prone_annotations | 2.45.0 | Apache 2.0 | https://errorprone.info/ |

### Apache Commons Libraries

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.apache.commons | commons-compress | 1.28.0 | Apache 2.0 | https://commons.apache.org/proper/commons-compress/ |
| org.apache.commons | commons-lang3 | 3.18.0 | Apache 2.0 | https://commons.apache.org/proper/commons-lang/ |
| commons-codec | commons-codec | 1.19.0 | Apache 2.0 | https://commons.apache.org/proper/commons-codec/ |
| commons-io | commons-io | 2.20.0 | Apache 2.0 | https://commons.apache.org/proper/commons-io/ |

### Apache HttpComponents

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.apache.httpcomponents | httpclient | 4.5.14 | Apache 2.0 | https://hc.apache.org/httpcomponents-client-4.5.x/ |
| org.apache.httpcomponents | httpcore | 4.4.16 | Apache 2.0 | https://hc.apache.org/httpcomponents-core-4.4.x/ |

### Security and Authentication

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.conscrypt | conscrypt-openjdk-uber | 2.5.2 | Apache 2.0 | https://github.com/google/conscrypt |
| com.google.auth | google-auth-library-credentials | 1.41.0 | BSD 3-Clause | https://github.com/googleapis/google-auth-library-java |
| com.google.auth | google-auth-library-oauth2-http | 1.41.0 | BSD 3-Clause | https://github.com/googleapis/google-auth-library-java |

### Monitoring and Metrics (Optional)

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| io.micrometer | micrometer-core | 1.16.2 | Apache 2.0 | https://micrometer.io/ |
| io.micrometer | micrometer-observation | 1.16.2 | Apache 2.0 | https://micrometer.io/ |
| org.hdrhistogram | HdrHistogram | 2.2.2 | Public Domain / CC0 | https://hdrhistogram.github.io/HdrHistogram/ |

---

## Test Dependencies

These dependencies are used only for testing and are not included in production distributions:

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.junit.jupiter | junit-jupiter | 6.0.2 | EPL 2.0 | https://junit.org/junit5/ |
| org.junit.jupiter | junit-jupiter-api | 6.0.2 | EPL 2.0 | https://junit.org/junit5/ |
| org.junit.jupiter | junit-jupiter-engine | 6.0.2 | EPL 2.0 | https://junit.org/junit5/ |
| org.junit.jupiter | junit-jupiter-params | 6.0.2 | EPL 2.0 | https://junit.org/junit5/ |
| org.assertj | assertj-core | 3.27.7 | Apache 2.0 | https://assertj.github.io/doc/ |
| org.mockito | mockito-core | 5.21.0 | MIT | https://site.mockito.org/ |
| org.testcontainers | testcontainers | 2.0.3 | MIT | https://testcontainers.com/ |
| org.testcontainers | testcontainers-junit-jupiter | 2.0.3 | MIT | https://testcontainers.com/ |
| org.testcontainers | testcontainers-toxiproxy | 2.0.3 | MIT | https://testcontainers.com/ |
| org.awaitility | awaitility | 4.3.0 | Apache 2.0 | https://github.com/awaitility/awaitility |
| com.jayway.jsonpath | json-path | 2.10.0 | Apache 2.0 | https://github.com/json-path/JsonPath |
| com.github.docker-java | docker-java-api | 3.7.0 | Apache 2.0 | https://github.com/docker-java/docker-java |
| net.bytebuddy | byte-buddy | 1.18.3 | Apache 2.0 | https://bytebuddy.net/ |

---

## Build-Time Dependencies

These dependencies are used only during the build process:

| Group ID | Artifact ID | Version | License | Homepage |
|----------|-------------|---------|---------|----------|
| org.apache.maven.plugins | maven-compiler-plugin | 3.x | Apache 2.0 | https://maven.apache.org/plugins/maven-compiler-plugin/ |
| org.apache.maven.plugins | maven-surefire-plugin | 3.x | Apache 2.0 | https://maven.apache.org/surefire/maven-surefire-plugin/ |
| org.jacoco | jacoco-maven-plugin | 0.8.x | EPL 2.0 | https://www.jacoco.org/jacoco/ |
| org.antlr | antlr4-maven-plugin | 4.9.1 | BSD 3-Clause | https://www.antlr.org/ |
| io.grpc | protoc-gen-grpc-java | 1.79.0 | Apache 2.0 | https://grpc.io/ |
| com.github.os72 | protoc-jar-maven-plugin | 3.x | Apache 2.0 | https://github.com/os72/protoc-jar-maven-plugin |

---

## License References

Full license texts are available in the `LICENSES/` directory:

- **Apache-2.0.txt** - Apache License, Version 2.0
- **MIT.txt** - MIT License
- **BSD-3-Clause.txt** - BSD 3-Clause License
- **EPL-2.0.txt** - Eclipse Public License 2.0
- **UPL-1.0.txt** - Universal Permissive License 1.0
- **LGPL-2.1-or-later.txt** - GNU Lesser General Public License 2.1 or later
- **EDL-1.0.txt** - Eclipse Distribution License 1.0

### Primary Project License

ArcadeDB itself is licensed under the **Apache License 2.0**.

See the [LICENSE](LICENSE) file in the root directory for the full license text.

### REUSE Compliance

This project follows the [REUSE Software Specification 3.3](https://reuse.software/spec/) for copyright and license attribution. All source files contain SPDX license identifiers and copyright notices in their headers.

---

## Contributor Acknowledgments

ArcadeDB is built on the shoulders of giants. We thank:

- **OrientDB community** - For the SQL engine and utility components
- **openCypher community** - For the standardized Cypher query language and TCK
- **Neo4j** - For creating Cypher and supporting openCypher
- **Apache Software Foundation** - For Lucene, TinkerPop, and numerous other libraries
- **The Java community** - For the robust ecosystem and tooling
- **All individual contributors** - Named in git history and @author tags

Special recognition to original authors mentioned in source files:
- Johann Sorel - SQLMethod interface design
- @ExtReMLapin - OpenCypher-Gremlin security fix

---

## How to Contribute

When adding new dependencies to ArcadeDB:

1. **Check License Compatibility** - Ensure the license is compatible with Apache 2.0
   - ✅ Approved: Apache 2.0, MIT, BSD, EPL, UPL, public domain
   - ❌ Incompatible: GPL, AGPL, proprietary licenses without explicit permission

2. **Update This File** - Add the dependency to the appropriate section above

3. **Update NOTICE File** - If the dependency is Apache-licensed and has its own NOTICE file, incorporate required notices

4. **Add License Text** - If introducing a new license type, add the full text to `LICENSES/` directory

5. **SPDX Headers** - Ensure all new source files have proper SPDX-FileCopyrightText and SPDX-License-Identifier headers

For detailed contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Questions or Concerns

If you have questions about licensing or attribution:

- **Open an issue:** https://github.com/ArcadeData/arcadedb/issues
- **Join our Discord:** https://discord.gg/w2Npx2B7hZ
- **Email:** info@arcadedata.com

---

*This attribution document is maintained as part of ArcadeDB's commitment to open source compliance and transparency. Last updated: 2026-02-07*
