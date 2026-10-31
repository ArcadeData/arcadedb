/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * How a schema or database-configuration change that does not go through an ordinary DDL entry reaches the followers,
 * end to end on ONE two-node cluster shared by every test ({@link SharedClusterRaftHATest}). Merged from
 * {@code Issue9747BareSchemaSaveReplicationIT}, {@code Issue9762TypeRenameReplicationIT},
 * {@code Issue9763DatabaseConfigurationReplicationIT}, {@code Issue9764JsonlImportSchemaMembersReplicationIT} and
 * {@code Issue7438ReplicaWritesDuringLeaderDdlIT}, which all ran on the same fixture: two servers, the default quorum,
 * in-memory Raft storage and no configuration override.
 * <ul>
 * <li><b>#9747</b>: a schema change that ended in a bare {@code LocalSchema.saveConfiguration()} - outside a schema
 * recording session - rewrote the leader's {@code schema.json} and shipped nothing, so the followers kept the old schema
 * until an unrelated DDL happened to ship the whole document. A constraint enforced on the leader was then not enforced on
 * a node that became leader after a failover, and database settings answered differently per node. One test per writer
 * family; each change is made on the leader, then every node is read back twice: its in-memory schema and its own
 * {@code schema.json}.</li>
 * <li><b>#9762</b>: {@code ALTER TYPE ... NAME} renamed the type and its bucket and index files on the leader only. The
 * rename ended in a bare schema save, so no {@code SCHEMA_ENTRY} was proposed, and a schema entry could not have carried
 * it anyway: it lists files to create and to remove, not renames. Every node is checked three ways after the rename: its
 * in-memory schema, its own {@code schema.json}, and the files on its disk.</li>
 * <li><b>#9763</b>: {@code ALTER DATABASE} for a setting kept in the database's {@code configuration.json} was written on
 * the node that ran it only. The followers, and whichever of them became leader after a failover, kept the previous
 * value; for {@code arcadedb.dateImplementation} that meant a different answer to the same read. Every node is checked in
 * memory, in its own {@code configuration.json} and, for the date implementation, by the class a read returns.</li>
 * <li><b>#9764</b>: a JSONL import restored the export's triggers, materialized views and function libraries through
 * {@code LocalSchema.restoreSchemaMembersFromJSON} followed by a bare {@code saveConfiguration()}, so the followers held
 * the imported types and records but none of the members.</li>
 * <li><b>#7438</b>: a replica keeps inserting into a bucket while the leader runs a {@code CREATE INDEX} over it. The
 * leader now refuses replica entries for the database for the length of the DDL, and the replica's retry loop rides it
 * out: every insert the replica was acknowledged for is in the index, on every node.</li>
 * </ul>
 * Why these share a cluster: every type, schema extension, trigger, view and function library a test creates is named
 * through {@link #uniqueName(String)}, and no assertion counts the types, files or buckets of the whole database: the
 * file checks of #9762 look only at files named after the test's own types, the record and index counts of #7438 are of
 * its own type. The settings the tests change are database-wide and replicated, so each test puts them back in a
 * {@code finally}: the schema settings of #9747 ({@code dateFormat}, {@code dateTimeFormat}, {@code zoneId}) by the
 * reverse change, the configuration settings of #9763 ({@code typeDefaultBuckets}, {@code txRetries},
 * {@code dateImplementation}) by a reverse {@code ALTER DATABASE} on the leader ({@link #restoreDatabaseSetting}), and
 * the {@code txWalFlush} value one #9763 test writes into a follower's own {@code configuration.json} past the
 * replication by removing it from that follower's file again: another #9763 test asserts no node's file holds it. The
 * peer capabilities two tests withdraw (#9762, #9763) were already restored in a {@code finally} by the sources. No
 * test stops, restarts or isolates a node or moves leadership.
 * <p>
 * Siblings, each with the fixture its tests need: {@link HaSchemaDdlReplicationThreeNodeIT} (#10349, #10506 and #8404,
 * three servers, shared), {@link HaSchemaDdlReplicationRestartIT} (#9762's restart test, restarts a follower),
 * {@link HaSchemaDdlReplicationFailoverIT} (#9747's failover test, stops the leader),
 * {@link HaSchemaDdlReplicationUnknownOutcomeIT} (#9904, a test hook that gives up on a dispatched entry) and
 * {@link HaSchemaDdlReplicationIsolationIT} (#9934, isolates the leader).
 * <p>
 * #7438's source class was tagged {@code slow}, so its test is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaSchemaDdlReplicationIT extends SharedClusterRaftHATest {

  /** #9762. */
  private static final int RECORDS = 50;

  /** #9763: how long a node is given to observe a peer's advertisement change (a few refresh rounds). */
  private static final long CAPABILITY_CONVERGENCE_TIMEOUT_MS = 30_000L;

  /** #9764. */
  private static final String SOURCE_PATH = "target/issue9764-source";
  private static final String EXPORT_FILE = "target/issue9764-export.jsonl.tgz";

  /** #7438. */
  private static final int SEED             = 3000;
  private static final int MIN_ACKNOWLEDGED = 20;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @BeforeEach
  @AfterEach
  void cleanIssue9764Source() {
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    new File(EXPORT_FILE).delete();
  }

  // ------------------------------------------------------------------ bare schema saves (#9747)

  /** Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}. */
  @Test
  void alterPropertyConstraintsViaSqlReachEveryNode() throws Exception {
    final Database leader = leader();
    final String p = uniqueName("P");
    leader.command("sql", "CREATE DOCUMENT TYPE " + p);
    leader.command("sql", "CREATE PROPERTY " + p + ".x INTEGER");
    leader.command("sql", "CREATE PROPERTY " + p + ".s STRING");
    assertClusterConsistency();

    leader.command("sql", "ALTER PROPERTY " + p + ".x MANDATORY true");
    leader.command("sql", "ALTER PROPERTY " + p + ".x NOTNULL true");
    leader.command("sql", "ALTER PROPERTY " + p + ".x READONLY true");
    leader.command("sql", "ALTER PROPERTY " + p + ".x MIN 1");
    leader.command("sql", "ALTER PROPERTY " + p + ".x MAX 10");
    leader.command("sql", "ALTER PROPERTY " + p + ".s REGEXP '[a-z]+'");
    leader.command("sql", "ALTER PROPERTY " + p + ".s CUSTOM label = 'probe'");

    assertOnEveryNode(schema -> {
      final Property x = schema.getType(p).getProperty("x");
      assertThat(x.isMandatory()).isTrue();
      assertThat(x.isNotNull()).isTrue();
      assertThat(x.isReadonly()).isTrue();
      assertThat(x.getMin()).isEqualTo("1");
      assertThat(x.getMax()).isEqualTo("10");
      final Property s = schema.getType(p).getProperty("s");
      assertThat(s.getRegexp()).isEqualTo("[a-z]+");
      assertThat(s.getCustomValue("label")).isEqualTo("probe");
    }, file -> {
      final JSONObject x = file.getJSONObject("types").getJSONObject(p).getJSONObject("properties").getJSONObject("x");
      assertThat(x.getBoolean("mandatory", false)).isTrue();
      assertThat(x.getBoolean("readonly", false)).isTrue();
    });
  }

  /** Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}. */
  @Test
  void propertySettersViaJavaReachEveryNode() throws Exception {
    final Database leader = leader();
    final String j = uniqueName("J");
    leader.command("sql", "CREATE DOCUMENT TYPE " + j);
    leader.command("sql", "CREATE PROPERTY " + j + ".x INTEGER");
    leader.command("sql", "CREATE PROPERTY " + j + ".s STRING");
    leader.command("sql", "CREATE PROPERTY " + j + ".l LIST");
    assertClusterConsistency();

    final Property x = leader.getSchema().getType(j).getProperty("x");
    x.setReadonly(true);
    x.setMandatory(true);
    x.setNotNull(true);
    x.setMin("2");
    x.setMax("20");
    final Property s = leader.getSchema().getType(j).getProperty("s");
    s.setRegexp("[0-9]+");
    s.setHidden(true);
    s.setCompression("fast");
    s.setCustomValue("k", "v");
    leader.getSchema().getType(j).getProperty("l").setOfType("STRING");

    assertOnEveryNode(schema -> {
      final Property nx = schema.getType(j).getProperty("x");
      assertThat(nx.isReadonly()).isTrue();
      assertThat(nx.isMandatory()).isTrue();
      assertThat(nx.isNotNull()).isTrue();
      assertThat(nx.getMin()).isEqualTo("2");
      assertThat(nx.getMax()).isEqualTo("20");
      final Property ns = schema.getType(j).getProperty("s");
      assertThat(ns.getRegexp()).isEqualTo("[0-9]+");
      assertThat(ns.isHidden()).isTrue();
      assertThat(ns.getCompression()).isEqualTo("fast");
      assertThat(ns.getCustomValue("k")).isEqualTo("v");
      assertThat(schema.getType(j).getProperty("l").getOfType()).isEqualTo("STRING");
    }, file -> {
      final JSONObject nx = file.getJSONObject("types").getJSONObject(j).getJSONObject("properties").getJSONObject("x");
      assertThat(nx.getBoolean("readonly", false)).isTrue();
      assertThat(nx.getBoolean("mandatory", false)).isTrue();
    });
  }

  /**
   * Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}.
   * <p>
   * The constraint has to be ENFORCED on a follower's copy, not just readable: the follower is the next leader after a
   * failover, and a record it accepts there violates what the leader refused.
   */
  @Test
  void aMandatoryPropertyIsEnforcedOnTheFollowerSchema() throws Exception {
    final Database leader = leader();
    final String m = uniqueName("M");
    leader.command("sql", "CREATE DOCUMENT TYPE " + m);
    leader.command("sql", "CREATE PROPERTY " + m + ".x INTEGER");
    assertClusterConsistency();

    leader.getSchema().getType(m).getProperty("x").setMandatory(true);
    // An ordinary record commit afterwards: the issue saw this carry nothing across either
    leader.transaction(() -> leader.newDocument(m).set("x", 1).save());
    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().getType(m).getProperty("x").isMandatory())
          .as("server %d", i).isTrue();
  }

  /**
   * Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}.
   * <p>
   * The path a user takes: the statement sent over HTTP to a FOLLOWER, which forwards it to the leader, where the HTTP
   * handler runs it inside a transaction of its own - so the schema save is postponed to the end of that transaction.
   */
  @Test
  void alterPropertySentOverHttpToAFollowerReachesEveryNode() throws Exception {
    final String dateFormatBefore = leader().getSchema().getDateFormat();
    try {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
      final int followerIndex = (leaderIndex + 1) % getServerCount();
      final String h = uniqueName("H");
      leader().command("sql", "CREATE DOCUMENT TYPE " + h);
      leader().command("sql", "CREATE PROPERTY " + h + ".x INTEGER");
      assertClusterConsistency();

      assertThat(executeCommand(followerIndex, "sql", "ALTER PROPERTY " + h + ".x MANDATORY true")).isNotNull();
      assertThat(executeCommand(followerIndex, "sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy/MM/dd'")).isNotNull();

      assertOnEveryNode(schema -> {
        assertThat(schema.getType(h).getProperty("x").isMandatory()).isTrue();
        assertThat(schema.getDateFormat()).isEqualTo("yyyy/MM/dd");
      }, file -> assertThat(file.getJSONObject("types").getJSONObject(h).getJSONObject("properties").getJSONObject("x")
          .getBoolean("mandatory", false)).isTrue());
    } finally {
      leader().command("sql", "ALTER DATABASE `arcadedb.dateFormat` '" + dateFormatBefore + "'");
    }
  }

  /** Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}. */
  @Test
  void schemaExtensionReachesEveryNode() throws Exception {
    final Database leader = leader();
    final String probe = uniqueName("probe");
    leader.getSchema().setExtension(probe, new JSONObject().put("a", 1));

    assertOnEveryNode(schema -> assertThat(schema.getExtension(probe).getInt("a")).isEqualTo(1),
        file -> assertThat(file.getJSONObject("extensions").getJSONObject(probe).getInt("a")).isEqualTo(1));

    leader.getSchema().setExtension(probe, null);
    assertOnEveryNode(schema -> assertThat(schema.getExtension(probe)).isNull(),
        file -> assertThat(file.getJSONObject("extensions", new JSONObject()).has(probe)).isFalse());
  }

  /** Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}. */
  @Test
  void alterDatabaseSchemaSettingsReachEveryNode() throws Exception {
    final Database leader = leader();
    final String dateFormatBefore = leader.getSchema().getDateFormat();
    final String dateTimeFormatBefore = leader.getSchema().getDateTimeFormat();
    final ZoneId zoneIdBefore = leader.getSchema().getZoneId();
    try {
      leader.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'dd-MM-yyyy'");
      leader.command("sql", "ALTER DATABASE `arcadedb.dateTimeFormat` 'dd-MM-yyyy HH:mm:ss'");
      ((LocalSchema) leader.getSchema().getEmbedded()).setZoneId(ZoneId.of("Asia/Tokyo"));

      assertOnEveryNode(schema -> {
        assertThat(schema.getDateFormat()).isEqualTo("dd-MM-yyyy");
        assertThat(schema.getDateTimeFormat()).isEqualTo("dd-MM-yyyy HH:mm:ss");
        assertThat(schema.getZoneId()).isEqualTo(ZoneId.of("Asia/Tokyo"));
      }, file -> {
        final JSONObject settings = file.getJSONObject("settings");
        assertThat(settings.getString("dateFormat")).isEqualTo("dd-MM-yyyy");
        assertThat(settings.getString("dateTimeFormat")).isEqualTo("dd-MM-yyyy HH:mm:ss");
        assertThat(settings.getString("zoneId")).isEqualTo("Asia/Tokyo");
      });
    } finally {
      final Database current = leader();
      current.command("sql", "ALTER DATABASE `arcadedb.dateFormat` '" + dateFormatBefore + "'");
      current.command("sql", "ALTER DATABASE `arcadedb.dateTimeFormat` '" + dateTimeFormatBefore + "'");
      ((LocalSchema) current.getSchema().getEmbedded()).setZoneId(zoneIdBefore);
    }
  }

  /** Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}. */
  @Test
  void edgeTypeLightweightAndUniqueReachEveryNode() throws Exception {
    final Database leader = leader();
    final String ev = uniqueName("EV");
    final String el = uniqueName("EL");
    final String eu = uniqueName("EU");
    leader.command("sql", "CREATE VERTEX TYPE " + ev);
    leader.command("sql", "CREATE EDGE TYPE " + el);
    leader.command("sql", "CREATE EDGE TYPE " + eu);
    assertClusterConsistency();

    // Lightweight and unique: no index is built for a lightweight type, so nothing but the flags changes
    leader.command("sql", "ALTER TYPE " + el + " WITH lightweight = true");
    leader.command("sql", "ALTER TYPE " + el + " WITH unique = true");
    leader.command("sql", "ALTER TYPE " + eu + " WITH unique = true");

    assertOnEveryNode(schema -> {
      final LocalEdgeType elType = (LocalEdgeType) schema.getType(el);
      assertThat(elType.isLightweight()).isTrue();
      assertThat(elType.isUnique()).isTrue();
      assertThat(((LocalEdgeType) schema.getType(eu)).isUnique()).isTrue();
    }, file -> {
    });
  }

  /** Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}. */
  @Test
  void timeSeriesDownsamplingPolicyReachesEveryNode() throws Exception {
    final Database leader = leader();
    final String ts = uniqueName("TS");
    leader.command("sql", "CREATE TIMESERIES TYPE " + ts + " TIMESTAMP ts FIELDS (v DOUBLE)");
    assertClusterConsistency();

    leader.command("sql", "ALTER TIMESERIES TYPE " + ts + " ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS");

    assertOnEveryNode(schema -> assertThat(((LocalTimeSeriesType) schema.getType(ts)).getDownsamplingTiers()).hasSize(1),
        file -> {
        });
  }

  /** Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}. */
  @Test
  void bucketSelectionStrategyReachesEveryNode() throws Exception {
    final Database leader = leader();
    final String b = uniqueName("B");
    leader.command("sql", "CREATE DOCUMENT TYPE " + b + " BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY " + b + ".tenant STRING");
    leader.command("sql", "CREATE INDEX ON " + b + " (tenant) UNIQUE");
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE " + b + " BucketSelectionStrategy `partitioned('tenant')`");

    assertOnEveryNode(schema -> assertThat(schema.getType(b).getBucketSelectionStrategy().getName()).isEqualTo("partitioned"),
        file -> assertThat(file.getJSONObject("types").getJSONObject(b).getJSONObject("bucketSelectionStrategy").getString("name")).isEqualTo(
            "partitioned"));
  }

  /**
   * Issue #9747, from {@code Issue9747BareSchemaSaveReplicationIT}.
   * <p>
   * The needsRepartition flag: set by a strategy change on a populated type, cleared by REBUILD TYPE. A follower that kept
   * it set would decline to prune the type's queries after a failover; one that never got it would prune over records
   * the old strategy placed.
   */
  @Test
  void needsRepartitionFlagReachesEveryNode() throws Exception {
    final Database leader = leader();
    final String nr = uniqueName("NR");
    leader.command("sql", "CREATE DOCUMENT TYPE " + nr + " BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY " + nr + ".tenant STRING");
    leader.command("sql", "CREATE INDEX ON " + nr + " (tenant) UNIQUE");
    leader.transaction(() -> {
      for (int i = 0; i < 10; i++)
        leader.newDocument(nr).set("tenant", "t" + i).save();
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE " + nr + " BucketSelectionStrategy `partitioned('tenant')`");
    assertOnEveryNode(schema -> assertThat(schema.getType(nr).isNeedsRepartition()).isTrue(), file -> {
    });

    leader.command("sql", "REBUILD TYPE " + nr + " WITH repartition = true");
    assertOnEveryNode(schema -> assertThat(schema.getType(nr).isNeedsRepartition()).isFalse(), file -> {
    });
  }

  // ------------------------------------------------------------------ type rename (#9762)

  /** Issue #9762, from {@code Issue9762TypeRenameReplicationIT}. */
  @Test
  void aDocumentTypeRenameReachesEveryNodeWithItsBucketAndIndexFiles() throws Exception {
    final Database leader = leader();
    final String r = uniqueName("R");
    final String r2 = uniqueName("R2");
    leader.command("sql", "CREATE DOCUMENT TYPE " + r + " BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY " + r + ".id INTEGER");
    leader.command("sql", "CREATE INDEX ON " + r + " (id) UNIQUE");
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument(r).set("id", i).save();
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE " + r + " NAME " + r2);

    assertRenamedOnEveryNode(r, r2, RECORDS);

    // The renamed files keep their ids, so the pages of a later transaction land in them on every node
    leader().transaction(() -> leader().newDocument(r2).set("id", RECORDS).save());
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(r2, false)).as("server %d", i).isEqualTo(RECORDS + 1);
  }

  /** Issue #9762, from {@code Issue9762TypeRenameReplicationIT}. */
  @Test
  void aVertexTypeRenameMovesItsEdgeBucketsOnEveryNode() throws Exception {
    final Database leader = leader();
    // Not V1/V2/E1/E2: the fixture creates those
    final String vr = uniqueName("Vr");
    final String vr2 = uniqueName("Vr2");
    final String er = uniqueName("Er");
    leader.command("sql", "CREATE VERTEX TYPE " + vr);
    leader.command("sql", "CREATE EDGE TYPE " + er);
    leader.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < RECORDS; i++) {
        final MutableVertex v = leader.newVertex(vr).set("id", i).save();
        if (previous != null)
          previous.newEdge(er, v).save();
        previous = v;
      }
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE " + vr + " NAME " + vr2);

    assertRenamedOnEveryNode(vr, vr2, RECORDS);
    for (int i = 0; i < getServerCount(); i++)
      try (final ResultSet rs = getServerDatabase(i, getDatabaseName()).query("sql",
          "SELECT count(*) AS c FROM (SELECT expand(out('" + er + "')) FROM " + vr2 + ")")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("server %d", i).isEqualTo(RECORDS - 1);
      }
  }

  /**
   * Issue #9762, from {@code Issue9762TypeRenameReplicationIT}.
   * <p>
   * The user's path: the statement sent over HTTP to a FOLLOWER, which forwards it to the leader.
   */
  @Test
  void aRenameSentOverHttpToAFollowerReachesEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database leader = leader();
    final String h = uniqueName("H");
    final String h2 = uniqueName("H2");
    leader.command("sql", "CREATE DOCUMENT TYPE " + h);
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument(h).set("id", i).save();
    });
    assertClusterConsistency();

    assertThat(executeCommand(followerIndex, "sql", "ALTER TYPE " + h + " NAME " + h2)).isNotNull();

    assertRenamedOnEveryNode(h, h2, RECORDS);
  }

  /**
   * Issue #9762, from {@code Issue9762TypeRenameReplicationIT}.
   * <p>
   * The Java API on a FOLLOWER's database has no exchange to forward through: the rename is refused there before a single
   * file moves, rather than renaming the files on that node alone.
   */
  @Test
  void aRenameOnAFollowerIsRefusedAndNothingMoves() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String f = uniqueName("F");
    final String f2 = uniqueName("F2");
    leader().command("sql", "CREATE DOCUMENT TYPE " + f);
    assertClusterConsistency();

    final Database follower = getServerDatabase(followerIndex, getDatabaseName());
    final Throwable refusal = catchThrowable(() -> follower.getSchema().getType(f).rename(f2));
    assertThat(causeChainOf(refusal)).as("refused on the follower: %s", refusal)
        .anyMatch(ServerIsNotTheLeaderException.class::isInstance);

    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      assertThat(database.getSchema().existsType(f)).as("server %d", i).isTrue();
      assertThat(database.getSchema().existsType(f2)).as("server %d", i).isFalse();
      assertThat(filesNamedAfter(database, f2)).as("server %d", i).isEmpty();
    }
  }

  /**
   * Issue #9762, from {@code Issue9762TypeRenameReplicationIT}.
   * <p>
   * A peer that cannot decode the rename section must not be sent one, and there is no other form of the rename to send
   * it: the rename is refused before a single file moves, and goes through unchanged once the peer advertises it.
   */
  @Test
  void aRenameIsRefusedWhileAPeerCannotDecodeItAndNothingMoves() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database leader = leader();
    final String c = uniqueName("C");
    final String c2 = uniqueName("C2");
    leader.command("sql", "CREATE DOCUMENT TYPE " + c);
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument(c).set("id", i).save();
    });
    assertClusterConsistency();

    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();
    final Set<String> withoutRenames = new HashSet<>(PeerCapabilities.LOCAL);
    withoutRenames.remove(PeerCapabilities.SCHEMA_FILE_RENAMES);
    follower.setAdvertisedCapabilities(withoutRenames);
    try {
      awaitLeaderSees(leaderIndex, followerIndex, false);

      final Throwable refusal = catchThrowable(() -> leader.command("sql", "ALTER TYPE " + c + " NAME " + c2));
      assertThat(causeChainOf(refusal)).as("the rename is refused naming the capability: %s", refusal)
          .anyMatch(ClusterCapabilityNotReadyException.class::isInstance);

      // Nothing moved, on any node
      for (int i = 0; i < getServerCount(); i++) {
        final Database database = getServerDatabase(i, getDatabaseName());
        assertThat(database.getSchema().existsType(c)).as("server %d", i).isTrue();
        assertThat(database.getSchema().existsType(c2)).as("server %d", i).isFalse();
        assertThat(filesNamedAfter(database, c2)).as("server %d", i).isEmpty();
        assertThat(filesNamedAfter(database, c)).as("server %d", i).isNotEmpty();
      }
    } finally {
      follower.setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    }

    awaitLeaderSees(leaderIndex, followerIndex, true);
    leader.command("sql", "ALTER TYPE " + c + " NAME " + c2);
    assertRenamedOnEveryNode(c, c2, RECORDS);
  }

  // ------------------------------------------------------------------ database configuration (#9763)

  /** Issue #9763, from {@code Issue9763DatabaseConfigurationReplicationIT}. */
  @Test
  void alterDatabaseOnTheLeaderReachesEveryNode() throws Exception {
    final Object bucketsBefore = databaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    final Object dateImplementationBefore = databaseSetting(GlobalConfiguration.DATE_IMPLEMENTATION);
    try {
      final Database leader = leader();
      final String d = uniqueName("D");
      leader.command("sql", "CREATE DOCUMENT TYPE " + d);
      leader.command("sql", "CREATE PROPERTY " + d + ".day DATE");
      leader.transaction(() -> leader.newDocument(d).set("day", LocalDate.of(2026, 10, 13)).save());
      assertClusterConsistency();

      // java.util.Date, not the default java.time.LocalDate: a node that kept the default must read differently
      leader.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'");
      leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 5");

      assertOnEveryNode((index, database) -> {
        assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.DATE_IMPLEMENTATION))
            .as("server %d in memory", index).isEqualTo("java.util.Date");
        assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
            .as("server %d in memory", index).isEqualTo(5);
        assertThat(database.getSerializer().getDateImplementation())
            .as("server %d serializer", index).isEqualTo(Date.class);
        try (final ResultSet rs = database.query("sql", "SELECT day FROM " + d)) {
          assertThat(rs.next().<Object>getProperty("day"))
              .as("server %d answers a read with the replicated implementation", index).isInstanceOf(Date.class);
        }
      }, (index, file) -> {
        assertThat(file.getString("dateImplementation")).as("server %d configuration.json", index)
            .isEqualTo("java.util.Date");
        assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index).isEqualTo(5);
      });
    } finally {
      restoreDatabaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, bucketsBefore);
      restoreDatabaseSetting(GlobalConfiguration.DATE_IMPLEMENTATION, dateImplementationBefore);
    }
  }

  /**
   * Issue #9763, from {@code Issue9763DatabaseConfigurationReplicationIT}.
   * <p>
   * The path a user takes: the statement sent over HTTP to a FOLLOWER, which forwards it to the leader, where the
   * HTTP handler runs it inside a transaction of its own.
   */
  @Test
  void alterDatabaseSentOverHttpToAFollowerReachesEveryNode() throws Exception {
    final Object bucketsBefore = databaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    try {
      final int followerIndex = (findLeaderIndex() + 1) % getServerCount();

      assertThat(executeCommand(followerIndex, "sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 7")).isNotNull();

      assertOnEveryNode((index, database) -> assertThat(
              database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
              .as("server %d in memory", index).isEqualTo(7),
          (index, file) -> assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index)
              .isEqualTo(7));
    } finally {
      restoreDatabaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, bucketsBefore);
    }
  }

  /**
   * Issue #9763, from {@code Issue9763DatabaseConfigurationReplicationIT}.
   * <p>
   * The second writer: the "set database setting" server command (HTTP and gRPC), which does not forward and so runs
   * on whichever node the client reached - here a follower.
   */
  @Test
  void setDatabaseSettingOnAFollowerReachesEveryNode() throws Exception {
    final Object bucketsBefore = databaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    try {
      final int followerIndex = (findLeaderIndex() + 1) % getServerCount();

      new ServerControlPlane(getServer(followerIndex)).setDatabaseSetting(getDatabaseName(),
          GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey(), "9");

      assertOnEveryNode((index, database) -> assertThat(
              database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
              .as("server %d in memory", index).isEqualTo(9),
          (index, file) -> assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index)
              .isEqualTo(9));
    } finally {
      restoreDatabaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, bucketsBefore);
    }
  }

  /**
   * Issue #9763, from {@code Issue9763DatabaseConfigurationReplicationIT}.
   * <p>
   * Only the changed settings travel, so a change replaces nothing else on any node: a value one node holds in its own
   * {@code configuration.json} only (from before this replication existed) survives a change to another setting made
   * elsewhere, and so do changes to different settings made on different nodes.
   */
  @Test
  void aChangeOnOneNodeKeepsEverySettingItDoesNotName() throws Exception {
    final Object bucketsBefore = databaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    final Object retriesBefore = databaseSetting(GlobalConfiguration.TX_RETRIES);
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final LocalDatabase followerLocal = (LocalDatabase) ((DatabaseInternal) getServerDatabase(followerIndex,
        getDatabaseName())).getEmbedded();
    try {
      // Written past the replication, as a node upgraded from a build without it can hold
      followerLocal.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
      followerLocal.saveConfiguration();

      leader().command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 6");
      new ServerControlPlane(getServer(followerIndex)).setDatabaseSetting(getDatabaseName(),
          GlobalConfiguration.TX_RETRIES.getKey(), "7");

      assertOnEveryNode((index, database) -> {
        assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
            .as("server %d in memory", index).isEqualTo(6);
        assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES))
            .as("server %d in memory", index).isEqualTo(7);
      }, (index, file) -> {
        assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index).isEqualTo(6);
        assertThat(file.getInt("txRetries")).as("server %d configuration.json", index).isEqualTo(7);
      });

      final DatabaseInternal follower = (DatabaseInternal) getServerDatabase(followerIndex, getDatabaseName());
      assertThat(follower.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH))
          .as("the follower's own value is not replaced by changes to other settings").isEqualTo(2);
      assertThat(readConfigurationFile(follower).getInt("txWalFlush")).isEqualTo(2);
    } finally {
      // The follower's own value goes the way it came, past the replication: aChangeDoesNotCarryAnotherPendingSettingOfItsNode
      // asserts that no node's file holds it
      followerLocal.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, null);
      followerLocal.saveConfiguration();
      restoreDatabaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, bucketsBefore);
      restoreDatabaseSetting(GlobalConfiguration.TX_RETRIES, retriesBefore);
    }
  }

  /**
   * Issue #9763, from {@code Issue9763DatabaseConfigurationReplicationIT}.
   * <p>
   * A peer that cannot decode the new entry type would halt on it, so the change is refused before anything is
   * submitted - and the node that ran it must not keep the setting in force on its own, which is the divergence the
   * replication exists to end.
   */
  @Test
  void aChangeIsRefusedAndRolledBackWhileAPeerCannotDecodeTheEntry() throws Exception {
    final Object bucketsSettingBefore = databaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    try {
      final int leaderIndex = findLeaderIndex();
      final int laggingIndex = (leaderIndex + 1) % getServerCount();
      final String laggingPeerId = peerIdForIndex(laggingIndex);
      final RaftHAServer lagging = getRaftPlugin(laggingIndex).getRaftHAServer();
      final Database leader = leader();
      final DatabaseInternal leaderDb = (DatabaseInternal) leader;
      final int bucketsBefore = leaderDb.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
      final Class<?> dateImplementationBefore = leaderDb.getSerializer().getDateImplementation();
      assertThat(dateImplementationBefore).as("precondition: the change below must be a change").isNotEqualTo(Date.class);

      lagging.setAdvertisedCapabilities(Set.of(PeerCapabilities.SCHEMA_DELTA));
      try {
        awaitObserved(leaderIndex, laggingPeerId,
            capabilities -> capabilities != null && !capabilities.contains(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY));

        assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 11"))
            .hasMessageContaining(laggingPeerId)
            .hasMessageContaining(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);
        assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
            .hasMessageContaining(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);

        assertThat(leaderDb.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
            .as("the refused setting must not stay in force on the leader alone").isEqualTo(bucketsBefore);
        assertThat(leaderDb.getSerializer().getDateImplementation())
            .as("nor its per-database side effect").isEqualTo(dateImplementationBefore);
        assertThat(getServer(laggingIndex).isStarted()).as("nothing was submitted, so nothing halted").isTrue();
      } finally {
        lagging.setAdvertisedCapabilities(PeerCapabilities.LOCAL);
      }

      awaitObserved(leaderIndex, laggingPeerId,
          capabilities -> capabilities != null && capabilities.contains(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY));
      leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 11");
      assertOnEveryNode((index, database) -> assertThat(
              database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
              .as("server %d once the cluster is uniform", index).isEqualTo(11),
          (index, file) -> assertThat(file.getInt("typeDefaultBuckets")).isEqualTo(11));
    } finally {
      restoreDatabaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, bucketsSettingBefore);
    }
  }

  /**
   * Issue #9763, from {@code Issue9763DatabaseConfigurationReplicationIT}.
   * <p>
   * Issue #9959: the entry a change submits names the setting it changed and nothing else. A value another statement on
   * the same node has put in force and not submitted yet - its save still ahead of it, and possibly refused - used to
   * travel with this change's entry and commit on every node. Here that other statement is the in-memory store each
   * writer makes before its save, left pending on the node that then runs a change to a different setting: through
   * {@code ALTER DATABASE} on the leader and through the "set database setting" command on a follower.
   */
  @Test
  void aChangeDoesNotCarryAnotherPendingSettingOfItsNode() throws Exception {
    final Object bucketsBefore = databaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    final Object retriesBefore = databaseSetting(GlobalConfiguration.TX_RETRIES);
    try {
      final int leaderIndex = findLeaderIndex();
      final int followerIndex = (leaderIndex + 1) % getServerCount();
      final String walFlushKey = GlobalConfiguration.TX_WAL_FLUSH.getKey();
      final LocalDatabase leaderLocal = (LocalDatabase) ((DatabaseInternal) leader()).getEmbedded();
      final LocalDatabase followerLocal = (LocalDatabase) ((DatabaseInternal) getServerDatabase(followerIndex,
          getDatabaseName())).getEmbedded();
      final int[] walFlushBefore = new int[getServerCount()];
      for (int i = 0; i < getServerCount(); i++) {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
        walFlushBefore[i] = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH);
        assertThat(readConfigurationFile(database).has("txWalFlush")).as("precondition: server %d", i).isFalse();
      }

      // The other statements: in force on their node, not submitted
      leaderLocal.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
      followerLocal.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
      try {
        leader().command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
        new ServerControlPlane(getServer(followerIndex)).setDatabaseSetting(getDatabaseName(),
            GlobalConfiguration.TX_RETRIES.getKey(), "8");

        assertOnEveryNode((index, database) -> {
          assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
              .as("server %d in memory", index).isEqualTo(4);
          assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES))
              .as("server %d in memory", index).isEqualTo(8);
          final int expectedWalFlush = index == leaderIndex ? 2 : index == followerIndex ? 1 : walFlushBefore[index];
          assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH))
              .as("server %d runs with its own pending value, or none, never another node's", index)
              .isEqualTo(expectedWalFlush);
        }, (index, file) -> {
          assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index).isEqualTo(4);
          assertThat(file.getInt("txRetries")).as("server %d configuration.json", index).isEqualTo(8);
          assertThat(file.has("txWalFlush")).as("server %d: no entry carried the pending setting", index).isFalse();
        });
      } finally {
        try {
          leaderLocal.getConfiguration().setValue(walFlushKey, null);
        } finally {
          followerLocal.getConfiguration().setValue(walFlushKey, null);
        }
      }
    } finally {
      restoreDatabaseSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, bucketsBefore);
      restoreDatabaseSetting(GlobalConfiguration.TX_RETRIES, retriesBefore);
    }
  }

  // ------------------------------------------------------------------ JSONL import of schema members (#9764)

  /**
   * Issue #9764, from {@code Issue9764JsonlImportSchemaMembersReplicationIT}.
   * <p>
   * The import runs on the leader against the replicated database, the way {@code IMPORT DATABASE} drives it. Every node
   * is then read back twice: its in-memory schema (what a query there runs against) and its own {@code schema.json}
   * (what it reopens with).
   */
  @Test
  void importedTriggerViewAndFunctionReachEveryNode() throws Exception {
    final String account = uniqueName("Account");
    final String accountAudit = uniqueName("AccountAudit");
    final String audit = uniqueName("audit");
    final String activeAccounts = uniqueName("ActiveAccounts");
    final String acct = uniqueName("acct");
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE " + account);
      source.command("sql", "CREATE PROPERTY " + account + ".name STRING");
      source.command("sql", "CREATE PROPERTY " + account + ".active BOOLEAN");
      source.command("sql", "CREATE DOCUMENT TYPE " + accountAudit);
      source.command("sql", "CREATE PROPERTY " + accountAudit + ".what STRING");
      source.transaction(() -> {
        source.command("sql", "INSERT INTO " + account + " SET name = 'a', active = true");
        source.command("sql", "INSERT INTO " + account + " SET name = 'b', active = false");
      });
      source.command("sql",
          "CREATE TRIGGER " + audit + " AFTER CREATE ON TYPE " + account + " EXECUTE SQL \"INSERT INTO " + accountAudit
              + " SET what = 'account'\"");
      source.command("sql",
          "CREATE MATERIALIZED VIEW " + activeAccounts + " AS SELECT name FROM " + account + " WHERE active = true");
      // No continuous aggregate here: creating one under HA makes the followers fail their next apply to the source
      // timeseries type and resync, import or not (#9886). The import restores it through the same call as the rest,
      // and Issue7886JsonlSchemaMembersRoundTripIT asserts it lands in schema.json.
      source.command("sql", "DEFINE FUNCTION " + acct + ".twice \"SELECT :a * 2 AS result\" PARAMETERS [a] LANGUAGE sql");
    }

    new Exporter(("-f " + EXPORT_FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    new Importer(leader, "file://" + new File(EXPORT_FILE).getAbsolutePath()).load();

    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      final Schema schema = database.getSchema();
      final String node = "server " + i + (i == leaderIndex ? " (leader)" : " (follower)");

      assertThat(schema.existsType(account)).as("%s has the imported type", node).isTrue();
      assertThat(database.countType(account, false)).as("%s has the imported records", node).isEqualTo(2);

      assertThat(schema.existsTrigger(audit)).as("%s has the imported trigger in memory", node).isTrue();
      assertThat(schema.getTrigger(audit).getTypeName()).as("%s trigger type", node).isEqualTo(account);
      assertThat(schema.existsMaterializedView(activeAccounts)).as("%s has the imported view in memory", node).isTrue();
      assertThat(schema.hasFunctionLibrary(acct)).as("%s has the imported function library in memory", node).isTrue();
      assertThat(schema.getFunctionLibrary(acct).getFunction("twice")).as("%s has the imported function", node)
          .isNotNull();

      final JSONObject onDisk = new JSONObject(
          Files.readString(new File(database.getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8));
      assertThat(onDisk.getJSONObject("triggers", new JSONObject()).has(audit))
          .as("%s schema.json names the imported trigger", node).isTrue();
      assertThat(onDisk.getJSONObject("materializedViews", new JSONObject()).has(activeAccounts))
          .as("%s schema.json names the imported view", node).isTrue();
      assertThat(onDisk.getJSONObject("functions", new JSONObject()).has(acct))
          .as("%s schema.json names the imported function library", node).isTrue();
    }

    // The restored trigger is live on the leader, and what it writes replicates like any other record
    leader.transaction(() -> leader.command("sql", "INSERT INTO " + account + " SET name = 'c', active = true"));
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(accountAudit, false))
          .as("server %d sees the row the imported trigger wrote", i).isEqualTo(1);
  }

  // ------------------------------------------------------------------ replica writes during leader DDL (#7438)

  /** Issue #7438, from {@code Issue7438ReplicaWritesDuringLeaderDdlIT}. */
  @Test
  @Tag("slow")
  void replicaInsertsDuringLeaderCreateIndexAreAllIndexed() throws Exception {
    final String typeName = uniqueName("Issue7438Doc");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      final DocumentType type = leaderDb.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
      type.getOrCreateProperty("id", Type.LONG);
      type.getOrCreateProperty("tag", Type.STRING);
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < SEED; i++)
        leaderDb.newDocument(typeName).set("id", (long) i).set("tag", "seed").save();
    });
    waitForReplicationIsCompleted(replicaIndex);

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicInteger acknowledged = new AtomicInteger();
    final AtomicInteger refused = new AtomicInteger();
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      final Future<?> writer = pool.submit(() -> {
        int next = SEED;
        while (!stop.get()) {
          final long id = next;
          try {
            replicaDb.transaction(() -> replicaDb.newDocument(typeName).set("id", id).set("tag", "replica").save(), false, 0);
            acknowledged.incrementAndGet();
            next++;
          } catch (final NeedRetryException e) {
            // Refused while the leader runs its DDL: the same id is retried, as any caller would.
            refused.incrementAndGet();
          }
        }
      });

      // DDLs (property plus index) back to back until the replica's writer has been refused at least once: a build that finishes without ever
      // overlapping a replica write proves nothing, so the loop keeps opening windows (bounded) rather than assume one.
      final long ddlDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
      int rounds = 0;
      // A fresh property and index per round, never a drop: dropping an index under a transaction the replica has in flight
      // closes its file beneath that transaction, a different matter from the window under test.
      while (refused.get() == 0 && System.nanoTime() < ddlDeadline) {
        final String property = "extra" + rounds;
        leaderDb.getSchema().getType(typeName).createProperty(property, Type.LONG);
        leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, typeName, property);
        rounds++;
      }
      assertThat(refused.get()).as("replica inserts refused during %d leader DDL round(s)", rounds).isGreaterThan(0);
      // The indexes that stay, so the final comparison has something to compare.
      leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, typeName, "id");
      leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, typeName, "tag");
      leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, typeName, "id", "tag");

      // The window must have let inserts through as well as refused them, or the run proves nothing.
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (acknowledged.get() < MIN_ACKNOWLEDGED && System.nanoTime() < deadline)
        Thread.sleep(50);
      stop.set(true);
      writer.get(120, TimeUnit.SECONDS);
      assertThat(acknowledged.get()).as("inserts acknowledged to the replica's writer").isGreaterThanOrEqualTo(MIN_ACKNOWLEDGED);
    } finally {
      stop.set(true);
      pool.shutdownNow();
    }

    waitForReplicationIsCompleted(replicaIndex);
    final long expected = SEED + acknowledged.get();
    // Every node holds the same records and, in every index, the same entries: two entries claiming one page version
    // spliced on apply, or a replica insert landing while the index is built, used to leave the nodes disagreeing.
    // (An insert prepared by the replica before it applied the new index is a separate matter, see #8686.)
    final Database leaderCheck = getServerDatabase(leaderIndex, getDatabaseName());
    final Database replicaCheck = getServerDatabase(replicaIndex, getDatabaseName());
    awaitValue(expected, () -> replicaCheck.countType(typeName, true));
    assertThat(leaderCheck.countType(typeName, true)).as("records on the leader (refused %d times)", refused.get())
        .isEqualTo(expected);
    assertThat(replicaCheck.countType(typeName, true)).as("records on the replica").isEqualTo(expected);
    for (final Index leaderIndexDef : leaderCheck.getSchema().getType(typeName).getAllIndexes(true)) {
      final Index replicaIndexDef = replicaCheck.getSchema().getIndexByName(leaderIndexDef.getName());
      assertThat(replicaIndexDef.countEntries()).as("index %s: replica vs leader", leaderIndexDef.getName())
          .isEqualTo(leaderIndexDef.countEntries());
    }
    assertClusterConsistency();
  }

  // ------------------------------------------------------------------ helpers

  @FunctionalInterface
  private interface NodeCheck<T> {
    void accept(int serverIndex, T value) throws Exception;
  }

  private Database leader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  /**
   * #9747: waits for every node to apply what the leader committed, then checks each node's in-memory schema and its own
   * {@code schema.json}.
   */
  private void assertOnEveryNode(final Consumer<Schema> inMemory, final Consumer<JSONObject> onDisk) throws IOException {
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      try {
        inMemory.accept(database.getSchema());
      } catch (final AssertionError e) {
        throw new AssertionError("server " + i + " in memory: " + e.getMessage(), e);
      }
      final File schemaFile = new File(database.getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      try {
        onDisk.accept(root);
      } catch (final AssertionError e) {
        throw new AssertionError("server " + i + " schema.json: " + e.getMessage(), e);
      }
    }
  }

  /** #9763: waits for every node to apply what the leader committed, then checks each node in memory and on disk. */
  private void assertOnEveryNode(final NodeCheck<DatabaseInternal> inMemory, final NodeCheck<JSONObject> onDisk)
      throws Exception {
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
      inMemory.accept(i, database);
      onDisk.accept(i, readConfigurationFile(database));
    }
  }

  private static JSONObject readConfigurationFile(final DatabaseInternal database) throws IOException {
    final File file = new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
    return new JSONObject(Files.readString(file.toPath(), StandardCharsets.UTF_8)).getJSONObject("configuration");
  }

  /** The value of a replicated database setting as the leader holds it, for {@link #restoreDatabaseSetting}. */
  private Object databaseSetting(final GlobalConfiguration setting) {
    return ((DatabaseInternal) leader()).getConfiguration().getValue(setting);
  }

  /**
   * Puts a replicated database setting a test changed back to the value it had before the test, through
   * {@code ALTER DATABASE} on the leader, so the next test of the shared cluster starts from the settings the cluster
   * started with. Nothing is submitted when the leader already holds that value. A failing restore is logged, never
   * thrown, so it cannot mask the test's own failure.
   */
  private void restoreDatabaseSetting(final GlobalConfiguration setting, final Object before) {
    try {
      final Database leader = leader();
      if (Objects.equals(((DatabaseInternal) leader).getConfiguration().getValue(setting), before))
        return;
      final String value = before instanceof Number ?
          before.toString() :
          "'" + (before instanceof Class<?> c ? c.getName() : String.valueOf(before)) + "'";
      leader.command("sql", "ALTER DATABASE `" + setting.getKey() + "` " + value);
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING, "TEST: could not restore database setting %s", e, setting.getKey());
    }
  }

  /** #9763: waits until the leader has PROBED the peer and recorded an advertisement satisfying {@code settled}. */
  private void awaitObserved(final int observerIndex, final String peerId, final Predicate<Set<String>> settled)
      throws InterruptedException {
    final RaftHAServer observer = getRaftPlugin(observerIndex).getRaftHAServer();
    final long deadline = System.currentTimeMillis() + CAPABILITY_CONVERGENCE_TIMEOUT_MS;
    Set<String> observed = observedCapabilities(observer, peerId);
    while (!settled.test(observed) && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
      observed = observedCapabilities(observer, peerId);
    }
    // A hang detector, not a latency assertion: the refresh runs every few seconds
    assertThat(settled.test(observed)).as("advertisement of %s seen by server %d: %s", peerId, observerIndex, observed)
        .isTrue();
  }

  private static Set<String> observedCapabilities(final RaftHAServer observer, final String peerId) {
    final PeerCapabilityRegistry.Advertisement advertisement =
        observer.getPeerCapabilityRegistry().freshAdvertisementOf(peerId);
    return advertisement != null ? advertisement.capabilities() : null;
  }

  /** #9762. */
  private void assertRenamedOnEveryNode(final String oldName, final String newName, final long records)
      throws IOException {
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      final Schema schema = database.getSchema();
      assertThat(schema.existsType(newName)).as("server %d knows type %s", i, newName).isTrue();
      assertThat(schema.existsType(oldName)).as("server %d still knows type %s", i, oldName).isFalse();
      assertThat(database.countType(newName, false)).as("server %d records", i).isEqualTo(records);
      for (final var bucket : schema.getType(newName).getBuckets(false))
        assertThat(bucket.getName()).as("server %d bucket", i).startsWith(newName + "_");

      final JSONObject file = new JSONObject(
          Files.readString(new File(database.getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8));
      assertThat(file.getJSONObject("types").has(newName)).as("server %d schema.json", i).isTrue();
      assertThat(file.getJSONObject("types").has(oldName)).as("server %d schema.json", i).isFalse();

      assertThat(filesNamedAfter(database, newName)).as("server %d files of %s", i, newName).isNotEmpty();
      assertThat(filesNamedAfter(database, oldName)).as("server %d files of %s", i, oldName).isEmpty();

      if (!schema.getType(newName).getAllIndexes(false).isEmpty())
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + newName + " WHERE id = 7")) {
          assertThat(rs.hasNext()).as("server %d index lookup", i).isTrue();
        }
    }
  }

  /**
   * #9762: the component files on disk whose name is the type's name followed by its separator. With the names
   * {@link #uniqueName(String)} gives ({@code R_3}), the separator also keeps {@code R_3} from matching {@code R_31}.
   */
  private static List<String> filesNamedAfter(final Database database, final String typeName) {
    final List<String> names = new ArrayList<>();
    final File[] files = new File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final File f : files)
        if (f.getName().startsWith(typeName + "_"))
          names.add(f.getName());
    return names;
  }

  /** #9762. */
  private void awaitLeaderSees(final int leaderIndex, final int followerIndex, final boolean capable)
      throws InterruptedException {
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final String followerPeerId = peerIdForIndex(followerIndex);
    final long deadline = System.currentTimeMillis() + 60_000L;
    while (System.currentTimeMillis() < deadline) {
      final PeerCapabilityRegistry.Advertisement advertisement =
          leaderRaft.getPeerCapabilityRegistry().freshAdvertisementOf(followerPeerId);
      if (advertisement != null && advertisement.capabilities().contains(PeerCapabilities.SCHEMA_FILE_RENAMES) == capable)
        return;
      Thread.sleep(200);
    }
    throw new AssertionError("the leader never observed peer " + followerPeerId + " advertising "
        + (capable ? "" : "no ") + PeerCapabilities.SCHEMA_FILE_RENAMES);
  }

  private static List<Throwable> causeChainOf(final Throwable t) {
    final List<Throwable> chain = new ArrayList<>();
    for (Throwable c = t; c != null && !chain.contains(c); c = c.getCause())
      chain.add(c);
    return chain;
  }
}
