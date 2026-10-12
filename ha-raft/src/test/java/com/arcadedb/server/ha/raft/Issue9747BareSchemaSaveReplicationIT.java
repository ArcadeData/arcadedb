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

import com.arcadedb.database.Database;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.ZoneId;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9747: a schema change that ended in a bare {@code LocalSchema.saveConfiguration()} - outside a schema recording
 * session - rewrote the leader's {@code schema.json} and shipped nothing, so the followers kept the old schema until an
 * unrelated DDL happened to ship the whole document. A constraint enforced on the leader was then not enforced on a node
 * that became leader after a failover, and database settings answered differently per node.
 * <p>
 * One test per writer family. Each change is made on the leader, then every node is read back twice: its in-memory
 * schema (what a query on that node enforces) and its own {@code schema.json} (what that node reopens with).
 */
class Issue9747BareSchemaSaveReplicationIT extends BaseRaftHATest {

  @Test
  void alterPropertyConstraintsViaSqlReachEveryNode() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE P");
    leader.command("sql", "CREATE PROPERTY P.x INTEGER");
    leader.command("sql", "CREATE PROPERTY P.s STRING");
    assertClusterConsistency();

    leader.command("sql", "ALTER PROPERTY P.x MANDATORY true");
    leader.command("sql", "ALTER PROPERTY P.x NOTNULL true");
    leader.command("sql", "ALTER PROPERTY P.x READONLY true");
    leader.command("sql", "ALTER PROPERTY P.x MIN 1");
    leader.command("sql", "ALTER PROPERTY P.x MAX 10");
    leader.command("sql", "ALTER PROPERTY P.s REGEXP '[a-z]+'");
    leader.command("sql", "ALTER PROPERTY P.s CUSTOM label = 'probe'");

    assertOnEveryNode(schema -> {
      final Property x = schema.getType("P").getProperty("x");
      assertThat(x.isMandatory()).isTrue();
      assertThat(x.isNotNull()).isTrue();
      assertThat(x.isReadonly()).isTrue();
      assertThat(x.getMin()).isEqualTo("1");
      assertThat(x.getMax()).isEqualTo("10");
      final Property s = schema.getType("P").getProperty("s");
      assertThat(s.getRegexp()).isEqualTo("[a-z]+");
      assertThat(s.getCustomValue("label")).isEqualTo("probe");
    }, file -> {
      final JSONObject x = file.getJSONObject("types").getJSONObject("P").getJSONObject("properties").getJSONObject("x");
      assertThat(x.getBoolean("mandatory", false)).isTrue();
      assertThat(x.getBoolean("readonly", false)).isTrue();
    });
  }

  @Test
  void propertySettersViaJavaReachEveryNode() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE J");
    leader.command("sql", "CREATE PROPERTY J.x INTEGER");
    leader.command("sql", "CREATE PROPERTY J.s STRING");
    leader.command("sql", "CREATE PROPERTY J.l LIST");
    assertClusterConsistency();

    final Property x = leader.getSchema().getType("J").getProperty("x");
    x.setReadonly(true);
    x.setMandatory(true);
    x.setNotNull(true);
    x.setMin("2");
    x.setMax("20");
    final Property s = leader.getSchema().getType("J").getProperty("s");
    s.setRegexp("[0-9]+");
    s.setHidden(true);
    s.setCompression("fast");
    s.setCustomValue("k", "v");
    leader.getSchema().getType("J").getProperty("l").setOfType("STRING");

    assertOnEveryNode(schema -> {
      final Property nx = schema.getType("J").getProperty("x");
      assertThat(nx.isReadonly()).isTrue();
      assertThat(nx.isMandatory()).isTrue();
      assertThat(nx.isNotNull()).isTrue();
      assertThat(nx.getMin()).isEqualTo("2");
      assertThat(nx.getMax()).isEqualTo("20");
      final Property ns = schema.getType("J").getProperty("s");
      assertThat(ns.getRegexp()).isEqualTo("[0-9]+");
      assertThat(ns.isHidden()).isTrue();
      assertThat(ns.getCompression()).isEqualTo("fast");
      assertThat(ns.getCustomValue("k")).isEqualTo("v");
      assertThat(schema.getType("J").getProperty("l").getOfType()).isEqualTo("STRING");
    }, file -> {
      final JSONObject nx = file.getJSONObject("types").getJSONObject("J").getJSONObject("properties").getJSONObject("x");
      assertThat(nx.getBoolean("readonly", false)).isTrue();
      assertThat(nx.getBoolean("mandatory", false)).isTrue();
    });
  }

  /**
   * The constraint has to be ENFORCED on a follower's copy, not just readable: the follower is the next leader after a
   * failover, and a record it accepts there violates what the leader refused.
   */
  @Test
  void aMandatoryPropertyIsEnforcedOnTheFollowerSchema() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE M");
    leader.command("sql", "CREATE PROPERTY M.x INTEGER");
    assertClusterConsistency();

    leader.getSchema().getType("M").getProperty("x").setMandatory(true);
    // An ordinary record commit afterwards: the issue saw this carry nothing across either
    leader.transaction(() -> leader.newDocument("M").set("x", 1).save());
    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().getType("M").getProperty("x").isMandatory())
          .as("server %d", i).isTrue();
  }

  /**
   * The path a user takes: the statement sent over HTTP to a FOLLOWER, which forwards it to the leader, where the HTTP
   * handler runs it inside a transaction of its own - so the schema save is postponed to the end of that transaction.
   */
  @Test
  void alterPropertySentOverHttpToAFollowerReachesEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leader().command("sql", "CREATE DOCUMENT TYPE H");
    leader().command("sql", "CREATE PROPERTY H.x INTEGER");
    assertClusterConsistency();

    assertThat(executeCommand(followerIndex, "sql", "ALTER PROPERTY H.x MANDATORY true")).isNotNull();
    assertThat(executeCommand(followerIndex, "sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy/MM/dd'")).isNotNull();

    assertOnEveryNode(schema -> {
      assertThat(schema.getType("H").getProperty("x").isMandatory()).isTrue();
      assertThat(schema.getDateFormat()).isEqualTo("yyyy/MM/dd");
    }, file -> assertThat(file.getJSONObject("types").getJSONObject("H").getJSONObject("properties").getJSONObject("x")
        .getBoolean("mandatory", false)).isTrue());
  }

  @Test
  void schemaExtensionReachesEveryNode() throws Exception {
    final Database leader = leader();
    leader.getSchema().setExtension("probe", new JSONObject().put("a", 1));

    assertOnEveryNode(schema -> assertThat(schema.getExtension("probe").getInt("a")).isEqualTo(1),
        file -> assertThat(file.getJSONObject("extensions").getJSONObject("probe").getInt("a")).isEqualTo(1));

    leader.getSchema().setExtension("probe", null);
    assertOnEveryNode(schema -> assertThat(schema.getExtension("probe")).isNull(),
        file -> assertThat(file.getJSONObject("extensions", new JSONObject()).has("probe")).isFalse());
  }

  @Test
  void alterDatabaseSchemaSettingsReachEveryNode() throws Exception {
    final Database leader = leader();
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
  }

  @Test
  void edgeTypeLightweightAndUniqueReachEveryNode() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE VERTEX TYPE EV");
    leader.command("sql", "CREATE EDGE TYPE EL");
    leader.command("sql", "CREATE EDGE TYPE EU");
    assertClusterConsistency();

    // Lightweight and unique: no index is built for a lightweight type, so nothing but the flags changes
    leader.command("sql", "ALTER TYPE EL WITH lightweight = true");
    leader.command("sql", "ALTER TYPE EL WITH unique = true");
    leader.command("sql", "ALTER TYPE EU WITH unique = true");

    assertOnEveryNode(schema -> {
      final LocalEdgeType el = (LocalEdgeType) schema.getType("EL");
      assertThat(el.isLightweight()).isTrue();
      assertThat(el.isUnique()).isTrue();
      assertThat(((LocalEdgeType) schema.getType("EU")).isUnique()).isTrue();
    }, file -> {
    });
  }

  @Test
  void timeSeriesDownsamplingPolicyReachesEveryNode() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE TIMESERIES TYPE TS TIMESTAMP ts FIELDS (v DOUBLE)");
    assertClusterConsistency();

    leader.command("sql", "ALTER TIMESERIES TYPE TS ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS");

    assertOnEveryNode(schema -> assertThat(((LocalTimeSeriesType) schema.getType("TS")).getDownsamplingTiers()).hasSize(1),
        file -> {
        });
  }

  @Test
  void bucketSelectionStrategyReachesEveryNode() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE B BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY B.tenant STRING");
    leader.command("sql", "CREATE INDEX ON B (tenant) UNIQUE");
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE B BucketSelectionStrategy `partitioned('tenant')`");

    assertOnEveryNode(schema -> assertThat(schema.getType("B").getBucketSelectionStrategy().getName()).isEqualTo("partitioned"),
        file -> assertThat(file.getJSONObject("types").getJSONObject("B").getJSONObject("bucketSelectionStrategy").getString("name")).isEqualTo(
            "partitioned"));
  }

  /**
   * The needsRepartition flag: set by a strategy change on a populated type, cleared by REBUILD TYPE. A follower that kept
   * it set would decline to prune the type's queries after a failover; one that never got it would prune over records
   * the old strategy placed.
   */
  @Test
  void needsRepartitionFlagReachesEveryNode() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE NR BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY NR.tenant STRING");
    leader.command("sql", "CREATE INDEX ON NR (tenant) UNIQUE");
    leader.transaction(() -> {
      for (int i = 0; i < 10; i++)
        leader.newDocument("NR").set("tenant", "t" + i).save();
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE NR BucketSelectionStrategy `partitioned('tenant')`");
    assertOnEveryNode(schema -> assertThat(schema.getType("NR").isNeedsRepartition()).isTrue(), file -> {
    });

    leader.command("sql", "REBUILD TYPE NR WITH repartition = true");
    assertOnEveryNode(schema -> assertThat(schema.getType("NR").isNeedsRepartition()).isFalse(), file -> {
    });
  }

  private Database leader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  /**
   * Waits for every node to apply what the leader committed, then checks each node's in-memory schema and its own
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
}
