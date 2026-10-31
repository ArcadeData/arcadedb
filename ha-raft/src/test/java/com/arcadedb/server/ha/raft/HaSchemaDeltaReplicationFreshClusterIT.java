/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.function.ToLongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6989 on a cluster of its own, sibling of {@link HaSchemaDeltaReplicationIT} with the same fixture (two servers,
 * {@code majority} quorum, {@code arcadedb.ha.schemaDelta} on) but a fresh cluster per test ({@link BaseRaftHATest}).
 * With the delta on, a DDL against a large schema must reach the follower through a Raft entry proportional to the
 * change rather than to the schema. The test builds a schema big enough that the difference matters, then runs a
 * series of single-property DDLs and checks that the leader actually took the delta path (it falls back to the
 * document by itself whenever the cached base is not what the followers hold, so a green functional assertion alone
 * would pass even if no delta was ever shipped) and that the follower ends up with exactly the schema the leader has.
 * <p>
 * Why it cannot share the cluster: its seed must be the FIRST schema the cluster ever ships. It asserts that the seed
 * went out as a whole document through a cumulative counter, which on a cluster where an earlier test already shipped
 * one is above zero before the seed runs (and the seed itself would then go out as a delta against that test's base),
 * so the assertion would pass whether or not the seed reached the shipping path. #6989's drop test, which compares a
 * counter only against a value it reads itself, runs on the shared cluster.
 * <p>
 * The source class was tagged {@code slow}.
 */
@Tag("slow")
class HaSchemaDeltaReplicationFreshClusterIT extends BaseRaftHATest {

  private static final int SEED_TYPES  = 150;
  private static final int DDL_CHANGES = 12;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_SCHEMA_DELTA, true);
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  /** Issue #6989, from {@code Issue6989SchemaDeltaReplicationIT}. */
  @Test
  void singlePropertyDdlShipsADeltaAndTheFollowerStaysIdentical() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    requireAPristineDatabase(leaderDb, "DeltaSeed_0");

    // A schema worth having a delta for: the point of the fix is the ratio between one type and all of them.
    leaderDb.transaction(() -> {
      final Schema schema = leaderDb.getSchema();
      for (int i = 0; i < SEED_TYPES; i++) {
        final DocumentType type = schema.createVertexType("DeltaSeed_" + i);
        type.createProperty("id_" + i, Type.STRING);
        type.createProperty("name_" + i, Type.STRING);
        type.createProperty("payload_" + i, Type.STRING);
      }
    });

    // The seed cannot be a delta - there is no base yet - so it primes the base every later DDL diffs against.
    assertThat(schemaDocumentsShipped())
        .as("the seed must have shipped as a whole document, or the shipping path was never reached at all")
        .isGreaterThan(0);

    final long deltasBefore = schemaDeltasShipped();

    // Each of these is the case from the issue: a few dozen bytes of logical schema.
    for (int i = 0; i < DDL_CHANGES; i++) {
      final int typeIndex = i;
      leaderDb.transaction(() -> leaderDb.getSchema().getType("DeltaSeed_" + typeIndex)
          .createProperty("added_" + typeIndex, Type.INTEGER));
    }

    assertThat(schemaDeltasShipped() - deltasBefore)
        .as("every DDL after the first must have gone out as a delta")
        .isGreaterThanOrEqualTo(DDL_CHANGES - 1);

    assertClusterConsistency();

    final Schema replicaSchema = getServerDatabase(replicaIndex, getDatabaseName()).getSchema();
    for (int i = 0; i < SEED_TYPES; i++) {
      final String typeName = "DeltaSeed_" + i;
      assertThat(replicaSchema.existsType(typeName)).as("replica has %s", typeName).isTrue();
      assertThat(replicaSchema.getType(typeName).existsProperty("payload_" + i)).isTrue();
    }
    for (int i = 0; i < DDL_CHANGES; i++)
      assertThat(replicaSchema.getType("DeltaSeed_" + i).existsProperty("added_" + i))
          .as("the property added by delta %d reached the replica", i)
          .isTrue();

    assertThat(replicaSchema.getEmbedded().toJSON().getJSONObject("types").keySet())
        .as("the replica's type set is exactly the leader's")
        .isEqualTo(leaderDb.getSchema().getEmbedded().toJSON().getJSONObject("types").keySet());
  }

  /**
   * The seed above assumes it is building the schema from nothing, and the whole test rests on that: the delta
   * counters are read against a base this method primes. {@code BaseGraphServerTest} points the servers at
   * {@code ./target/databasesN}, which the build's stale-database clean does NOT match (its fileset is
   * {@code target/databases}, without the index) - so a run killed part-way leaves them behind and the seed
   * fails with a bare "Cannot create type ... because already exists" that names neither the cause nor the fix.
   * Say both here instead.
   */
  private void requireAPristineDatabase(final Database leaderDb, final String firstSeededType) {
    assertThat(leaderDb.getSchema().existsType(firstSeededType))
        .as("this test seeds its own schema and needs a pristine database; remove the leftover "
            + "ha-raft/target/databases* directories an aborted run left behind")
        .isFalse();
  }

  /** The leader can move between tests, so ask every node rather than guessing which one shipped. */
  private long schemaDeltasShipped() {
    return sumOverNodes(RaftReplicatedDatabase::getSchemaDeltasShipped);
  }

  private long schemaDocumentsShipped() {
    return sumOverNodes(RaftReplicatedDatabase::getSchemaDocumentsShipped);
  }

  private long sumOverNodes(final ToLongFunction<RaftReplicatedDatabase> counter) {
    long total = 0;
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal wrapped =
          ((DatabaseInternal) getServerDatabase(i, getDatabaseName())).getWrappedDatabaseInstance();
      if (wrapped instanceof final RaftReplicatedDatabase raft)
        total += counter.applyAsLong(raft);
    }
    return total;
  }
}
