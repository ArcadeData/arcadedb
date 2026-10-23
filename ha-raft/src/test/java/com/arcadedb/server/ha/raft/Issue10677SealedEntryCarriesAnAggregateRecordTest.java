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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.arcadedb.server.ha.raft.ArcadeStateMachine.carriesAContinuousAggregateRecordNotHeldHere;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10677: an entry that installs sealed TimeSeries bytes and creates no file is a compaction's, which changes no schema, unless
 * the schema it carries records a range of invalidated samples on a continuous aggregate that the follower does not hold - the
 * record a delete of sealed samples writes in the same replication unit (issue #10428). This is what tells the two apart.
 */
class Issue10677SealedEntryCarriesAnAggregateRecordTest {
  private static final String ROOT = "./target/databases/issue-10677-";

  private DatabaseInternal leader;
  private DatabaseInternal follower;

  @BeforeEach
  void setUp() {
    leader = create("leader");
    follower = create("follower");
  }

  @AfterEach
  void tearDown() {
    for (final DatabaseInternal db : new DatabaseInternal[] { leader, follower })
      if (db != null && db.isOpen())
        db.drop();
  }

  @Test
  void aSchemaWithNoRecordIsACompactions() {
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isFalse();
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, null)).isFalse();
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, "")).isFalse();
  }

  @Test
  void aRecordTheFollowerDoesNotHoldIsASchemaChange() {
    aggregateOf(leader).invalidate(100L, 200L);
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();
  }

  @Test
  void aRecordTheFollowerHoldsAlreadyIsNot() {
    aggregateOf(leader).invalidate(100L, 200L);
    aggregateOf(follower).invalidate(100L, 200L);
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isFalse();
  }

  @Test
  void aRecordWiderThanTheOneTheFollowerHoldsIsASchemaChange() {
    aggregateOf(follower).invalidate(100L, 200L);
    aggregateOf(leader).invalidate(100L, 200L);
    aggregateOf(leader).invalidate(50L, 120L);
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();

    aggregateOf(leader).invalidate(100L, 300L);
    aggregateOf(follower).invalidate(50L, 120L);
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();
  }

  @Test
  void aRecordOnAnAggregateTheFollowerDoesNotHaveIsASchemaChange() {
    follower.getSchema().dropContinuousAggregate("hourly");
    aggregateOf(leader).invalidate(100L, 200L);
    assertThat(carriesAContinuousAggregateRecordNotHeldHere(follower, schemaOf(leader))).isTrue();
  }

  private static DatabaseInternal create(final String name) {
    final String path = ROOT + name;
    FileUtils.deleteRecursively(new File(path));
    final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(path).create();
    db.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)").close();
    db.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, count(*) AS n FROM sensor GROUP BY hr").close();
    return db;
  }

  private static ContinuousAggregateImpl aggregateOf(final DatabaseInternal db) {
    return (ContinuousAggregateImpl) db.getSchema().getContinuousAggregate("hourly");
  }

  private static String schemaOf(final DatabaseInternal db) {
    return db.getSchema().getEmbedded().toJSON().toString();
  }
}
