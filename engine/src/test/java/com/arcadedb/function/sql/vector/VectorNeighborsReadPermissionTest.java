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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A user who may read a type but not its subtype searches a vector index spanning both. A row of a bucket the user cannot read is
 * never handed out, whether the query projects only {@code @rid} and the distance (the record read is deferred, issue #10973, so the
 * permission to read the bucket is asked for each row emitted) or reads a property of the record.
 */
class VectorNeighborsReadPermissionTest {
  private static final String PATH = "target/databases/VectorNeighborsReadPermissionTest";

  private DatabaseFactory factory;
  private Database        database;
  private Set<Integer>    deniedBucketIds;

  @BeforeEach
  void createCorpus() {
    // Without a security manager the engine asks no file permission at all, whatever user is bound
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("sql", "CREATE VERTEX TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.name STRING");
    database.command("sql", "CREATE PROPERTY Asset.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE VERTEX TYPE Image EXTENDS Asset");
    database.command("sql", "CREATE INDEX ON Asset (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"COSINE\" }");
    database.transaction(() -> {
      database.newVertex("Asset").set("name", "public-a", "vector", new float[] { 1f, 0f }).save();
      database.newVertex("Asset").set("name", "public-b", "vector", new float[] { 0.6f, 0.8f }).save();
      database.newVertex("Image").set("name", "secret-a", "vector", new float[] { 0.99f, 0.1f }).save();
      database.newVertex("Image").set("name", "secret-b", "vector", new float[] { 0.1f, 0.99f }).save();
    });
    deniedBucketIds = new HashSet<>(database.getSchema().getType("Image").getBucketIds(false));
  }

  @AfterEach
  void dropDatabase() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  private void bindUserDeniedTheSubtype() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "scoped";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !typeName.equals("Image");
      }

      @Override
      public long getResultSetLimit() {
        return -1;
      }

      @Override
      public long getReadTimeout() {
        return -1;
      }
    });
  }

  private List<RID> ridsOf(final String projection, final int k) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT " + projection + " FROM (SELECT expand(vector.neighbors('Asset[vector]', [1.0, 0.0], " + k + ")))")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rids.add(((Identifiable) row.getProperty("rid")).getIdentity());
      }
    }
    return rids;
  }

  @Test
  void aUserWhoMayReadEveryBucketIsAnsweredTheNearestOfBoth() {
    assertThat(ridsOf("@rid AS rid, distance", 4)).hasSize(4);
    assertThat(ridsOf("@rid AS rid, name", 4)).hasSize(4);
  }

  @Test
  void aRowOfAnUnreadableBucketIsNeverHandedOutWhateverTheProjectionAndK() {
    bindUserDeniedTheSubtype();
    for (int k = 1; k <= 4; k++)
      for (final String projection : new String[] { "@rid AS rid, distance", "@rid AS rid, name", "@rid AS rid" }) {
        List<RID> rids;
        try {
          rids = ridsOf(projection, k);
        } catch (final SecurityException e) {
          // the query fails when a row the user may not read would be answered
          continue;
        }
        for (final RID rid : rids)
          assertThat(deniedBucketIds).as("k=%d, projection %s answered %s", k, projection, rid).doesNotContain(rid.getBucketId());
      }
  }

  @Test
  void aSearchThatReachesEveryRecordFailsForAUserWhoMayNotReadOneOfThem() {
    bindUserDeniedTheSubtype();
    assertThatThrownBy(() -> ridsOf("@rid AS rid, distance", 4)).isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to read records");
    assertThatThrownBy(() -> ridsOf("@rid AS rid, name", 4)).isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed to read records");
  }

  @Test
  void aMaxDistanceBelowTheUnreadableRowKeepsTheQueryAnswerable() {
    bindUserDeniedTheSubtype();
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid FROM (SELECT expand(vector.neighbors('Asset[vector]', [1.0, 0.0], 4, { maxDistance: 0.0001 })))")) {
      while (rs.hasNext())
        rids.add(((Identifiable) rs.next().getProperty("rid")).getIdentity());
    }
    assertThat(rids).hasSize(1);
    assertThat(deniedBucketIds).doesNotContain(rids.getFirst().getBucketId());
  }
}
