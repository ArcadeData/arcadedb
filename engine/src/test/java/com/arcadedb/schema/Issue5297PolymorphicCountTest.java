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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5297: the polymorphic bucket cache of a super type must stay in sync when buckets
 * are added to (or removed from) a sub type that is already linked to it. Before the fix, only a schema reload
 * repaired the cache, so on a HA cluster the leader (which mutates the schema in place) reported a different
 * `count(*)` on the base type than the followers (which rebuild the schema from the replicated schema.json).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5297PolymorphicCountTest extends TestHelper {

  private long countViaSQL(final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + typeName)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  public void bucketAddedToSubTypeAfterInheritanceIsCountedOnSuperType() {
    database.getSchema().createVertexType("V");
    database.getSchema().createVertexType("Tenant").addSuperType("V");

    // ADD A SECOND BUCKET TO THE SUB TYPE *AFTER* THE INHERITANCE HAS BEEN DECLARED
    database.command("sql", "ALTER TYPE Tenant BUCKET +Tenant_extra");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newVertex("Tenant").set("id", i).save();
    });

    assertThat(database.getSchema().getType("Tenant").getBuckets(false)).hasSize(2);
    // V's own bucket + the 2 buckets of Tenant
    assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(3);

    assertThat(database.countType("Tenant", true)).isEqualTo(20);
    assertThat(database.countType("V", true)).isEqualTo(20);
    assertThat(countViaSQL("V")).isEqualTo(20);

    // THE STALE CACHE HID THE RECORDS FROM EVERY POLYMORPHIC READ, NOT JUST FROM count(*)
    try (final ResultSet rs = database.query("sql", "SELECT FROM V")) {
      assertThat(rs.stream().count()).isEqualTo(20);
    }
  }

  /**
   * Models the HA asymmetry reported in the issue: the leader keeps mutating its in-memory schema while the followers
   * rebuild theirs from the replicated schema.json. Reopening the database here replays exactly the followers' path,
   * so both counts must agree.
   */
  @Test
  public void inMemorySchemaCountMatchesTheReloadedSchemaCount() {
    database.getSchema().createVertexType("V");
    database.getSchema().createVertexType("Tenant").addSuperType("V");
    database.getSchema().createVertexType("Agent").addSuperType("V");

    database.command("sql", "ALTER TYPE Tenant BUCKET +Tenant_extra");
    database.command("sql", "ALTER TYPE Agent BUCKET +Agent_extra");

    database.transaction(() -> {
      for (int i = 0; i < 12; i++)
        database.newVertex("Tenant").set("id", i).save();
      for (int i = 0; i < 31; i++)
        database.newVertex("Agent").set("id", i).save();
    });

    final long beforeReload = countViaSQL("V");

    reopenDatabase();

    assertThat(countViaSQL("V")).isEqualTo(43);
    assertThat(beforeReload).isEqualTo(43);
  }

  @Test
  public void bucketRemovedFromSubTypeAfterInheritanceIsDroppedFromSuperType() {
    database.getSchema().createVertexType("V");
    final DocumentType tenant = database.getSchema().buildVertexType().withName("Tenant").withTotalBuckets(2).create();
    tenant.addSuperType("V");

    assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(3);

    database.command("sql", "ALTER TYPE Tenant BUCKET -Tenant_1");

    assertThat(database.getSchema().getType("Tenant").getBuckets(false)).hasSize(1);
    assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(2);
  }

  @Test
  public void grantingMoreBucketsOnExistingTypeKeepsSuperTypeInSync() {
    database.getSchema().createVertexType("V");
    // FIRST CALL: CREATES THE TYPE WITH 1 BUCKET AND LINKS IT TO V
    database.getSchema().buildVertexType().withName("Tenant").withTotalBuckets(1).withSuperType("V").withIgnoreIfExists(true)
        .create();
    // SECOND CALL (IDEMPOTENT PROVISIONING): GROWS THE TYPE TO 4 BUCKETS, V MUST SEE THEM ALL
    database.getSchema().buildVertexType().withName("Tenant").withTotalBuckets(4).withSuperType("V").withIgnoreIfExists(true)
        .create();

    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newVertex("Tenant").set("id", i).save();
    });

    assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(5);
    assertThat(countViaSQL("V")).isEqualTo(40);
  }

  @Test
  public void multiLevelHierarchyDoesNotDoubleCountIntermediateBuckets() {
    database.getSchema().createVertexType("V");
    database.getSchema().createVertexType("Account").addSuperType("V");
    database.getSchema().createVertexType("Customer").addSuperType("Account");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("Account").set("id", i).save();
      for (int i = 0; i < 7; i++)
        database.newVertex("Customer").set("id", i).save();
    });

    assertThat(database.getSchema().getType("V").getBuckets(true)).hasSize(3);
    assertThat(database.getSchema().getType("Account").getBuckets(true)).hasSize(2);
    assertThat(countViaSQL("Customer")).isEqualTo(7);
    assertThat(countViaSQL("Account")).isEqualTo(12);
    assertThat(countViaSQL("V")).isEqualTo(12);
  }
}
