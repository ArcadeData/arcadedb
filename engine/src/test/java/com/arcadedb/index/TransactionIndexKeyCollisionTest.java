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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5055: on a NON-UNIQUE index, when an uncommitted entry has the SAME composite key as a committed one,
 * in-transaction index iteration must emit BOTH RIDs. The tx-overlay merge in {@code LSMTreeIndexCursor}
 * used to treat "key present in the overlay" as authoritative and shadow the disk cursor's equal-key
 * entries, dropping one of the two distinct RIDs until the transaction ended.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionIndexKeyCollisionTest extends TestHelper {

  @Test
  void inTxNonUniqueKeyCollisionSeesBothRids() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("Doc", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    // Committed baseline: one row with composite key (1,'m').
    database.transaction(() -> database.newDocument("Doc").set("a", 1).set("b", "m").save());

    database.begin();
    try {
      // Uncommitted second row with the SAME composite key (1,'m'), different RID.
      database.newDocument("Doc").set("a", 1).set("b", "m").save();

      final Set<String> rids = new HashSet<>();
      final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc WHERE a = 1");
      while (rs.hasNext())
        rids.add(rs.next().getProperty("rid").toString());

      assertThat(rids)
          .as("in-tx iteration on a non-unique index must return BOTH the committed and the uncommitted RID for key (1,'m') (#5055)")
          .hasSize(2);
    } finally {
      database.rollback();
    }
  }

  @Test
  void inTxNonUniqueKeyCollisionMultipleOverlayRids() {
    // Several uncommitted rows colliding with a single committed one on the same key.
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc2").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("Doc2", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> database.newDocument("Doc2").set("a", 1).set("b", "m").save());

    database.begin();
    try {
      database.newDocument("Doc2").set("a", 1).set("b", "m").save();
      database.newDocument("Doc2").set("a", 1).set("b", "m").save();

      int count = 0;
      final ResultSet rs = database.query("sql", "SELECT FROM Doc2 WHERE a = 1");
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
      assertThat(count)
          .as("in-tx iteration must return 1 committed + 2 uncommitted rows for the colliding key (#5055)")
          .isEqualTo(3);
    } finally {
      database.rollback();
    }
  }

  @Test
  void inTxNonUniqueKeyCollisionDescendingOrder() {
    // Descending traversal exercises the mirror branch of the tx/disk merge.
    final DocumentType type = database.getSchema().buildDocumentType().withName("DocDesc").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("DocDesc", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      database.newDocument("DocDesc").set("a", 1).set("b", "m").save();
      database.newDocument("DocDesc").set("a", 2).set("b", "m").save();
    });

    database.begin();
    try {
      // Uncommitted rows colliding with both committed keys.
      database.newDocument("DocDesc").set("a", 1).set("b", "m").save();
      database.newDocument("DocDesc").set("a", 2).set("b", "m").save();

      int count = 0;
      final ResultSet rs = database.query("sql", "SELECT FROM DocDesc WHERE a >= 1 AND a <= 2 ORDER BY a DESC, b DESC");
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
      assertThat(count)
          .as("descending in-tx iteration must merge committed and uncommitted RIDs at both colliding keys (#5055)")
          .isEqualTo(4);
    } finally {
      database.rollback();
    }
  }

  @Test
  void inTxNonUniqueKeyCollisionFullScan() {
    // Full index scan (no range bounds) uses a different constructor path than a range lookup.
    final DocumentType type = database.getSchema().buildDocumentType().withName("DocFull").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("DocFull", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> database.newDocument("DocFull").set("a", 1).set("b", "m").save());

    database.begin();
    try {
      database.newDocument("DocFull").set("a", 1).set("b", "m").save();

      int count = 0;
      final ResultSet rs = database.query("sql", "SELECT FROM DocFull");
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
      assertThat(count).as("full scan must see both colliding RIDs during in-tx iteration (#5055)").isEqualTo(2);
    } finally {
      database.rollback();
    }
  }

  @Test
  void inTxDescendingOverlayTopEntryNotDropped() {
    // Regression for the direction bug uncovered by #5055: getClosestEntryInTx's toKeys filter always used
    // the ascending sense, so a DESCENDING range scan dropped uncommitted overlay entries sitting above the
    // range's lower bound (the top of the scan) - even with NO committed collision at all.
    final DocumentType type = database.getSchema().buildDocumentType().withName("DocDescTop").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("DocDescTop", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    // Committed anchor only at the LOW end of the range.
    database.transaction(() -> database.newDocument("DocDescTop").set("a", 1).set("b", "m").save());

    database.begin();
    try {
      // Uncommitted entries strictly ABOVE the lower bound (a=1): these were the ones being dropped.
      database.newDocument("DocDescTop").set("a", 2).set("b", "m").save();
      database.newDocument("DocDescTop").set("a", 3).set("b", "m").save();

      int count = 0;
      final ResultSet rs = database.query("sql", "SELECT FROM DocDescTop WHERE a >= 1 AND a <= 3 ORDER BY a DESC");
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
      assertThat(count)
          .as("descending in-tx range scan must see uncommitted entries above the lower bound (#5055)")
          .isEqualTo(3);
    } finally {
      database.rollback();
    }
  }

  @Test
  void afterCommitBothRidsVisible() {
    // Sanity: post-commit the two colliding RIDs are already visible today; the defect is in-tx only.
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc3").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("Doc3", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> database.newDocument("Doc3").set("a", 1).set("b", "m").save());
    database.transaction(() -> database.newDocument("Doc3").set("a", 1).set("b", "m").save());

    int count = 0;
    final ResultSet rs = database.query("sql", "SELECT FROM Doc3 WHERE a = 1");
    while (rs.hasNext()) {
      rs.next();
      ++count;
    }
    assertThat(count).isEqualTo(2);
  }
}
