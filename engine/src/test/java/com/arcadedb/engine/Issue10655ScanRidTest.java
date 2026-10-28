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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10655 item 3: a scan built a bare {@link RID} per slot and the record then copied it into a {@link DatabaseRID}. The scan now
 * builds the {@link DatabaseRID} once. The identity a scanned record hands out must stay what it was: a {@link DatabaseRID} bound
 * to the record's database, equal to and hashing as the bare RID, and usable after the scan.
 */
class Issue10655ScanRidTest extends TestHelper {
  private static final int ROWS = 300;

  @Test
  void scannedDocumentsKeepTheirIdentity() {
    final DocumentType type = database.getSchema().createDocumentType("Doc10655", 1);
    final Set<RID> saved = new HashSet<>();
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        saved.add(database.newDocument("Doc10655").set("id", i).save().getIdentity());
    });
    assertThat(saved).hasSize(ROWS);

    final DatabaseInternal bound = ((DatabaseInternal) database).getWrappedDatabaseInstance();
    final Set<RID> scanned = new HashSet<>();
    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
    final Iterator<Record> iterator = bucket.iterator();
    while (iterator.hasNext()) {
      final Record record = iterator.next();
      final RID rid = record.getIdentity();
      assertThat(rid).isInstanceOf(DatabaseRID.class);
      assertThat(((DatabaseRID) rid).getBoundDatabase()).isSameAs(bound);
      assertThat(record.getIdentity()).as("the identity is one object, not rebuilt on each call").isSameAs(rid);
      assertThat(rid).isEqualTo(new RID(rid.getBucketId(), rid.getPosition()));
      assertThat(new RID(rid.getBucketId(), rid.getPosition())).isEqualTo(rid);
      assertThat(rid.hashCode()).isEqualTo(new RID(rid.getBucketId(), rid.getPosition()).hashCode());
      scanned.add(rid);
    }
    assertThat(scanned).isEqualTo(saved);

    // THE RIDS WORK AFTER THE SCAN: THEY RESOLVE AGAINST THEIR DATABASE
    for (final RID rid : scanned)
      assertThat(rid.asDocument().getInteger("id")).isNotNull();
  }

  @Test
  void rowsOfAQueryAndATransactionCachedRecordKeepTheirIdentity() {
    database.getSchema().createVertexType("V10655", 1);
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newVertex("V10655").set("id", i).save();
    });

    // THE SQL SCAN GOES THROUGH BucketIterator TOO; A RECORD MODIFIED IN THE OPEN TRANSACTION IS ANSWERED FROM IT, NOT FROM THE PAGE
    database.begin();
    try {
      final RID first = database.query("sql", "SELECT FROM V10655 WHERE id = 0").next().getIdentity().orElseThrow();
      final MutableDocument modified = first.asVertex().modify().set("id", 1000);
      modified.save();

      int count = 0;
      boolean sawModified = false;
      final var rs = database.query("sql", "SELECT FROM V10655");
      while (rs.hasNext()) {
        final Vertex vertex = rs.next().getVertex().orElseThrow();
        assertThat(vertex.getIdentity()).isInstanceOf(DatabaseRID.class);
        assertThat(vertex.getIdentity().asVertex().getIdentity()).isEqualTo(vertex.getIdentity());
        if (vertex.getIdentity().equals(first)) {
          sawModified = true;
          assertThat(vertex.getInteger("id")).isEqualTo(1000);
        }
        count++;
      }
      assertThat(count).isEqualTo(50);
      assertThat(sawModified).isTrue();
    } finally {
      database.rollback();
    }
  }

  @Test
  void scannedRecordsAreBoundToTheirOwnDatabaseWhenTwoAreOpen() {
    database.getSchema().createDocumentType("Doc10655", 1);
    database.transaction(() -> database.newDocument("Doc10655").set("id", 1).save());

    final DatabaseFactory otherFactory = new DatabaseFactory("target/databases/Issue10655Other");
    if (otherFactory.exists())
      otherFactory.open().drop();
    final Database other = otherFactory.create();
    try {
      other.getSchema().createDocumentType("Doc10655", 1);
      other.transaction(() -> other.newDocument("Doc10655").set("id", 2).save());

      for (final Database db : new Database[] { database, other }) {
        final Iterator<Record> iterator = db.iterateType("Doc10655", true);
        assertThat(iterator.hasNext()).isTrue();
        final Document document = (Document) iterator.next();
        assertThat(((DatabaseRID) document.getIdentity()).getBoundDatabase())
            .isSameAs(((DatabaseInternal) db).getWrappedDatabaseInstance());
        assertThat(document.getIdentity().asDocument().getInteger("id")).isEqualTo(document.getInteger("id"));
      }
    } finally {
      other.drop();
    }
  }
}
