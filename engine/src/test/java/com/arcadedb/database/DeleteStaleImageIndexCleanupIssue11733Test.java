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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A delete removes the index entries of the record as it is when the delete reaches it, not as it was when the record was
 * read. A record read, then changed by a concurrent transaction that committed, then deleted, used to keep the entry the
 * concurrent change added, pointing at a record that no longer exists: once the RID was reused, an indexed lookup returned
 * a record holding another value (issue #11733).
 */
class DeleteStaleImageIndexCleanupIssue11733Test extends TestHelper {

  @Test
  void aDocumentChangedAfterItsReadLeavesNoIndexEntry() throws InterruptedException {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D");
      database.command("sql", "CREATE PROPERTY D.v INTEGER");
      database.command("sql", "CREATE INDEX ON D (v) NOTUNIQUE");
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D").set("v", 1).save().getIdentity());

    deleteAfterAConcurrentUpdate(rid[0]);

    assertThat(entries("D[v]", 1)).isEmpty();
    assertThat(entries("D[v]", 2)).isEmpty();
  }

  @Test
  void aVertexAndAnEdgeChangedAfterTheirReadLeaveNoIndexEntry() throws InterruptedException {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.command("sql", "CREATE PROPERTY V.v INTEGER");
      database.command("sql", "CREATE INDEX ON V (v) NOTUNIQUE");
      database.getSchema().createEdgeType("E");
      database.command("sql", "CREATE PROPERTY E.v INTEGER");
      database.command("sql", "CREATE INDEX ON E (v) NOTUNIQUE");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("v", 1).save();
      final MutableVertex b = database.newVertex("V").set("v", 9).save();
      rids[0] = a.newEdge("E", b, "v", 1).getIdentity();
      rids[1] = a.getIdentity();
    });

    deleteAfterAConcurrentUpdate(rids[0]);
    assertThat(entries("E[v]", 1)).isEmpty();
    assertThat(entries("E[v]", 2)).isEmpty();

    deleteAfterAConcurrentUpdate(rids[1]);
    assertThat(entries("V[v]", 1)).isEmpty();
    assertThat(entries("V[v]", 2)).isEmpty();
    assertThat(entries("V[v]", 9)).hasSize(1);
  }

  /** Reads the record, lets another transaction set {@code v = 2} and commit, then deletes the record as it was read. */
  private void deleteAfterAConcurrentUpdate(final RID rid) throws InterruptedException {
    database.begin();
    final Document readBefore = rid.asDocument(true);
    assertThat(readBefore.getInteger("v")).isEqualTo(1);

    final Thread concurrent = new Thread(() -> database.transaction(() -> rid.asDocument().modify().set("v", 2).save()));
    concurrent.start();
    concurrent.join();

    readBefore.delete();
    database.commit();
    assertThat(database.existsRecord(rid)).isFalse();
  }

  private List<RID> entries(final String indexName, final int key) {
    final List<RID> found = new ArrayList<>();
    database.transaction(() -> {
      final IndexCursor cursor = ((TypeIndex) database.getSchema().getIndexByName(indexName)).get(new Object[] { key });
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
    });
    return found;
  }
}
