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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9428: a page written by another node reaches this one through {@link LSMVectorIndex#applyReplicatedPageUpdate},
 * which added its entries to the location index but left the id allocator where it was. The next local insert then
 * minted an id a replicated entry already held and superseded it: on a 3-node cluster the vectors a follower inserted
 * after the leader's first batch replaced the leader's in the index of every node.
 * <p>
 * The peer's write is simulated with an entry appended at an explicit id - the id the allocator here hands out next -
 * which the location index learns of but the allocator does not, exactly the state a replicated page leaves behind.
 */
class Issue9428ReplicatedPageAdvancesVectorIdTest extends TestHelper {
  private static final String TYPE_NAME = "Probe";

  @Test
  void aReplicatedPageAdvancesTheVectorIdAllocator() throws Exception {
    final VertexType type = database.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("vector", float[].class);
    // NOT INDEXED: ONLY HOLDS THE RECORD THE SIMULATED PEER ENTRY POINTS AT
    database.getSchema().buildVertexType().withName("Other").withTotalBuckets(1).create();
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(2);
    final TypeIndex typeIndex = builder.create();
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    database.transaction(() -> database.newVertex(TYPE_NAME).set("vector", new float[] { 0.1f, 0.2f }).save());
    assertThat(index.countEntries()).isEqualTo(1);

    // THE PEER'S ENTRY: THE NEXT ID THIS NODE WOULD MINT, FOR A RECORD THIS NODE DID NOT INDEX ITSELF
    final int peerId = index.residentLocationsForTest().getNextId();
    database.transaction(() -> {
      final RID peerRecord = database.newVertex("Other").save().getIdentity();
      index.persistEntryForTest(peerId, peerRecord, new float[] { 0.3f, 0.4f });
    });
    assertThat(index.countEntries()).isEqualTo(2);

    // THE PAGE ARRIVES THROUGH REPLICATION
    final DatabaseInternal db = (DatabaseInternal) database;
    final int lastPage = index.getTotalPages() - 1;
    final ImmutablePage page = db.getPageManager()
        .getImmutablePage(new PageId(db, index.getFileId(), lastPage), index.getPageSize(), false, false);
    index.applyReplicatedPageUpdate(page.modify());

    database.transaction(() -> database.newVertex(TYPE_NAME).set("vector", new float[] { 0.5f, 0.6f }).save());
    assertThat(index.countEntries()).as("the next local insert must not reuse the replicated entry's id").isEqualTo(3);
  }
}
