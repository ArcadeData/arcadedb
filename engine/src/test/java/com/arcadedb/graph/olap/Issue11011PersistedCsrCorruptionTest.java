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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11011: the persisted CSR (format 5) is parsed while it streams in, before its checksum is known, so a corrupt
 * file must be refused by the bounds on what it declares, never by an exception escaping the loader or an allocation as
 * large as a corrupt length asks for. Every byte of the file is a candidate: header, frame lengths, array lengths, string
 * lengths, counts and payload. The loader answers the snapshot or null, nothing else.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11011PersistedCsrCorruptionTest extends TestHelper {
  private static final String   VIEW         = "corrupt";
  private static final String[] VERTEX_TYPES = { "V" };
  private static final String[] EDGE_TYPES   = { "E" };
  private static final String[] EDGE_PROPS   = { "w" };

  @Test
  void aCorruptFileIsRefusedWithoutAnExceptionOrAnUnboundedAllocation() throws Exception {
    createGraph();
    GraphAnalyticalView.builder(database).withName(VIEW).withVertexTypes(VERTEX_TYPES).withEdgeTypes(EDGE_TYPES)
        .withEdgeProperties(EDGE_PROPS).build();
    final File file = GraphAnalyticalViewCSRPersistence.fileFor(database, VIEW);
    // A clean close persists the CSR; nothing is committed after, so its certificate holds for the loads below
    reopenDatabase();
    assertThat(file).exists();
    final byte[] original = Files.readAllBytes(file.toPath());
    final long txId = ((DatabaseInternal) database).getTransactionManager().getLastTransactionId();

    assertThat(load(file, original, txId)).isNotNull();

    final Random random = new Random(11011);
    for (int i = 0; i < 400; i++) {
      final byte[] corrupt = original.clone();
      final int flips = 1 + random.nextInt(3);
      for (int f = 0; f < flips; f++)
        corrupt[random.nextInt(corrupt.length)] ^= (byte) (1 + random.nextInt(255));
      // A flipped byte never reaches a caller: the checksum, over the whole file, refuses what the bounds let through
      assertThat(load(file, corrupt, txId)).isNull();
    }
    // The header decides what the restored view claims to cover (its scanned edge types, the buckets of the edges it does
    // not hold): every one of its bytes is checked, one at a time, since random flips rarely land there
    for (int position = 0; position < Math.min(original.length, 512); position++) {
      final byte[] corrupt = original.clone();
      corrupt[position] ^= (byte) 0x5A;
      assertThat(load(file, corrupt, txId)).as("byte %d flipped", position).isNull();
    }
    for (int i = 0; i < 100; i++)
      assertThat(load(file, Arrays.copyOf(original, random.nextInt(original.length)), txId)).isNull();

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW).close();
  }

  private GraphAnalyticalView.Snapshot load(final File file, final byte[] content, final long txId) throws Exception {
    Files.write(file.toPath(), content);
    return GraphAnalyticalViewCSRPersistence.load(database, VIEW, VERTEX_TYPES, EDGE_TYPES, null, EDGE_PROPS, txId, -1L);
  }

  private void createGraph() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E").createProperty("w", Type.DOUBLE);
    database.transaction(() -> {
      final MutableVertex[] vertices = new MutableVertex[200];
      for (int i = 0; i < vertices.length; i++)
        vertices[i] = database.newVertex("V").set("id", i, "name", "vertex-" + i).save();
      for (int i = 0; i < 2_000; i++)
        vertices[i % vertices.length].newEdge("E", vertices[(i * 7 + 3) % vertices.length], "w", i / 10.0);
    });
  }
}
