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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #4113: GraphBatch must honour unique edge constraints.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphBatchUniqueIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType zone = database.getSchema().createVertexType("zone");
      zone.createProperty("id", Type.STRING);

      final VertexType device = database.getSchema().createVertexType("device");
      device.createProperty("id", Type.STRING);

      final EdgeType edge = database.getSchema().createEdgeType("zone_device");
      edge.createProperty("from_id", Type.STRING);
      edge.createProperty("to_id", Type.STRING);

      database.getSchema().buildTypeIndex("zone_device", new String[] { "from_id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      database.getSchema().buildTypeIndex("zone_device", new String[] { "to_id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      database.getSchema().buildTypeIndex("zone_device", new String[] { "from_id", "to_id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    });
  }

  /**
   * Reproduces the exact scenario from issue #4113: two identical edges added in the same
   * batch must trigger a {@link DuplicatedKeyException} on the unique (from_id, to_id) index.
   */
  @Test
  void duplicateEdgeInSameFlush() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex zone = database.newVertex("zone").set("id", "zone1").save();
      final MutableVertex device = database.newVertex("device").set("id", "device1").save();
      rids[0] = zone.getIdentity();
      rids[1] = device.getIdentity();
    });

    assertThatThrownBy(() -> {
      try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
        batch.newEdge(rids[0], "zone_device", rids[1], "from_id", "zone1", "to_id", "device1");
        batch.newEdge(rids[0], "zone_device", rids[1], "from_id", "zone1", "to_id", "device1");
      }
    }).isInstanceOf(DuplicatedKeyException.class);

    // After the failed batch, no edges should be persisted (TX rolled back).
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM zone_device")) {
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isZero();
      }
    });
  }

  /**
   * A duplicate appearing in a LATER flush than the original must still be rejected.
   */
  @Test
  void duplicateEdgeAcrossFlushes() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex zone = database.newVertex("zone").set("id", "zone1").save();
      final MutableVertex device = database.newVertex("device").set("id", "device1").save();
      rids[0] = zone.getIdentity();
      rids[1] = device.getIdentity();
    });

    assertThatThrownBy(() -> {
      try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
        batch.newEdge(rids[0], "zone_device", rids[1], "from_id", "zone1", "to_id", "device1");
        batch.flush();
        batch.newEdge(rids[0], "zone_device", rids[1], "from_id", "zone1", "to_id", "device1");
      }
    }).isInstanceOf(DuplicatedKeyException.class);

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM zone_device")) {
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isOne();
      }
    });
  }

  /**
   * Two distinct edges with the same {@code from_id} but different {@code to_id} must
   * succeed (the NOTUNIQUE single-column indexes on {@code from_id} / {@code to_id} should
   * allow duplicates while the composite UNIQUE index enforces uniqueness).
   */
  @Test
  void distinctEdgesAreAllowed() {
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex zone = database.newVertex("zone").set("id", "zone1").save();
      final MutableVertex device1 = database.newVertex("device").set("id", "device1").save();
      final MutableVertex device2 = database.newVertex("device").set("id", "device2").save();
      rids[0] = zone.getIdentity();
      rids[1] = device1.getIdentity();
      rids[2] = device2.getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
      batch.newEdge(rids[0], "zone_device", rids[1], "from_id", "zone1", "to_id", "device1");
      batch.newEdge(rids[0], "zone_device", rids[2], "from_id", "zone1", "to_id", "device2");
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM zone_device")) {
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(2L);
      }
    });
  }
}
