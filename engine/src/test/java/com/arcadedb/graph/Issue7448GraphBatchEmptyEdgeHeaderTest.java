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
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DocumentInternal;
import com.arcadedb.database.RID;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7448 (discussion #7439): every edge written by {@link GraphBatch} without properties was reported
 * as corrupted the first time its properties were read. The bulk writer stored the header end offset one byte short,
 * pointing at the property count instead of past it, and the property-count validation added for #5774 rejects that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7448GraphBatchEmptyEdgeHeaderTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Work7448");
      database.getSchema().createEdgeType("Cite7448");
    });
  }

  @Test
  void propertyLessEdgeFromGraphBatchReadsBackClean() {
    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < vertices.length; i++)
        vertices[i] = database.newVertex("Work7448").set("id", i).save().getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
      batch.newEdge(vertices[0], "Cite7448", vertices[1]);
    }

    final List<String> reported = WarningCapture.captureWarnings(() -> database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "select from Cite7448")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Edge edge = row.toElement().asEdge();

        assertThat(edge.getOut()).isEqualTo(vertices[0]);
        assertThat(edge.getIn()).isEqualTo(vertices[1]);
        assertThat(edge.getPropertyNames()).isEmpty();
        assertThat(edge.toMap(false)).isEmpty();
        assertThat(edge.toJSON().has("@rid")).isTrue();
        assertThat(row.getPropertyNames()).isEmpty();
        assertThat(rs.hasNext()).isFalse();

        // The accessors above materialised the record: its header end offset must point right past the property
        // count, where the values section starts, the same layout BinarySerializer.serializeProperties() writes
        final Binary buffer = ((BaseRecord) edge).getBuffer();
        buffer.position(((DocumentInternal) edge).getPropertiesStartingPosition());
        final int headerEndOffset = buffer.getInt();
        assertThat(buffer.getUnsignedNumber()).isZero();
        assertThat(headerEndOffset).as("header end offset must follow the property count").isEqualTo(buffer.position());
      }
    }));

    assertThat(reported.stream().filter(m -> m.contains("Possible corrupted record")).toList())
        .as("a property-less batch edge must not be reported as corrupted (captured=%s)", reported)
        .isEmpty();
  }
}
