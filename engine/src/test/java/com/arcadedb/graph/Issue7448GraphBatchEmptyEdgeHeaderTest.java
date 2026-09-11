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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

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

    final List<String> reported = new CopyOnWriteArrayList<>();
    final Logger originalLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(reported, originalLogger));
    try {
      database.transaction(() -> {
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
      });

      assertThat(reported.stream().filter(m -> m.contains("Possible corrupted record")).toList())
          .as("a property-less batch edge must not be reported as corrupted (captured=%s)", reported)
          .isEmpty();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  /**
   * Captures WARNING-and-above messages into a list while forwarding every record to the production logger.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> messages;
    private final Logger       delegate;

    CapturingLogger(final List<String> messages, final Logger delegate) {
      this.messages = messages;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template, good enough for the substring matching above.
        }
      }
      messages.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
          arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
          arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
