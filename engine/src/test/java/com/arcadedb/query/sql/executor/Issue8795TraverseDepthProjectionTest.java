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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.function.java.JavaMethodFunctionLibraryDefinition;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8795: TRAVERSE ran its projection on every vertex it visited, including those at MAXDEPTH, and dropped the
 * result afterwards. On a graph that fans out the last level is the largest, so most of the out() work loaded
 * neighbor lists nobody used. The projection must run only on vertices that can still expand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8795TraverseDepthProjectionTest extends TestHelper {
  private static final AtomicInteger TOUCHED = new AtomicInteger();

  /** The projection under test: counts its calls and leads nowhere. */
  public static Object touch(final Object ignored) {
    TOUCHED.incrementAndGet();
    return List.of();
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    // A CHAIN OF 6 VERTICES, SO MAXDEPTH 3 REACHES VERTEX 3 AND NOTHING BEYOND
    database.transaction(() -> {
      MutableVertex prev = null;
      for (int i = 0; i < 6; i++) {
        final MutableVertex v = database.newVertex("V").set("id", i).save();
        if (prev != null)
          prev.newEdge("E", v);
        prev = v;
      }
    });
    try {
      database.getSchema().registerFunctionLibrary(
          new JavaMethodFunctionLibraryDefinition("t8795", Issue8795TraverseDepthProjectionTest.class.getMethod("touch", Object.class)));
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @AfterEach
  void unregister() {
    database.getSchema().unregisterFunctionLibrary("t8795");
  }

  @Test
  void projectionDoesNotRunOnVerticesAtMaxDepth() {
    for (final String strategy : new String[] { "DEPTH_FIRST", "BREADTH_FIRST" }) {
      TOUCHED.set(0);
      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "TRAVERSE out('E'), `t8795.touch`(@rid) FROM (SELECT FROM V WHERE id = 0) MAXDEPTH 3 STRATEGY " + strategy)) {
        while (rs.hasNext())
          ids.add(rs.next().<Integer>getProperty("id"));
      }
      assertThat(ids).as(strategy).containsExactlyInAnyOrder(0, 1, 2, 3);
      // DEPTH 0, 1 AND 2 EXPAND; THE VERTEX AT DEPTH 3 DOES NOT
      assertThat(TOUCHED.get()).as(strategy).isEqualTo(3);
    }
  }
}
