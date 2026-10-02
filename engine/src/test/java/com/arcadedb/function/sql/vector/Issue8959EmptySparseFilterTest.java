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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8959 for {@code vector.sparseNeighbors}: a {@code filter} that resolves to no RIDs matches nothing, it does not
 * lift the restriction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8959EmptySparseFilterTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Doc").create();
      type.createProperty("tenant", Type.STRING);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex("Doc", new String[] { "tokens", "weights" }).withSparseVectorType().withDimensions(4)
          .create();
      for (final String tenant : new String[] { "a", "b" }) {
        final MutableDocument d = database.newDocument("Doc");
        d.set("tenant", tenant);
        d.set("tokens", new int[] { 1 });
        d.set("weights", new float[] { 1.0f });
        d.save();
      }
    });
  }

  private int count(final String filter, final String tenant) {
    final List<RID> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', [1], [1.0], 5, { filter: " + filter + " }))",
        Map.of("tenant", tenant, "ids", ids))) {
      int n = 0;
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
      return n;
    }
  }

  @Test
  void subqueryMatchingNothingReturnsNothing() {
    assertThat(count("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "c")).isZero();
  }

  @Test
  void emptyRidListReturnsNothing() {
    assertThat(count(":ids", "a")).isZero();
  }

  @Test
  void nestedEmptyListAndListOfNullsReturnNothing() {
    final List<Object> nested = new ArrayList<>();
    nested.add(new ArrayList<RID>());
    final List<Object> nulls = new ArrayList<>();
    nulls.add(null);
    nulls.add(null);
    for (final List<Object> ids : List.of(nested, nulls))
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', [1], [1.0], 5, { filter: :ids }))", Map.of("ids", ids))) {
        assertThat(rs.hasNext()).isFalse();
      }
  }

  @Test
  void filterMatchingARecordStillRestrictsTheSearch() {
    assertThat(count("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "a")).isEqualTo(1);
  }

  @Test
  void badIndexSpecStaysLoudWithAnEmptyFilter() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(`vector.sparseNeighbors`('Nope[tokens,weights]', [1], [1.0], 5, { filter: :ids }))",
          Map.of("ids", new ArrayList<RID>()))) {
        rs.hasNext();
      }
    }).isInstanceOf(SchemaException.class);
  }
}
