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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rows a sort buffers are charged to the query heap budget for the lists they hold at any level of nesting: a row whose
 * column is a list of lists of lists of numbers takes as much of the budget as one whose column is the list of numbers.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class NestedValuesHeapBudgetTest extends TestHelper {
  private static final int ROWS     = 40;
  private static final int ELEMENTS = 5_000;

  @BeforeEach
  void fill() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    database.getSchema().createDocumentType("Nested");
    database.getSchema().createDocumentType("Direct");
    database.getSchema().createVertexType("Node");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final List<Object> numbers = new ArrayList<>(ELEMENTS);
        for (int k = 0; k < ELEMENTS; k++)
          numbers.add(1_000_000 + i * ELEMENTS + k);
        final List<Object> nested = new ArrayList<>();
        nested.add(List.of(List.of(numbers)));
        database.newDocument("Nested").set("i", i, "n", nested).save();
        database.newDocument("Direct").set("i", i, "n", numbers).save();
      }
      for (int i = 0; i < 2_000; i++)
        database.newVertex("Node").set("i", i, "name", "node-" + i).save();
    });
  }

  @AfterEach
  void resetBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void aSortOfRowsHoldingNestedListsIsRefusedWhenTheyDoNotFitTheBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

    assertThatThrownBy(() -> drain("SELECT i, n FROM Direct ORDER BY i DESC")).as("the same numbers held directly")
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThatThrownBy(() -> drain("SELECT i, n FROM Nested ORDER BY i DESC")).as("the same numbers nested in lists")
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).as("the refused queries gave their heap back").isZero();
  }

  @Test
  void aSortOfRowsHoldingNestedListsRunsWhenTheyFitTheBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    assertThat(drain("SELECT i FROM Nested ORDER BY i DESC")).isEqualTo(ROWS);

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(512L);
    assertThat(drain("SELECT i, n FROM Nested ORDER BY i DESC")).isEqualTo(ROWS);
    assertThat(drain("SELECT i, n FROM Direct ORDER BY i DESC")).isEqualTo(ROWS);
    assertThat(QueryHeapBudget.getReservedBytes()).as("the queries gave their heap back").isZero();
  }

  @Test
  void aRecordNestedInListsIsChargedForItsSizeAsOneHeldDirectlyIs() {
    final List<Object> records = new ArrayList<>();
    final Iterator<Record> nodes = database.iterateType("Node", true);
    while (nodes.hasNext())
      records.add(nodes.next());
    assertThat(records).hasSize(2_000);

    final ResultInternal direct = new ResultInternal();
    direct.setProperty("n", records);
    final ResultInternal nested = new ResultInternal();
    nested.setProperty("n", List.of(List.of(List.of(records))));

    final long directBytes = HeapEstimator.estimate(direct);
    assertThat(directBytes).isGreaterThan(records.size() * 100L);
    assertThat(HeapEstimator.estimate(nested)).isGreaterThanOrEqualTo(directBytes * 9 / 10);
  }

  private int drain(final String sql) {
    int rows = 0;
    try (final ResultSet resultSet = database.query("sql", sql)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        if (row.hasProperty("n"))
          assertThat(row.<Collection<?>>getProperty("n")).isNotEmpty();
        ++rows;
      }
    }
    return rows;
  }
}
