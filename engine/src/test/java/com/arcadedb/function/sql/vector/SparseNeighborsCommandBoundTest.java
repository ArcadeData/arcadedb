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
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code vector.sparseNeighbors} stops when the command it belongs to is out of time or was asked to stop, like its dense
 * counterpart {@code vector.neighbors} does: the bound is checked before every search pass, between the records the answer
 * loads, and inside the index search.
 * <p>
 * Every case builds the command context by hand with the bound already reached, so the outcome does not depend on how
 * fast the machine scans: a function that ignores its context returns the rows, one that honours it fails.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SparseNeighborsCommandBoundTest extends TestHelper {
  private static final int    DOCS      = 400;
  private static final int    DIMS      = 32;
  private static final int    NNZ       = 6;
  private static final int    K         = 10;
  private static final String TYPE_NAME = "BoundedSparseDoc";
  private static final String INDEX     = TYPE_NAME + "[tokens,weights]";

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  @Test
  void theSearchAnswersWhenNothingBoundsTheCommand() {
    createCorpus(TYPE_NAME, 1);
    final List<?> rows = search(unboundedContext(), K);
    assertThat(rows).as("the fixture answers, so the failures below are the bound and nothing else").hasSize(K);
  }

  @Test
  void aReachedCommandDeadlineStopsTheSearch() {
    createCorpus(TYPE_NAME, 1);
    final BasicCommandContext context = unboundedContext();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test bound");

    assertThatThrownBy(() -> search(context, K)).isInstanceOf(TimeoutException.class);
  }

  @Test
  void aReachedCommandDeadlineStopsAGroupedSearch() {
    createCorpus(TYPE_NAME, 1);
    final BasicCommandContext context = unboundedContext();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test bound");

    assertThatThrownBy(() -> new SQLFunctionVectorSparseNeighbors().execute(null, null, null,
        new Object[] { INDEX, queryIndices(), queryValues(), K, Map.of("groupBy", "grp", "groupSize", 2) }, context))
        .isInstanceOf(TimeoutException.class);
  }

  @Test
  void aReachedCommandDeadlineStopsASearchOverSeveralBuckets() {
    createCorpus(TYPE_NAME, 4);
    final BasicCommandContext context = unboundedContext();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test bound");

    assertThatThrownBy(() -> search(context, K)).isInstanceOf(TimeoutException.class);
  }

  @Test
  void aTerminatedStatementStopsTheSearch() {
    createCorpus(TYPE_NAME, 1);
    try (final RunningQuery statement = registry.register(database.getName(), "admin", "test", null, "bound-test",
        RunningQuery.Client.NONE)) {
      final BasicCommandContext context = unboundedContext();
      assertThat(statement.terminate("root")).isTrue();

      assertThatThrownBy(() -> search(context, K)).isInstanceOf(QueryTerminatedException.class);
    }
  }

  @Test
  void aTerminatedStatementStopsASearchOverSeveralBuckets() {
    createCorpus(TYPE_NAME, 4);
    try (final RunningQuery statement = registry.register(database.getName(), "admin", "test", null, "bound-test",
        RunningQuery.Client.NONE)) {
      final BasicCommandContext context = unboundedContext();
      assertThat(statement.terminate("root")).isTrue();

      assertThatThrownBy(() -> search(context, K)).isInstanceOf(QueryTerminatedException.class);
    }
  }

  @Test
  void aTerminatedStatementStopsTheSearchRunByAQuery() {
    createCorpus(TYPE_NAME, 1);
    try (final RunningQuery statement = registry.register(database.getName(), "admin", "test", null, "bound-test",
        RunningQuery.Client.NONE)) {
      assertThat(statement.terminate("root")).isTrue();

      assertThatThrownBy(() -> {
        try (final ResultSet resultSet = database.query("sql",
            "SELECT expand(vector.sparseNeighbors('" + INDEX + "', ?, ?, " + K + "))", queryIndices(), queryValues())) {
          while (resultSet.hasNext())
            resultSet.next();
        }
      }).isInstanceOf(QueryTerminatedException.class);
    }
  }

  private BasicCommandContext unboundedContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private List<?> search(final BasicCommandContext context, final int k) {
    return (List<?>) new SQLFunctionVectorSparseNeighbors().execute(null, null, null,
        new Object[] { INDEX, queryIndices(), queryValues(), k }, context);
  }

  private static int[] queryIndices() {
    final int[] indices = new int[NNZ];
    for (int i = 0; i < NNZ; i++)
      indices[i] = i;
    return indices;
  }

  private static float[] queryValues() {
    final float[] values = new float[NNZ];
    for (int i = 0; i < NNZ; i++)
      values[i] = 1.0f;
    return values;
  }

  private void createCorpus(final String typeName, final int buckets) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + typeName + ".weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY " + typeName + ".grp INTEGER");
      database.getSchema().buildTypeIndex(typeName, new String[] { "tokens", "weights" }).withSparseVectorType()
          .withDimensions(DIMS).create();

      final Random random = new Random(0x5A5AL);
      for (int i = 0; i < DOCS; i++) {
        final int[] indices = new int[NNZ];
        final float[] values = new float[NNZ];
        for (int j = 0; j < NNZ; j++) {
          indices[j] = j;
          values[j] = 0.1f + random.nextFloat();
        }
        final MutableDocument document = database.newDocument(typeName);
        document.set("tokens", indices);
        document.set("weights", values);
        document.set("grp", i % 7);
        document.save();
      }
    });
  }
}
