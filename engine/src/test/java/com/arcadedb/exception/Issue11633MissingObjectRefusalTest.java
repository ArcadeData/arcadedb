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
package com.arcadedb.exception;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.query.search.FullTextQuery;
import com.arcadedb.query.search.VectorSearch;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11633: refusals of a missing type, bucket, index, record or function that were raised as a plain
 * {@code IllegalArgumentException}, so every protocol answered them as a generic invalid argument rather than as the missing
 * object they report. The census ({@code MissingObjectRefusalCensusTest}) now ranges over every throwable; this pins what the
 * converted sites answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11633MissingObjectRefusalTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person").createProperty("name", String.class);
      database.getSchema().getType("Person").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      database.getSchema().createEdgeType("Knows");
      database.newVertex("Person").set("name", "a").save();
    });
  }

  @Test
  void aMissingFullTextOrVectorIndexOfTheSearchServicesIsASchemaFailure() {
    assertSchema(() -> FullTextQuery.search(database, new JSONObject().put("indexName", "nosuch").put("queryText", "x")));
    assertSchema(() -> VectorSearch.search(database, new JSONObject().put("indexName", "nosuch")
        .put("queryVector", new JSONArray(List.of(1.0, 0.0))).put("k", 3)));
  }

  @Test
  void aMissingEdgeTypeOfACypherProcedureIsASchemaFailure() {
    final Throwable error = catchThrowable(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Person) CALL algo.dijkstra(a, a, 'NoSuchRel', 'w') YIELD weight RETURN weight")) {
        rs.stream().count();
      }
    });
    assertThat(ErrorCategory.of(error)).as(String.valueOf(error)).isEqualTo(ErrorCategory.SCHEMA);
  }

  @Test
  void aMissingEdgeTypeOfAGraphBatchIsASchemaFailure() {
    final RID person = database.query("sql", "SELECT FROM Person").next().getIdentity().orElseThrow();
    final Throwable error = catchThrowable(() -> database.transaction(() -> {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(person, "NoSuchEdge", person);
      }
    }));
    assertThat(CauseChain.find(error, SchemaException.class)).as(String.valueOf(error)).isNotNull();

    // a type that exists but is not an edge type is a wrong argument, not a missing object
    final Throwable notAnEdge = catchThrowable(() -> database.transaction(() -> {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(person, "Person", person);
      }
    }));
    assertThat(CauseChain.find(notAnEdge, IllegalArgumentException.class)).as(String.valueOf(notAnEdge)).isNotNull();
    assertThat(CauseChain.find(notAnEdge, SchemaException.class)).as(String.valueOf(notAnEdge)).isNull();
  }

  @Test
  void aMissingEndpointOfNewEdgeByKeysIsAMissingRecord() {
    final Throwable error = catchThrowable(() -> database.transaction(() -> database.newEdgeByKeys("Person", new String[] { "name" },
        new Object[] { "a" }, "Person", new String[] { "name" }, new Object[] { "nobody" }, false, "Knows", true)));
    assertThat(error).isInstanceOf(RecordNotFoundException.class).hasMessageContaining("Cannot find destination vertex");
    assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.NOT_FOUND);
  }

  @Test
  void aMissingFunctionOfALibraryIsAnUnknownFunction() {
    database.command("sql", "DEFINE FUNCTION lib11633.answer 'SELECT 42 AS result' LANGUAGE sql");

    for (final String statement : List.of("SELECT lib11633.nosuch()", "DELETE FUNCTION nosuchlib11633.f")) {
      final Throwable error = catchThrowable(() -> {
        try (final ResultSet rs = database.command("sql", statement)) {
          rs.stream().count();
        }
      });
      assertThat(CauseChain.find(error, UnknownFunctionException.class)).as(statement + ": " + error).isNotNull();
      assertThat(ErrorCategory.of(error)).as(statement + ": " + error).isEqualTo(ErrorCategory.PARSING);
    }

    assertThat(catchThrowable(() -> database.getSchema().getFunction("lib11633", "nosuch"))).isInstanceOf(UnknownFunctionException.class);
    assertThat(catchThrowable(() -> database.getSchema().getFunctionLibrary("nosuchlib11633"))).isInstanceOf(
        UnknownFunctionException.class);
  }

  private static void assertSchema(final Executable call) {
    final Throwable error = catchThrowable(call::execute);
    assertThat(error).isInstanceOf(SchemaException.class).hasMessageContaining("does not exist");
    assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.SCHEMA);
    // HTTP answers a SchemaException that carries a cause as a server fault (500): the refusal of a missing index carries none
    assertThat(error.getCause()).isNull();
  }
}
