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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9468: a GraphQL row answered no property type, so the HTTP serializer formatted a
 * DATETIME_NANOS column with the precision of the value instead of the declared one, unlike a SQL row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9468PropertyTypeTest extends AbstractGraphQLTest {

  private static final String SDL = """
      type Query { docs: [DocView] @sql(statement: "SELECT FROM Doc") }
      type DocView { id: String dtn: String dtm: String dts: String }""";

  @Test
  void graphQLRowAnswersThePropertyTypeOfTheColumn() {
    executeDocTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ docs { dtn dtm dts } }")) {
        final Result row = resultSet.next();
        assertThat(row.getPropertyType("dtn")).isEqualTo(Type.DATETIME_NANOS);
        assertThat(row.getPropertyType("dtm")).isEqualTo(Type.DATETIME_MICROS);
        assertThat(row.getPropertyType("dts")).isEqualTo(Type.DATETIME_SECOND);
      }
    });
  }

  @Test
  void aliasedFieldAnswersThePropertyTypeOfItsColumn() {
    executeDocTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ docs { when: dtn } }")) {
        assertThat(resultSet.next().getPropertyType("when")).isEqualTo(Type.DATETIME_NANOS);
      }
    });
  }

  @Test
  void httpSerializationKeepsTheDeclaredNanosecondPrecisionLikeSql() {
    executeDocTest(database -> {
      final JsonSerializer serializer = JsonSerializer.createJsonSerializer();
      final String sql;
      try (final ResultSet resultSet = database.query("sql", "SELECT dtn FROM Doc")) {
        sql = serializer.serializeResult(database, resultSet.next()).getString("dtn");
      }
      try (final ResultSet resultSet = database.query("graphql", "{ docs { dtn } }")) {
        final JSONObject json = serializer.serializeResult(database, resultSet.next());
        assertThat(json.getString("dtn")).isEqualTo(sql);
      }
    });
  }

  private void executeDocTest(final Consumer<Database> assertions) {
    executeTest(database -> {
      database.getSchema().getOrCreateDocumentType("Doc");
      database.getSchema().getType("Doc").createProperty("dtn", Type.DATETIME_NANOS);
      database.getSchema().getType("Doc").createProperty("dtm", Type.DATETIME_MICROS);
      database.getSchema().getType("Doc").createProperty("dts", Type.DATETIME_SECOND);
      final LocalDateTime now = LocalDateTime.of(2026, 10, 7, 21, 39, 34, 645_360_000);
      database.newDocument("Doc").set("id", "d1").set("dtn", now).set("dtm", now).set("dts", now).save();
      database.command("graphql", SDL);
      assertions.accept(database);
      return null;
    });
  }
}
