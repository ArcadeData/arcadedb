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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6383: {@code BooleanValue.getValue()} always returned {@code null}
 * (the parsed literal was discarded) and {@code FloatValue.getValue()} returned the raw string
 * image instead of a number, corrupting the generated SQL WHERE clause for Boolean/Float
 * GraphQL query arguments.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6383BooleanAndFloatArgumentsTest {

  private static final String DB_PATH = "./target/testgraphql_issue6383";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void booleanAndFloatArgumentsFilterCorrectly() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      if (factory.exists())
        factory.open().drop();

      final Database database = factory.create();
      try {
        database.transaction(() -> {
          final Schema schema = database.getSchema();
          schema.getOrCreateVertexType("Book");

          final MutableVertex book1 = database.newVertex("Book");
          book1.set("title", "In Stock Cheap");
          book1.set("inStock", true);
          book1.set("rating", 3.5f);
          book1.save();

          final MutableVertex book2 = database.newVertex("Book");
          book2.set("title", "Out Of Stock Expensive");
          book2.set("inStock", false);
          book2.set("rating", 4.5f);
          book2.save();
        });

        database.transaction(() -> {
          final String types = """
              type Query {
                books(inStock: Boolean rating: Float): [Book]
              }

              type Book {
                title: String
                inStock: Boolean
                rating: Float
              }""";
          database.command("graphql", types);

          // Boolean argument: before the fix, getValue() returned null, so the generated WHERE
          // compared against `null` and matched nothing.
          try (final ResultSet resultSet = database.query("graphql", "{ books(inStock: true) { title } }")) {
            assertThat(resultSet.hasNext()).isTrue();
            final Result record = resultSet.next();
            assertThat(record.<String>getProperty("title")).isEqualTo("In Stock Cheap");
            assertThat(resultSet.hasNext()).isFalse();
          }

          try (final ResultSet resultSet = database.query("graphql", "{ books(inStock: false) { title } }")) {
            assertThat(resultSet.hasNext()).isTrue();
            final Result record = resultSet.next();
            assertThat(record.<String>getProperty("title")).isEqualTo("Out Of Stock Expensive");
            assertThat(resultSet.hasNext()).isFalse();
          }

          // Float argument: before the fix, getValue() returned the raw string "4.5", so the
          // generated WHERE quoted it (`rating = "4.5"`) and a numeric property never matched.
          try (final ResultSet resultSet = database.query("graphql", "{ books(rating: 4.5) { title } }")) {
            assertThat(resultSet.hasNext()).isTrue();
            final Result record = resultSet.next();
            assertThat(record.<String>getProperty("title")).isEqualTo("Out Of Stock Expensive");
            assertThat(resultSet.hasNext()).isFalse();
          }
        });
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }
  }
}
