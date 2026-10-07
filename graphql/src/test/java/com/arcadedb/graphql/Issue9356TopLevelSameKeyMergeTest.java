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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9356: two top-level selections sharing a response key are ONE field per the GraphQL
 * specification, but the gate counting the operation's selections rejected them as "multiple queries".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9356TopLevelSameKeyMergeTest extends AbstractGraphQLTest {

  @Test
  void introspectionTypeSelectedTwiceIsMerged() {
    executeTest(database -> {
      defineTypes(database);
      final Result record = single(database, "{ __type(name: \"Book\") { name } __type(name: \"Book\") { kind } }");
      assertThat(record.getPropertyNames()).containsExactly("name", "kind");
      assertThat(record.<String>getProperty("name")).isEqualTo("Book");
      assertThat(record.<String>getProperty("kind")).isEqualTo("OBJECT");
      return null;
    });
  }

  @Test
  void introspectionSchemaSelectedTwiceIsMerged() {
    executeTest(database -> {
      defineTypes(database);
      final Result record = single(database, "{ __schema { queryType { name } } __schema { queryType { kind } } }");
      assertThat(record.<Result>getProperty("queryType").getPropertyNames()).containsExactly("name", "kind");
      return null;
    });
  }

  @Test
  void dataFieldSelectedTwiceThroughAFragmentIsMerged() {
    executeTest(database -> {
      defineTypes(database);
      final Result record = single(database,
          "{ bookById(id: \"book-1\") { name } ...Q } fragment Q on Query { bookById(id: \"book-1\") { pageCount } }");
      assertThat(record.getPropertyNames()).contains("name", "pageCount");
      assertThat(record.<String>getProperty("name")).isNotNull();
      assertThat(record.<Integer>getProperty("pageCount")).isEqualTo(223);
      return null;
    });
  }

  @Test
  void dataFieldSelectedTwiceDirectlyIsMerged() {
    executeTest(database -> {
      defineTypes(database);
      final Result record = single(database, "{ bookById(id: \"book-1\") { id } bookById(id: \"book-1\") { name pageCount } }");
      assertThat(record.getPropertyNames()).contains("id", "name", "pageCount");
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      return null;
    });
  }

  @Test
  void overlappingSubFieldsOfDuplicatedTopLevelSelectionsAreReturnedOnce() {
    executeTest(database -> {
      defineTypes(database);
      final Result record = single(database, "{ bookById(id: \"book-1\") { id name } bookById(id: \"book-1\") { name pageCount } }");
      assertThat(record.getPropertyNames().stream().filter("name"::equals).count()).isEqualTo(1);
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<Integer>getProperty("pageCount")).isEqualTo(223);
      return null;
    });
  }

  @Test
  void sameKeyWithDifferentArgumentsStaysRejected() {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> database.query("graphql", "{ bookById(id: \"book-1\") { id } bookById(id: \"book-2\") { name } }"))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("multiple queries");
      assertThatThrownBy(() -> database.query("graphql", "{ __type(name: \"Book\") { name } __type(name: \"Author\") { kind } }"))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("multiple queries");
      return null;
    });
  }

  @Test
  void differentResponseKeysStayRejected() {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> database.query("graphql", "{ bookById(id: \"book-1\") { id } b: bookById(id: \"book-1\") { name } }"))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("multiple queries");
      return null;
    });
  }

  private static Result single(final Database database, final String query) {
    try (final ResultSet resultSet = database.query("graphql", query)) {
      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat(resultSet.hasNext()).isFalse();
      return record;
    }
  }
}
