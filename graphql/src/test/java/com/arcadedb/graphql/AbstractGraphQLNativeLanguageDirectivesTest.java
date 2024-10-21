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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractGraphQLNativeLanguageDirectivesTest extends AbstractGraphQLTest {
  protected int getExpectedPropertiesMetadata() {
    return 3;
  }

  @Test
  public void testUseTypeDefinitionForReturn() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ bookByName(bookNameParameter: \"Harry Potter and the Philosopher's Stone\")}")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames().size()).isEqualTo(4 + getExpectedPropertiesMetadata());
        assertThat(record.<Collection<?>>getProperty("authors")).hasSize(1);
        assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
        assertThat(resultSet.hasNext()).isFalse();
      }

      try (final ResultSet resultSet = database.query("graphql", "{ bookByName(bookNameParameter: \"Mr. brain\") }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames().size()).isEqualTo(4 + getExpectedPropertiesMetadata());
        assertThat(record.<Collection<?>>getProperty("authors")).hasSize(1);
        assertThat(record.<String>getProperty("name")).isEqualTo("Mr. brain");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  public void testCustomDefinitionForReturn() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ bookByName(bookNameParameter: \"Harry Potter and the Philosopher's Stone\"){ id name pageCount } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames().size()).isEqualTo(3 + getExpectedPropertiesMetadata());
        assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
        assertThat(resultSet.hasNext()).isFalse();
      }

      try (final ResultSet resultSet = database.query("graphql",
          "{ bookByName(bookNameParameter: \"Mr. brain\"){ id name pageCount } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames().size()).isEqualTo(3 + getExpectedPropertiesMetadata());
        assertThat(record.<String>getProperty("name")).isEqualTo("Mr. brain");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
