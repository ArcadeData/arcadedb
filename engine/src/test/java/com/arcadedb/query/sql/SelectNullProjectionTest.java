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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for SELECT with NULL literal projections (issue #565).
 * Verifies that queries returning null values execute without error and produce
 * exactly one result record.
 */
class SelectNullProjectionTest extends TestHelper {

  @Test
  void selectNullLiteralReturnsOneResult() {
    try (final ResultSet rs = database.query("SQL", "SELECT null")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result).isNotNull();
      assertThat(result.getPropertyNames()).hasSize(1);
      final String propName = result.getPropertyNames().iterator().next();
      assertThat(result.<Object>getProperty(propName)).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void selectNullLiteralWithAliasReturnsOneResult() {
    try (final ResultSet rs = database.query("SQL", "SELECT null AS myNull")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result).isNotNull();
      assertThat(result.<Object>getProperty("myNull")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void selectIfWithNullConditionReturnsOneResult() {
    try (final ResultSet rs = database.query("SQL", "SELECT if(null, true, false)")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result).isNotNull();
      assertThat(result.getPropertyNames()).hasSize(1);
      final String propName = result.getPropertyNames().iterator().next();
      assertThat(result.<Object>getProperty(propName)).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void selectIfWithNullConditionAndAliasReturnsNull() {
    try (final ResultSet rs = database.query("SQL", "SELECT if(null, true, false) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result).isNotNull();
      assertThat(result.<Object>getProperty("result")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
