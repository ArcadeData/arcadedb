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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6179: {@code BaseExpression.isEarlyCalculated()} classified an expression on its leftmost
 * term alone and never looked at the modifier chain, so {@code 'Mr.'.append(surname)} was reported
 * as computable without a record. The planner takes that as licence to use an index and to evaluate
 * the expression against a {@code null} record, so the record-dependent part silently resolved to
 * null and the query answered differently depending on whether an index existed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6179EarlyCalculatedModifierTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType person = database.getSchema().createVertexType("Person");
      person.createProperty("fullName", Type.STRING);
      person.createProperty("surname", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Person", "fullName");

      final VertexType noIndex = database.getSchema().createVertexType("PersonNoIndex");
      noIndex.createProperty("fullName", Type.STRING);
      noIndex.createProperty("surname", Type.STRING);
    });
  }

  private void createFixture() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET fullName = 'Mr.Smith', surname = 'Smith'");
      database.command("sql", "INSERT INTO Person SET fullName = 'Mr.Jones', surname = 'Jones'");
      database.command("sql", "INSERT INTO PersonNoIndex SET fullName = 'Mr.Smith', surname = 'Smith'");
      database.command("sql", "INSERT INTO PersonNoIndex SET fullName = 'Mr.Jones', surname = 'Jones'");
    });
  }

  @Test
  void aMethodCallOnARecordFieldIsNotEarlyCalculated() {
    createFixture();

    database.transaction(() -> {
      // the indexed and the non-indexed type hold the same rows, so both queries must answer the same
      try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person WHERE fullName = 'Mr.'.append(surname)")) {
        assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactlyInAnyOrder("Mr.Smith",
            "Mr.Jones");
      }

      try (final ResultSet scanned = database.query("sql",
          "SELECT fullName FROM PersonNoIndex WHERE fullName = 'Mr.'.append(surname)")) {
        assertThat(scanned.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactlyInAnyOrder("Mr.Smith",
            "Mr.Jones");
      }
    });
  }

  @Test
  void anIndexIsNotUsedWhenTheRightSideDependsOnTheRecord() {
    createFixture();

    database.transaction(() -> {
      try (final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT fullName FROM Person WHERE fullName = 'Mr.'.append(surname)")) {
        final String plan = explain.next().getProperty("executionPlanAsString");
        assertThat(plan).doesNotContain("FETCH FROM INDEX");
      }
    });
  }

  @Test
  void aCollectionSelectorOnARecordFieldIsNotEarlyCalculated() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET fullName = 'b', surname = 'x', idx = 1");
      database.command("sql", "INSERT INTO PersonNoIndex SET fullName = 'b', surname = 'x', idx = 1");
    });

    database.transaction(() -> {
      try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person WHERE fullName = ['a','b'][idx]")) {
        assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("b");
      }

      try (final ResultSet scanned = database.query("sql", "SELECT fullName FROM PersonNoIndex WHERE fullName = ['a','b'][idx]")) {
        assertThat(scanned.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("b");
      }
    });
  }

  @Test
  void aCaseExpressionOverARecordFieldIsNotEarlyCalculated() {
    createFixture();

    database.transaction(() -> {
      final String filter = " WHERE fullName = CASE surname WHEN 'Smith' THEN 'Mr.Smith' ELSE 'nomatch' END";

      try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person" + filter)) {
        assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("Mr.Smith");
      }

      try (final ResultSet scanned = database.query("sql", "SELECT fullName FROM PersonNoIndex" + filter)) {
        assertThat(scanned.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("Mr.Smith");
      }
    });
  }

  /**
   * The other half of the contract: a function whose arguments need no record stays "early calculated", so an
   * equality against it is still resolved through the index. Only expressions that reach the record are excluded.
   */
  @Test
  void aFunctionOverLiteralsIsStillEarlyCalculated() {
    createFixture();

    database.transaction(() -> {
      try (final ResultSet explain = database.query("sql", "EXPLAIN SELECT fullName FROM Person WHERE fullName = uuid()")) {
        final String plan = explain.next().getProperty("executionPlanAsString");
        assertThat(plan).contains("FETCH FROM INDEX");
      }
    });
  }

  @Test
  void aModifierOverALiteralIsStillEarlyCalculated() {
    createFixture();

    database.transaction(() -> {
      try (final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT fullName FROM Person WHERE fullName = 'Mr.'.append('Smith')")) {
        final String plan = explain.next().getProperty("executionPlanAsString");
        assertThat(plan).contains("FETCH FROM INDEX");
      }

      try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person WHERE fullName = 'Mr.'.append('Smith')")) {
        assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("Mr.Smith");
      }
    });
  }
}
