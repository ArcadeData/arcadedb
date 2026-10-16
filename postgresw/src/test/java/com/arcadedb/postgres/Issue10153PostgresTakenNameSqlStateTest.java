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
package com.arcadedb.postgres;

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10153: a {@code CREATE} on a taken name reached a Postgres client as {@code XX000} internal_error, or as {@code 42P01}
 * undefined_table - the opposite of the refusal - for the ones raised as a {@code SchemaException}. It is the SQLSTATE Postgres
 * answers the same statement with now: {@code 42P07} for a relation, {@code 42701} for a column, {@code 42710} for anything else.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10153PostgresTakenNameSqlStateTest {

  @Test
  void aTakenRelationNameIsDuplicateTable() {
    for (final AlreadyExistsException.Kind kind : new AlreadyExistsException.Kind[] { AlreadyExistsException.Kind.TYPE,
        AlreadyExistsException.Kind.INDEX, AlreadyExistsException.Kind.MATERIALIZED_VIEW,
        AlreadyExistsException.Kind.CONTINUOUS_AGGREGATE })
      assertThat(PostgresNetworkExecutor.sqlStateFor(taken(kind))).as("%s", kind).isEqualTo("42P07");
  }

  @Test
  void aTakenPropertyNameIsDuplicateColumn() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(taken(AlreadyExistsException.Kind.PROPERTY))).isEqualTo("42701");
  }

  @Test
  void anyOtherTakenNameIsDuplicateObject() {
    for (final AlreadyExistsException.Kind kind : new AlreadyExistsException.Kind[] { AlreadyExistsException.Kind.CONSTRAINT,
        AlreadyExistsException.Kind.BUCKET, AlreadyExistsException.Kind.TRIGGER, AlreadyExistsException.Kind.FUNCTION,
        AlreadyExistsException.Kind.FUNCTION_LIBRARY, AlreadyExistsException.Kind.GRAPH_ANALYTICAL_VIEW, null })
      assertThat(PostgresNetworkExecutor.sqlStateFor(taken(kind))).as("%s", kind).isEqualTo("42710");
  }

  @Test
  void theCodeIsTheSameThroughAWrapperAndAMissingTypeKeepsItsOwn() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandExecutionException("Error on executing the statement", taken(AlreadyExistsException.Kind.TYPE)))).isEqualTo("42P07");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SchemaException("Type 'X' was not found"))).isEqualTo("42P01");
  }

  private static AlreadyExistsException taken(final AlreadyExistsException.Kind kind) {
    return new AlreadyExistsException(kind, "taken");
  }
}
