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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #12037: a schema statement refused because of the data already stored left as a
 * {@code CommandExecutionException} (MANDATORY and NOTNULL over records that lack the value), which HTTP answers 500, while MIN,
 * MAX and REGEXP refusals of the same kind answered 400. They leave as the type a write the same rule refuses already has: a
 * {@link ValidationException} naming the constraint. So does the refusal to change the partition key of a stored record.
 */
class StoredDataRefusalTest extends TestHelper {

  @Test
  void anExistenceConstraintOverRecordsThatViolateItIsAValidationRefusal() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p INTEGER");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET q = 1");
      database.command("sql", "INSERT INTO T SET p = null, n = null");
      database.command("sql", "INSERT INTO T SET p = 5");
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.m INTEGER (MANDATORY true)"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot create property 'T.m'")
        .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(ValidationException.Kind.MANDATORY));
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.n INTEGER (NOTNULL true)"))
        .isInstanceOf(ValidationException.class)
        .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(ValidationException.Kind.NOT_NULL));
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.p MANDATORY true"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot set MANDATORY")
        .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(ValidationException.Kind.MANDATORY));
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.p NOTNULL true"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot set NOTNULL")
        .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(ValidationException.Kind.NOT_NULL));

    // and nothing was declared
    final DocumentType type = database.getSchema().getType("T");
    assertThat(type.existsProperty("m")).isFalse();
    assertThat(type.existsProperty("n")).isFalse();
    assertThat(type.getProperty("p").isMandatory()).isFalse();
    assertThat(type.getProperty("p").isNotNull()).isFalse();
  }

  /** A value constraint keeps the kind of the value check it was refused by, and that check stays the cause. */
  @Test
  void aValueConstraintOverRecordsThatViolateItKeepsItsKindAndItsCause() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p INTEGER");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET p = 5, m = 5"));

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.p MIN 10"))
        .isInstanceOf(ValidationException.class)
        .hasCauseInstanceOf(ValidationException.class)
        .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(ValidationException.Kind.MIN));
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.m INTEGER (MIN 10)"))
        .isInstanceOf(ValidationException.class);
    assertThat(database.getSchema().getType("T").getProperty("p").getMin()).isNull();
  }

  @Test
  void changingThePartitionKeyIsAValidationRefusal() {
    createPartitionedType();
    database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 1"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE T SET k = 2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot modify the partition key");
  }

  private void createPartitionedType() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 4");
    database.command("sql", "CREATE PROPERTY T.k INTEGER");
    database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
    database.command("sql", "ALTER TYPE T BUCKETSELECTIONSTRATEGY `partitioned('k')`");
  }
}
