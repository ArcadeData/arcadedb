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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7228: {@code CREATE INDEX <name> IF NOT EXISTS ON <subType> (...)} was refused when
 * the index of that name is declared on a SUPER type of {@code <subType>}, with "an index with that name already
 * exists on type '...'. Drop it first or choose a different index name".
 * <p>
 * The refusal was wrong on its own terms. A type index is built over {@code getBuckets(true)} - the polymorphic
 * bucket list - so an index declared on {@code Elemento} already indexes every {@code ElementoNoIdentificable}
 * record; there is nothing for the statement to create and nothing the guard has to warn about. The unnamed form of
 * the very same statement had always been a no-op, because {@code TypeIndexBuilder.create()} finds the inherited
 * index through {@code getPolymorphicIndexByProperties}; only the named form, which is answered by a shortcut in
 * {@link com.arcadedb.query.sql.parser.CreateIndexStatement} before any builder exists, disagreed. That made a
 * schema script non-idempotent the moment one of its named indexes moved up the hierarchy - the one thing
 * IF NOT EXISTS is written to prevent.
 * <p>
 * The relation is checked in one direction only, which the tests below pin down: an index on a SUB type covers
 * strictly fewer buckets than a request on its parent, so it does not satisfy it and is still reported.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7228CreateIndexIfNotExistsSuperTypeTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Elemento");
      database.command("sql", "CREATE PROPERTY Elemento.uuid STRING");
      database.command("sql", "CREATE VERTEX TYPE ElementoNoIdentificable EXTENDS Elemento");
      database.command("sql", "CREATE VERTEX TYPE ElementoIdentificable EXTENDS Elemento");
    });
  }

  /**
   * The reported case verbatim: the index lives on the super type and the schema script asks for it again, by name,
   * on a sub type.
   */
  @Test
  void guardedNamedRequestOverAnIndexInheritedFromTheSuperTypeIsANoOp() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");

    try (final ResultSet rs = database.command("sql",
        "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isFalse();
      assertThat(result.<String>getProperty("name")).isEqualTo("Elemento_uuid");
    }

    // Exactly one index, still the one on the super type: the no-op must not have created a second.
    assertThat(database.getSchema().existsIndex("Elemento_uuid")).isTrue();
    assertThat(database.getSchema().getIndexByName("Elemento_uuid").getTypeName()).isEqualTo("Elemento");
    assertThat(database.getSchema().getType("ElementoNoIdentificable").getAllIndexes(false)).isEmpty();
  }

  /**
   * The point of answering the guard rather than refusing it: the inherited index really does constrain the sub
   * type, so the statement that was told "already exists" was told the truth.
   */
  @Test
  void theInheritedIndexEnforcesUniquenessOnTheSubType() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");
    database.command("sql", "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE");

    database.transaction(() -> database.command("sql", "INSERT INTO ElementoNoIdentificable SET uuid = 'u1'"));

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO ElementoNoIdentificable SET uuid = 'u1'")))
        .isInstanceOf(DuplicatedKeyException.class);

    // ...and across the hierarchy, not just within the one sub type.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO ElementoIdentificable SET uuid = 'u1'")))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /**
   * A whole schema script re-run end to end: every statement is guarded, so the second run must change nothing.
   */
  @Test
  void aGuardedSchemaScriptStaysIdempotentAcrossTheHierarchy() {
    final String script = "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `Elemento` (uuid) UNIQUE;"
        + "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE;"
        + "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoIdentificable` (uuid) UNIQUE;";

    database.command("sqlscript", script);
    database.command("sqlscript", script);

    assertThat(database.getSchema().getIndexes()).filteredOn(i -> "Elemento_uuid".equals(i.getName())).hasSize(1);
  }

  /**
   * The other direction is NOT symmetric: an index on a sub type indexes only that sub type's buckets, so it cannot
   * answer a request that spans the whole hierarchy. The conflict is still reported, naming both types.
   */
  @Test
  void guardedNamedRequestOverAnIndexOnASubTypeIsStillReported() {
    database.command("sql", "CREATE INDEX `sub_uuid` ON `ElementoNoIdentificable` (uuid) UNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX `sub_uuid` IF NOT EXISTS ON `Elemento` (uuid) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sub_uuid")
        .hasMessageContaining("Elemento")
        .hasMessageContaining("ElementoNoIdentificable");

    assertThat(database.getSchema().getIndexByName("sub_uuid").getTypeName()).isEqualTo("ElementoNoIdentificable");
  }

  /**
   * An unrelated type is still a conflict: the fix must not have turned the name check into a formality.
   */
  @Test
  void guardedNamedRequestOverAnIndexOnAnUnrelatedTypeIsStillReported() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Otro");
      database.command("sql", "CREATE PROPERTY Otro.uuid STRING");
    });
    database.command("sql", "CREATE INDEX `otro_uuid` ON `Otro` (uuid) UNIQUE");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX `otro_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("otro_uuid")
        .hasMessageContaining("Otro");
  }

  /**
   * The inherited index has to satisfy the request on its own terms too: a NOTUNIQUE parent index does not answer a
   * request for the UNIQUE constraint, exactly as it does not when the index is the type's own (issue #5675).
   */
  @Test
  void anInheritedIndexOfTheWrongKindDoesNotAnswerTheGuard() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) NOTUNIQUE");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE"))
        .hasMessageContaining("Elemento_uuid");
  }

  /**
   * Different properties under the same name stay a conflict even up the hierarchy: the name matched, the request
   * did not.
   */
  @Test
  void guardedNamedRequestOnDifferentPropertiesOfTheSuperTypeIsStillReported() {
    database.transaction(() -> database.command("sql", "CREATE PROPERTY Elemento.code STRING"));
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (code) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Elemento_uuid")
        .hasMessageContaining("code");
  }

  /**
   * Without the guard the statement is not asking to be made idempotent, so the pre-existing index is still
   * reported rather than silently accepted.
   */
  @Test
  void anUnguardedNamedRequestOverAnInheritedIndexIsStillReported() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX `Elemento_uuid` ON `ElementoNoIdentificable` (uuid) UNIQUE"))
        .hasMessageContaining("Elemento_uuid");
  }
}
