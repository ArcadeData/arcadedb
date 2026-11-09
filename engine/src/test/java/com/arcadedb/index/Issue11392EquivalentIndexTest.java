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
import com.arcadedb.exception.EquivalentIndexException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11392: an index the type already has under another name, or asked for twice without {@code IF NOT EXISTS}, is refused
 * as an {@link EquivalentIndexException}: the definition exists already, which every wire protocol answers as a conflict
 * ({@link ErrorCategory#ALREADY_EXISTS}, Bolt {@code Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists}, Postgres
 * {@code 42P07}) rather than as a malformed argument. Still an {@link IllegalArgumentException}, as the builders raised before,
 * and an index of a DIFFERENT definition on the same properties keeps the plain one: that is no equivalent rule.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11392EquivalentIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Eq").createProperty("a", Type.STRING);
      database.getSchema().getType("Eq").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    });
  }

  @Test
  void theSameIndexUnderAnotherNameIsEquivalent() {
    final Throwable refused = catchThrowable(() -> database.command("sql", "CREATE INDEX eqOther ON Eq (a) NOTUNIQUE"));

    assertThat(refused).isInstanceOf(IllegalArgumentException.class);
    assertThat(causeChainHolds(refused)).as("the refusal is an equivalent index").isTrue();
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
  }

  @Test
  void theSameIndexAskedTwiceWithoutIfNotExistsIsEquivalent() {
    final Throwable refused = catchThrowable(
        () -> database.getSchema().getType("Eq").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a"));

    assertThat(refused).isInstanceOf(EquivalentIndexException.class).hasMessageContaining("It already provides what was requested");
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
  }

  @Test
  void aCypherIndexUnderAnotherNameIsEquivalent() {
    database.command("opencypher", "CREATE INDEX cyA FOR (n:CyEq) ON (n.a)");

    final Throwable refused = catchThrowable(() -> database.command("opencypher", "CREATE INDEX cyB FOR (n:CyEq) ON (n.a)"));

    assertThat(causeChainHolds(refused)).isTrue();
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
  }

  /** The control: another definition on the same properties is no equivalent rule, and stays the argument error it was. */
  @Test
  void anotherDefinitionOnTheSamePropertiesIsNotEquivalent() {
    final Throwable refused = catchThrowable(
        () -> database.getSchema().getType("Eq").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "a"));

    assertThat(refused).isInstanceOf(IllegalArgumentException.class).isNotInstanceOf(EquivalentIndexException.class);
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.VALIDATION);
  }

  private static boolean causeChainHolds(final Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause())
      if (t instanceof EquivalentIndexException)
        return true;
    return false;
  }
}
