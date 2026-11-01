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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11063: a follower inside a client transaction has the leader create the types of a multi-label node, and it must be ONE
 * schema entry (the follower's re-base requires it, see #10067). The statement the follower sends is {@code ensureLabelTypes}: the
 * labels that are missing and the composite, in one outermost schema frame, answering whether this call made the composite.
 */
class Issue11063EnsureLabelTypesFunctionTest extends TestHelper {

  @Test
  void missingLabelsAndTheCompositeAreOneSchemaEntry() {
    final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> {
      try (final ResultSet rs = database.command("sql", "SELECT ensureLabelTypes(['Lb2', 'Lb1']) AS created")) {
        assertThat(rs.next().<Boolean>getProperty("created")).isTrue();
      }
    });

    assertThat(sessions).isEqualTo(1);
    assertThat(database.getSchema().existsType("Lb1")).isTrue();
    assertThat(database.getSchema().existsType("Lb2")).isTrue();
    assertThat(database.getSchema().getType("Lb1~Lb2").instanceOf("Lb1")).isTrue();
    assertThat(database.getSchema().getType("Lb1~Lb2").instanceOf("Lb2")).isTrue();
  }

  @Test
  void aCompositeThatIsThereAlreadyAnswersNotCreatedAndPublishesNothing() {
    ensure("['Lc1', 'Lc2']");

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> {
      try (final ResultSet rs = database.command("sql", "SELECT ensureLabelTypes(['Lc1', 'Lc2']) AS created")) {
        assertThat(rs.next().<Boolean>getProperty("created")).isFalse();
      }
    });

    assertThat(sessions).isZero();
  }

  @Test
  void aLabelWithAQuoteIsPassedThroughIntact() {
    ensure("['it\\'s', 'Ld2']");
    assertThat(database.getSchema().existsType("it's")).isTrue();
  }

  /** The result set is lazy: the function runs when the row is read. */
  private void ensure(final String labels) {
    try (final ResultSet rs = database.command("sql", "SELECT ensureLabelTypes(" + labels + ") AS created")) {
      rs.next();
    }
  }
}
