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
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10147: a {@code CREATE ... TYPE} statement runs in one schema frame, and whatever waits for the async workers to drain has to be
 * asked BEFORE the frame is opened, or it waits for workers that need the write lock the frame holds, until its timeout. Linking a super
 * type is known to the base class; building an index is known only to the statement that does it, which says so through
 * {@code createsAnIndex()}. Nothing forces a statement to say so, and a missing answer shows only under load, so what every kind of
 * statement answers is pinned here: a new kind of statement, or a new way for an existing one to build an index, is a decision to take
 * in this test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10147CreatesAnIndexHookTest extends TestHelper {

  @Test
  void onlyAUniqueEdgeTypeBuildsAnIndexWhenItIsCreated() {
    assertThat(statement("CREATE EDGE TYPE R UNIQUE").createsAnIndex()).as("a UNIQUE edge type builds its constraint's index").isTrue();
    assertThat(statement("CREATE EDGE TYPE R EXTENDS B UNIQUE").createsAnIndex()).isTrue();

    assertThat(statement("CREATE EDGE TYPE R").createsAnIndex()).isFalse();
    assertThat(statement("CREATE EDGE TYPE R LIGHTWEIGHT").createsAnIndex()).isFalse();
    assertThat(statement("CREATE EDGE TYPE R UNIDIRECTIONAL").createsAnIndex()).isFalse();
    assertThat(statement("CREATE VERTEX TYPE V").createsAnIndex()).isFalse();
    assertThat(statement("CREATE VERTEX TYPE V EXTENDS A, B").createsAnIndex()).as("linking is asked for through the super types").isFalse();
    assertThat(statement("CREATE DOCUMENT TYPE D").createsAnIndex()).isFalse();
  }

  private CreateTypeAbstractStatement statement(final String sql) {
    return (CreateTypeAbstractStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
  }
}
