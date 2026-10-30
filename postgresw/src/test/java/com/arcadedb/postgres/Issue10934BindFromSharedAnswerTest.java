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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10934: {@link PostgresPortal#bindFrom} was documented as leaving {@code fullResultSet} at its fresh default, while it
 * hands every portal bound from a statement the answer PARSE precomputed (SHOW, system and catalog queries): one list, shared
 * by every portal of the statement. The sharing is what makes those answers work, so it stays, documented, and the shared
 * lists are now handed out read-only: a portal that wrote into one in place would change the answer of all the others.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10934BindFromSharedAnswerTest {

  @Test
  void portalsBoundFromOneStatementShareItsPrecomputedAnswerReadOnly() {
    final PostgresPortal template = new PostgresPortal("SHOW TRANSACTION ISOLATION LEVEL", "sql");
    template.executed = true;
    template.fullResultSet = new ArrayList<>(List.of(new ResultInternal(Map.of("transaction_isolation", "read committed"))));
    template.cachedResultSet = new ArrayList<>(template.fullResultSet);

    final PostgresPortal first = PostgresPortal.bindFrom(template);
    final PostgresPortal second = PostgresPortal.bindFrom(template);

    // THE PRECOMPUTED ANSWER IS INHERITED, NOT LEFT AT ITS DEFAULT: AN EXECUTE OF A BOUND PORTAL SERVES IT
    assertThat(first.executed).isTrue();
    assertThat(first.fullResultSet).containsExactlyElementsOf(template.fullResultSet);
    assertThat(second.fullResultSet).containsExactlyElementsOf(template.fullResultSet);
    assertThat(first.cachedResultSet).containsExactlyElementsOf(template.cachedResultSet);
    assertThat(first.statement).isSameAs(template);

    // ...AND READ-ONLY: NO PORTAL CAN CHANGE WHAT THE OTHERS OF THE STATEMENT ANSWER
    final Result extra = new ResultInternal(Map.of("transaction_isolation", "serializable"));
    assertThatThrownBy(() -> first.fullResultSet.add(extra)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> first.fullResultSet.clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> first.cachedResultSet.set(0, extra)).isInstanceOf(UnsupportedOperationException.class);
    assertThat(second.fullResultSet).hasSize(1);
    assertThat(template.fullResultSet).hasSize(1);

    // a portal bound from a portal bound from the template keeps the same view, not a view of a view
    assertThat(PostgresPortal.bindFrom(first).fullResultSet).isSameAs(first.fullResultSet);
  }

  @Test
  void theReplacementOfAnInheritedAnswerIsThePortalsOwn() {
    final PostgresPortal template = new PostgresPortal("SELECT 1", "sql");
    template.fullResultSet = new ArrayList<>(List.of(new ResultInternal(Map.of("a", 1))));

    final PostgresPortal first = PostgresPortal.bindFrom(template);
    final PostgresPortal second = PostgresPortal.bindFrom(template);
    // AN EXECUTE THAT RE-RUNS THE STATEMENT REPLACES THE WHOLE FIELD (PostgresNetworkExecutor.executeCommand)
    first.fullResultSet = List.of(new ResultInternal(Map.of("a", 2)));

    assertThat(second.fullResultSet).singleElement().satisfies(r -> assertThat((Integer) r.getProperty("a")).isEqualTo(1));
    assertThat(template.fullResultSet).singleElement().satisfies(r -> assertThat((Integer) r.getProperty("a")).isEqualTo(1));
  }

  @Test
  void aStatementWithoutAPrecomputedAnswerBindsPortalsWithoutOne() {
    final PostgresPortal portal = PostgresPortal.bindFrom(new PostgresPortal("SELECT FROM V", "sql"));
    assertThat(portal.fullResultSet).isNull();
    assertThat(portal.cachedResultSet).isNull();
    assertThat(portal.executed).isFalse();
  }
}
