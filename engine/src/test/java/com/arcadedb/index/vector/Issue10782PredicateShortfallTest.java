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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.vector.RecordPredicateRidSet;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10782: a predicate allow-list reports an unbounded size, since it decides its members one candidate at a time. By
 * size alone the shortfall fallback read every selective predicate as a graph that needs rebuilding, and logged a WARNING
 * saying so on each such search of {@code vector.neighbors} with a {@code where} option (#10169), and now of the Cypher
 * {@code SEARCH} clause's {@code WHERE}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10782PredicateShortfallTest extends TestHelper {

  @Test
  void aPredicateShortfallIsThePredicatesNotTheGraphs() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final Set<RID> predicate = RecordPredicateRidSet.matching(record -> true, context);

    assertThat(predicate).isInstanceOf(PredicateRidSet.class);
    assertThat(predicate.size()).as("the reason size cannot decide it").isEqualTo(Integer.MAX_VALUE);
    assertThat(LSMVectorIndex.shortfallIsAllowListDriven(predicate, 5)).isTrue();
  }
}
