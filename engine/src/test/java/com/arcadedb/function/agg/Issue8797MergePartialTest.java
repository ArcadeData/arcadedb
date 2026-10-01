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
package com.arcadedb.function.agg;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8797: the partial states of the Cypher avg(), min() and max() merge into what one instance fed every row holds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8797MergePartialTest {
  private static void feed(final com.arcadedb.function.StatelessFunction function, final Object... values) {
    for (final Object value : values)
      function.execute(new Object[] { value }, null);
  }

  @Test
  void avgMerges() {
    final CypherAvgFunction a = new CypherAvgFunction();
    final CypherAvgFunction b = new CypherAvgFunction();
    feed(a, 1, 2, null);
    feed(b, 6);
    a.mergePartial(b);
    assertThat(a.getAggregatedResult()).isEqualTo(3.0);

    final CypherAvgFunction empty = new CypherAvgFunction();
    empty.mergePartial(new CypherAvgFunction());
    assertThat(empty.getAggregatedResult()).isNull();
    empty.mergePartial(a);
    assertThat(empty.getAggregatedResult()).isEqualTo(3.0);
  }

  @Test
  void minAndMaxMergeIncludingEmptyAndNullOnlyPartials() {
    final CypherMinFunction min = new CypherMinFunction();
    final CypherMinFunction otherMin = new CypherMinFunction();
    feed(min, 5, 7);
    feed(otherMin, 3, null);
    min.mergePartial(otherMin);
    min.mergePartial(new CypherMinFunction());
    assertThat(min.getAggregatedResult()).isEqualTo(3);

    final CypherMaxFunction max = new CypherMaxFunction();
    final CypherMaxFunction nullsOnly = new CypherMaxFunction();
    feed(nullsOnly, (Object) null);
    nullsOnly.mergePartial(max);
    assertThat(nullsOnly.getAggregatedResult()).isNull();
    feed(max, "a", "c");
    final CypherMaxFunction other = new CypherMaxFunction();
    feed(other, "b");
    other.mergePartial(max);
    assertThat(other.getAggregatedResult()).isEqualTo("c");
  }
}
