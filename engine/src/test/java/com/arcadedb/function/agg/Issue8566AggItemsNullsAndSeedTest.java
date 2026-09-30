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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8566: a null in the value list of {@code agg.minItems}/{@code agg.maxItems} shortened it
 * and tripped the pairing guard, the minimum seed was {@code Double.MAX_VALUE} instead of +Infinity, and the empty
 * {@code agg.statistics} result had no {@code median} key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8566AggItemsNullsAndSeedTest {
  private final AggMinItems min = new AggMinItems();
  private final AggMaxItems max = new AggMaxItems();
  private final AggStatistics statistics = new AggStatistics();

  @Test
  @SuppressWarnings("unchecked")
  void nullValueIsSkippedTogetherWithItsItem() {
    final List<String> items = List.of("a", "b", "c");

    Map<String, Object> r = (Map<String, Object>) min.execute(new Object[] { Arrays.asList(3, null, 2), items }, null);
    assertThat(r.get("value")).isEqualTo(2.0);
    assertThat(r.get("items")).isEqualTo(List.of("c"));

    r = (Map<String, Object>) max.execute(new Object[] { Arrays.asList(3, null, 2), items }, null);
    assertThat(r.get("value")).isEqualTo(3.0);
    assertThat(r.get("items")).isEqualTo(List.of("a"));

    // the skipped position must not shift the pairing of the rest
    r = (Map<String, Object>) min.execute(new Object[] { Arrays.asList(null, 5, 1, 1), List.of("a", "b", "c", "d") }, null);
    assertThat(r.get("items")).isEqualTo(List.of("c", "d"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void realMispairingAndAllNullStillYieldNoAnswer() {
    Map<String, Object> r = (Map<String, Object>) min.execute(new Object[] { List.of(1, 2, 3), List.of("a", "b") }, null);
    assertThat(r.get("value")).isNull();
    assertThat((List<Object>) r.get("items")).isEmpty();

    r = (Map<String, Object>) max.execute(new Object[] { Arrays.asList(null, null), List.of("a", "b") }, null);
    assertThat(r.get("value")).isNull();
    assertThat((List<Object>) r.get("items")).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  void infinitiesArePairedOnBothEnds() {
    Map<String, Object> r = (Map<String, Object>) min.execute(
        new Object[] { List.of(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY), List.of("a", "b") }, null);
    assertThat(r.get("value")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(r.get("items")).isEqualTo(List.of("a", "b"));

    r = (Map<String, Object>) max.execute(
        new Object[] { List.of(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY), List.of("a", "b") }, null);
    assertThat(r.get("value")).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(r.get("items")).isEqualTo(List.of("a", "b"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void statisticsMinSeedAndEmptyShape() {
    Map<String, Object> r = (Map<String, Object>) statistics.execute(new Object[] { List.of(Double.POSITIVE_INFINITY) }, null);
    assertThat(r.get("min")).isEqualTo(Double.POSITIVE_INFINITY);

    final Map<String, Object> empty = (Map<String, Object>) statistics.execute(new Object[] { List.of() }, null);
    final Map<String, Object> full = (Map<String, Object>) statistics.execute(new Object[] { List.of(1, 2, 3) }, null);
    assertThat(empty.keySet()).isEqualTo(full.keySet());
    assertThat(empty.get("median")).isNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  void nanIsSkippedLikeAMissingValue() {
    final List<String> items = List.of("a", "b", "c");
    Map<String, Object> r = (Map<String, Object>) min.execute(new Object[] { Arrays.asList(Double.NaN, 1, 2), items }, null);
    assertThat(r.get("value")).isEqualTo(1.0);
    assertThat(r.get("items")).isEqualTo(List.of("b"));

    r = (Map<String, Object>) max.execute(new Object[] { Arrays.asList(Double.NaN, 1, 2), items }, null);
    assertThat(r.get("value")).isEqualTo(2.0);
    assertThat(r.get("items")).isEqualTo(List.of("c"));
  }
}
