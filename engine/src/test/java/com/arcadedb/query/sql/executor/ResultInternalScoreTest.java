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
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResultInternalScoreTest {

  @Test
  void scoreProperty() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("title", "Test");
    result.setScore(0.95f);

    assertThat(result.getScore()).isEqualTo(0.95f);
    assertThat((Float) result.getProperty("$score")).isEqualTo(0.95f);
  }

  @Test
  void scoreDefaultsToZero() {
    final ResultInternal result = new ResultInternal();
    assertThat(result.getScore()).isEqualTo(0f);
  }

  @Test
  void scoreInPropertyNames() {
    final ResultInternal result = new ResultInternal();
    result.setScore(1.5f);

    assertThat(result.hasProperty("$score")).isTrue();
    assertThat(result.getPropertyNames()).contains("$score");
  }

  @Test
  void scoreNotInPropertyNamesWhenZero() {
    final ResultInternal result = new ResultInternal();

    assertThat(result.hasProperty("$score")).isTrue(); // $score is always available
    assertThat(result.getPropertyNames()).doesNotContain("$score"); // but not in names when 0
  }

  @Test
  void explicitNullScoreOverridesInternalScore() {
    // A projection that explicitly assigns $score = null must keep the null and not resurface the
    // internal scoring float (issue: vector/full-text scoring overriding explicit nulls).
    final ResultInternal result = new ResultInternal();
    result.setScore(0.95f);
    result.setProperty("$score", null);

    assertThat((Object) result.getProperty("$score")).isNull();
    // The raw internal score is still available through the dedicated accessor.
    assertThat(result.getScore()).isEqualTo(0.95f);
  }

  @Test
  void explicitNullSimilarityOverridesInternalSimilarity() {
    final ResultInternal result = new ResultInternal();
    result.setSimilarity(0.8f);
    result.setProperty("$similarity", null);

    assertThat((Object) result.getProperty("$similarity")).isNull();
    assertThat(result.getSimilarity()).isEqualTo(0.8f);
  }

  @Test
  void scoreFallbackStillAppliesWhenKeyAbsent() {
    // When $score is not projected at all, getProperty falls back to the internal score field.
    final ResultInternal result = new ResultInternal();
    result.setProperty("title", "Test");
    result.setScore(0.42f);

    assertThat((Float) result.getProperty("$score")).isEqualTo(0.42f);
  }

  @Test
  void explicitNonNullScoreOverridesInternalScore() {
    final ResultInternal result = new ResultInternal();
    result.setScore(0.95f);
    result.setProperty("$score", 0.10f);

    assertThat((Float) result.getProperty("$score")).isEqualTo(0.10f);
  }

  @Test
  void removedScoreIsNotResurrectedByInternalScore() {
    // Explicitly removing $score must keep it absent (null), not fall back to the internal scoring float.
    final ResultInternal result = new ResultInternal();
    result.setScore(0.95f);
    result.setProperty("$score", 0.10f);
    result.removeProperty("$score");

    assertThat((Object) result.getProperty("$score")).isNull();
    // The raw internal score is still available through the dedicated accessor.
    assertThat(result.getScore()).isEqualTo(0.95f);
  }
}
