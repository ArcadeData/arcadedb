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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.data.DatasetGenerator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

class FullTextBroadTest {
  private static final long SEED = 42L;

  @Test
  void broadQueriesMatchAbout45Percent() {
    final int posts = FullTextWorkload.posts(Scale.XS);
    final List<Set<String>> bodies = new ArrayList<>(posts);
    for (int i = 0; i < posts; i++)
      bodies.add(new HashSet<>(Arrays.asList(FullTextWorkload.postBody(SEED, i).split(" "))));
    final SplittableRandom random = new SplittableRandom(7);
    double sum = 0;
    final int queries = 50;
    for (int q = 0; q < queries; q++) {
      final String[] terms = DatasetGenerator.broadQuery(random).split(" OR ");
      assertThat(terms.length).isGreaterThan(1);
      int matching = 0;
      for (final Set<String> body : bodies)
        for (final String term : terms)
          if (body.contains(term)) {
            matching++;
            break;
          }
      sum += (double) matching / posts;
    }
    assertThat(sum / queries).isBetween(0.40, 0.50);
  }

  @Test
  void deletedShareIsTenPercent() {
    final int posts = 20_000;
    int deleted = 0;
    for (int i = 0; i < posts; i++)
      if (DatasetGenerator.broadDeleted(SEED, i))
        deleted++;
    assertThat((double) deleted / posts).isBetween(0.08, 0.12);
  }

  @Test
  void existingCasesKeepTheirOrder() {
    final List<String> cases = new FullTextWorkload().cases();
    assertThat(cases.subList(0, 5)).containsExactly("build", "bm25-single-term", "bm25-multi-term", "bm25-phrase", "hybrid");
    assertThat(cases.subList(5, cases.size())).containsExactly("bm25-broad-or", "bm25-broad-or-filtered");
  }

  @Test
  void sSizeCoversTheRuleInputs() {
    // 1,000 queries per rep at S; the corpus must be big enough for every query to return its 50 rows after the filter
    assertThat(FullTextWorkload.QUERIES_PER_REP).isGreaterThanOrEqualTo(1_000);
    assertThat(FullTextWorkload.posts(Scale.S)).isGreaterThan(1_000);
  }

  @Test
  void sizingModelFitsTheStepAndCaseCaps() {
    // a sizing model, not a measurement of the run: the filtered case costs about 0.5 ms per 1,000 posts per query, a constant
    // measured at S only and extrapolated to M and L; 7 steps (2 warm-ups + 5 reps)
    for (final Scale scale : Scale.values()) {
      final double stepSeconds = 0.0005 * FullTextWorkload.posts(scale) / 1000.0 * FullTextWorkload.broadQueries(scale) / 1000.0 * 1000.0;
      assertThat(stepSeconds).as(scale + " step seconds").isLessThan(30 * 60);
      assertThat(7 * stepSeconds).as(scale + " case seconds").isLessThan(60 * 60);
    }
    assertThat(FullTextWorkload.broadQueries(Scale.S)).isGreaterThanOrEqualTo(1_000);
  }
}
