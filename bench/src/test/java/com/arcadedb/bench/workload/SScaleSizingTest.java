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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The S sizing rule of the phase 2 design (#12433): every case at S measures something. A timed operation takes at least 5 s per
 * repetition, a latency case runs at least 1,000 operations per repetition. A case that cannot meet it at S is left out at S
 * rather than measured as noise.
 */
class SScaleSizingTest {
  private static final int MIN_OPS_PER_REP = 1_000;

  @Test
  void bulkImportIsLeftOutAtSAndRunsAtEveryOtherScale() {
    // 2M rows import in 4.5 s on the JVM at M (2026-11-23): S would need more rows than M to reach 5 s
    final IngestionWorkload w = new IngestionWorkload();
    assertThat(w.cases(Scale.S)).containsExactly(IngestionWorkload.TX_1T, IngestionWorkload.TX_8T);
    for (final Scale scale : new Scale[] { Scale.XS, Scale.M, Scale.L })
      assertThat(w.cases(scale)).isEqualTo(w.cases());
    assertThat(w.cases()).contains(IngestionWorkload.BULK);
  }

  @Test
  void txInsertRecordsAThousandBatchesPerRepAtS() {
    assertThat(IngestionWorkload.rows(Scale.S) / IngestionWorkload.BATCH).isGreaterThanOrEqualTo(MIN_OPS_PER_REP);
  }

  @Test
  void vectorQueriesAndBuildAreSizedForS() {
    assertThat(VectorWorkload.queryCount(Scale.S)).isGreaterThanOrEqualTo(MIN_OPS_PER_REP);
    // 20k vectors built in 1.0 s on the JVM at S (2026-11-26) and 200k in 15.4 s at M: 80k is the smallest round size over 5 s
    assertThat(VectorWorkload.vectors(Scale.S)).isGreaterThanOrEqualTo(80_000);
  }

  @Test
  void facetQueriesRunAThousandPerRepAtS() {
    assertThat(FacetWorkload.queriesPerRep(Scale.S)).isGreaterThanOrEqualTo(MIN_OPS_PER_REP);
  }

  @Test
  void graphAndFullTextQueriesRunAThousandPerRep() {
    assertThat(GraphOltpWorkload.QUERIES_PER_REP).isGreaterThanOrEqualTo(MIN_OPS_PER_REP);
    assertThat(FullTextWorkload.QUERIES_PER_REP).isGreaterThanOrEqualTo(MIN_OPS_PER_REP);
  }
}
