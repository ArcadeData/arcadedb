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
package com.arcadedb.containers.ha.facet;

import com.arcadedb.test.support.PrometheusScrape;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusScrapeTest {
  private static final String BODY = """
      # HELP arcadedb_columnar_rebuilds_total
      # TYPE arcadedb_columnar_rebuilds_total counter
      arcadedb_columnar_rebuilds_total{database="chaos",reason="create",type="FacetAsset",} 1.0
      arcadedb_columnar_rebuilds_total{database="chaos",reason="snapshot_install",type="FacetAsset",} 3.0
      arcadedb_columnar_rebuilds_total_extra 9.0
      arcadedb_columnar_segments{database="chaos",} 4.0
      """;

  @Test
  void sumsEverySeriesOfExactlyThatName() {
    assertThat(PrometheusScrape.sum(BODY, "arcadedb_columnar_rebuilds_total")).isEqualTo(4.0);
  }

  @Test
  void filtersByLabel() {
    assertThat(PrometheusScrape.sum(BODY, "arcadedb_columnar_rebuilds_total", "reason", "snapshot_install")).isEqualTo(3.0);
  }

  @Test
  void labelValueIsMatchedWholeNotAsPrefix() {
    assertThat(PrometheusScrape.sum(BODY, "arcadedb_columnar_rebuilds_total", "reason", "snapshot")).isZero();
  }

  @Test
  void absentNameIsZero() {
    assertThat(PrometheusScrape.sum(BODY, "arcadedb_columnar_nope")).isZero();
  }

  @Test
  void unlabelledSample() {
    assertThat(PrometheusScrape.sum("x_total 2.5\n", "x_total")).isEqualTo(2.5);
  }
}
