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

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final fix batch 2 (r1 M3): only a collect step that opened holds anything an abandoned result set could keep, so only such a step is
 * registered with the cleaner. The plan cache's templates and their copies, built for every execution and EXPLAIN, are not.
 */
class FacetCollectStepCleanerTest extends TestHelper {
  private static FacetCollectStep collectStep(final ResultSet rs) {
    for (final ExecutionStep step : rs.getExecutionPlan().orElseThrow().getSteps())
      if (step instanceof FacetCollectStep collect)
        return collect;
    throw new AssertionError("No collect step in the plan");
  }

  @Test
  void onlyAnOpenedStepIsRegisteredWithTheCleaner() {
    database.command("sql", "CREATE DOCUMENT TYPE Product");
    database.command("sql", "CREATE PROPERTY Product.brand STRING");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Product").set("brand", "brand-" + (i % 3)).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM Product FACETS brand TERMS")) {
      // the result set fetched its first rows: the step opened
      final FacetCollectStep step = collectStep(rs);
      assertThat(step.registeredWithCleaner()).as("opened").isTrue();
      final FacetCollectStep copy = (FacetCollectStep) step.copy(null);
      assertThat(copy.registeredWithCleaner()).as("a copy for another execution, not opened").isFalse();
      assertThat(rs.getFacets()).isNotNull();
    }
  }
}
