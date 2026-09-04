/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7143 (second item): {@code CREATE ... IF NOT EXISTS} answered one row the first
 * time and ZERO rows on a retry, so a client that checks the row count to confirm success read the retry as a
 * failure - which is exactly what the guard exists to prevent. {@code CREATE INDEX}, {@code CREATE TRIGGER}
 * and {@code CREATE GRAPH ANALYTICAL VIEW} already answered one row carrying a {@code created} flag; every
 * guarded CREATE now follows that convention.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7143CreateIfNotExistsRowShapeTest extends TestHelper {

  @Test
  void everyGuardedCreateAnswersOneRowWithACreatedFlagOnBothCalls() {
    assertCreatedThenIdempotent("create document type Doc1 if not exists", "typeName", "Doc1");
    assertCreatedThenIdempotent("create vertex type Vert1 if not exists", "typeName", "Vert1");
    assertCreatedThenIdempotent("create edge type Edge1 if not exists", "typeName", "Edge1");
    assertCreatedThenIdempotent("create bucket Bucket1 if not exists", "bucketName", "Bucket1");
    assertCreatedThenIdempotent("create property Doc1.name if not exists string", "propertyName", "name");
    assertCreatedThenIdempotent(
        "create timeseries type Series1 if not exists timestamp ts tags (sensor string) fields (value double)",
        "typeName", "Series1");
    assertCreatedThenIdempotent("create index if not exists on Doc1 (name) unique", "name", "Doc1[name]");
  }

  /**
   * Runs {@code statement} twice and asserts both calls answer exactly one row naming the same object, the
   * first with {@code created=true} and the second with {@code created=false}.
   */
  private void assertCreatedThenIdempotent(final String statement, final String nameProperty,
      final String expectedName) {
    final Result first = single(statement);
    assertThat(first.<Boolean>getProperty("created")).as("%s (first call)", statement).isTrue();
    assertThat(first.<Object>getProperty(nameProperty)).as("%s (first call)", statement).isEqualTo(expectedName);

    final Result second = single(statement);
    assertThat(second.<Boolean>getProperty("created")).as("%s (retry)", statement).isFalse();
    assertThat(second.<Object>getProperty(nameProperty)).as("%s (retry)", statement).isEqualTo(expectedName);
    assertThat(second.<Object>getProperty("operation")).as("%s (retry)", statement)
        .isEqualTo(first.getProperty("operation"));
  }

  private Result single(final String statement) {
    try (final ResultSet rs = database.command("sql", statement)) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).as("row count of: %s", statement).hasSize(1);
      return rows.getFirst();
    }
  }
}
