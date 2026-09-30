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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7754 review follow-up: a schemaless column holding both a boolean and a timestamp must still be sortable and
 * filterable, because the refusal of the BOOLEAN-versus-timestamp pair must not turn into a query failure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7754MixedBooleanTemporalQueryTest extends TestHelper {
  @Test
  void orderByAndComparisonOnAMixedColumnDoNotFail() {
    database.getSchema().createDocumentType("Mixed");
    database.transaction(() -> {
      database.newDocument("Mixed").set("v", true).save();
      database.newDocument("Mixed").set("v", LocalDateTime.of(2026, 1, 1, 0, 0)).save();
      database.newDocument("Mixed").set("v", false).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM Mixed ORDER BY v")) {
      assertThat(rs.stream().count()).isEqualTo(3L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM Mixed WHERE v > ?", LocalDateTime.of(2025, 1, 1, 0, 0))) {
      assertThat(rs.stream().count()).isLessThanOrEqualTo(3L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM Mixed WHERE v < true")) {
      assertThat(rs.stream().count()).isLessThanOrEqualTo(3L);
    }
  }
}
