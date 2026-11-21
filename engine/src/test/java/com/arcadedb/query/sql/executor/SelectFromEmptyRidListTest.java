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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A SELECT whose target is the literal empty list {@code []} has no record to read: it answers no rows (one zero row for a
 * global aggregate), as a RID list that filtering emptied does, instead of failing with a bare {@code UnsupportedOperationException}
 * from the planner.
 */
class SelectFromEmptyRidListTest extends TestHelper {

  private List<Result> query(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().toList();
    }
  }

  @Test
  void emptyRidListIsAnEmptyTarget() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> database.newDocument("D").set("x", 1).save());

    assertThat(query("SELECT FROM []")).isEmpty();
    assertThat(query("SELECT @rid FROM []")).isEmpty();
    assertThat(query("SELECT FROM [] WHERE x = 1")).isEmpty();
    assertThat(query("SELECT FROM [] ORDER BY x LIMIT 5")).isEmpty();

    final List<Result> count = query("SELECT count(*) AS c FROM []");
    assertThat(count).hasSize(1);
    assertThat(count.getFirst().<Long>getProperty("c")).isZero();
  }
}
