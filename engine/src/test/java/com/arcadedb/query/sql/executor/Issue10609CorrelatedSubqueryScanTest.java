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
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10609: a subquery that reads {@code $parent.$current} and is run once per row of the enclosing scan used to scan in
 * parallel itself, so its workers read the very record the enclosing thread was holding, from several threads at once. A record
 * is not shared between threads (its binary content has a read position), and the workers logged thousands of
 * {@code Possible corrupted record} errors and compared rows against nothing: the answer of the statement depended on the
 * timing of the threads. A statement run under a row of an enclosing statement scans on its own thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10609CorrelatedSubqueryScanTest extends TestHelper {
  private static final int ROWS = 2_000;

  @Test
  void aCorrelatedSubqueryCountsTheSameRowsWhicheverThreadsRunIt() {
    database.getSchema().createDocumentType("Doc", 2);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Doc").set("n", i).save();
    });

    // rows whose residue modulo 7 is shared by `perResidue` rows, brute-forced here
    final int[] members = new int[7];
    for (int i = 0; i < ROWS; i++)
      members[i % 7]++;
    final int target = members[0];
    int expected = 0;
    for (int residue = 0; residue < 7; residue++)
      if (members[residue] == target)
        expected += members[residue];

    try (final var rs = database.query("sql",
        "SELECT count(*) AS c FROM Doc WHERE (SELECT count(*) AS c FROM Doc d WHERE d.n % 7 = $parent.$current.n % 7)[0].c = ?",
        target)) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo((long) expected);
    }
  }

  @Test
  void aStatementRunUnderARowOfAnEnclosingStatementDoesNotScanInParallel() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final BasicCommandContext outer = new BasicCommandContext();
    outer.setDatabase(db);
    assertThat(ParallelTypeScan.isAllowed(outer)).as("a statement of its own").isTrue();

    final BasicCommandContext inner = new BasicCommandContext();
    inner.setDatabase(db);
    inner.setParent(outer);
    assertThat(ParallelTypeScan.isAllowed(inner)).as("a FROM-subquery or a LET of the whole statement: no row yet").isTrue();

    outer.setVariable("current", new ResultInternal());
    assertThat(ParallelTypeScan.isAllowed(inner)).as("run once per row of the enclosing scan").isFalse();

    final BasicCommandContext deeper = new BasicCommandContext();
    deeper.setDatabase(db);
    deeper.setParent(inner);
    assertThat(ParallelTypeScan.isAllowed(deeper)).as("a row anywhere up the chain").isFalse();
  }
}
