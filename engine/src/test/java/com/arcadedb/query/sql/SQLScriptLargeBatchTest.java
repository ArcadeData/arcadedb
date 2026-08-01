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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A batch script chains one execution step per statement, so releasing the plan must not walk the
 * chain recursively: a large import would blow the stack.
 */
public class SQLScriptLargeBatchTest extends TestHelper {
  private static final int STATEMENTS = 20_000;

  @Test
  void closingALargeBatchScriptDoesNotOverflowTheStack() throws InterruptedException {
    database.command("sql", "CREATE DOCUMENT TYPE ImportRow");

    final StringBuilder script = new StringBuilder("begin;\n");
    for (int i = 0; i < STATEMENTS; i++)
      script.append("INSERT INTO ImportRow SET id = ").append(i).append(";\n");
    script.append("commit;\n");

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    // run on a thread with an explicit stack size so the outcome does not depend on the JVM default
    final Thread runner = new Thread(null, () -> {
      try (final ResultSet rs = database.command("sqlscript", script.toString())) {
        while (rs.hasNext())
          rs.next();
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "large-batch", 1024 * 1024);

    runner.start();
    runner.join();

    assertThat(failure.get()).isNull();
    // count(@rid) scans: countType() would read the cached bucket counter instead of ground truth
    try (final ResultSet counted = database.query("sql", "SELECT count(@rid) AS total FROM ImportRow")) {
      assertThat(counted.next().<Long>getProperty("total")).isEqualTo(STATEMENTS);
    }
  }
}
