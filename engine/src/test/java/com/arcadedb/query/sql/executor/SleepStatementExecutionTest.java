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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
class SleepStatementExecutionTest extends TestHelper {
  @Test
  void basic() {
    // Monotonic clock: Thread.sleep is measured against it, while the wall clock can be adjusted during the sleep and
    // report a 1000 ms sleep as 957 ms (seen on a full engine run)
    final long begin = System.nanoTime();
    final ResultSet result = database.command("sql", "sleep 1000");
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin)).isGreaterThanOrEqualTo(1000L);
    //printExecutionPlan(null, result);
//    assertThat(result).isNotNull();
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("operation")).isEqualTo("sleep");
    assertThat(result.hasNext()).isFalse();
  }
}
