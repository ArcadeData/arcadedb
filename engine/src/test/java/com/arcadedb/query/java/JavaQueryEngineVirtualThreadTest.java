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
package com.arcadedb.query.java;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Test for Java Query Engine with Virtual Threads support.
 * Tests both virtual thread and traditional platform thread execution.
 */
public class JavaQueryEngineVirtualThreadTest extends TestHelper {

  @Test
  public void testVirtualThreadsEnabled() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_ENGINES_USE_VIRTUAL_THREADS, true);

    database.getQueryEngine("java").registerFunctions(JavaMethods.class.getName() + "::sum");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);
  }

  @Test
  public void testPlatformThreadsEnabled() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_ENGINES_USE_VIRTUAL_THREADS, false);

    database.getQueryEngine("java").registerFunctions(JavaMethods.class.getName() + "::sum");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);
  }

  @Test
  public void testConcurrentExecutionWithVirtualThreads() throws InterruptedException {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_ENGINES_USE_VIRTUAL_THREADS, true);

    database.getQueryEngine("java").registerFunctions(JavaMethods.class.getName() + "::sum");

    // Execute multiple concurrent queries
    final CountDownLatch latch = new CountDownLatch(10);
    final ExecutorService executor = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 10; i++) {
      final int index = i;
      executor.submit(() -> {
        try {
          final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", index, index * 2);
          assertThat(result.hasNext()).isTrue();
          assertThat((Integer) result.next().getProperty("value")).isEqualTo(index + index * 2);
        } finally {
          latch.countDown();
        }
      });
    }

    try {
      assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testConcurrentExecutionWithPlatformThreads() throws InterruptedException {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_ENGINES_USE_VIRTUAL_THREADS, false);

    database.getQueryEngine("java").registerFunctions(JavaMethods.class.getName() + "::sum");

    // Execute multiple concurrent queries
    final CountDownLatch latch = new CountDownLatch(10);
    final ExecutorService executor = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 10; i++) {
      final int index = i;
      executor.submit(() -> {
        try {
          final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", index, index * 2);
          assertThat(result.hasNext()).isTrue();
          assertThat((Integer) result.next().getProperty("value")).isEqualTo(index + index * 2);
        } finally {
          latch.countDown();
        }
      });
    }

    try {
      assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdown();
    }
  }
}
