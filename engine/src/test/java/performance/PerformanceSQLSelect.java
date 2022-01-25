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
package performance;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceSQLSelect extends TestHelper {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10;

  public static void main(String[] args) {
    new PerformanceSQLSelect().run();
  }

  @Override
  protected String getDatabasePath() {
    return PerformanceTest.DATABASE_PATH;
  }

  private void run() {
    database.async().setParallelLevel(4);

    try {
      for (int i = 0; i < MAX_LOOPS; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        final ResultSet rs = database.command("SQL", "select from " + TYPE_NAME + " where id < 1l");
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((long) record.getProperty("id") < 1);
          row.incrementAndGet();
        }

        System.out
            .println("Found " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + ")");
      }
    } finally {
      database.close();
    }
  }
}
