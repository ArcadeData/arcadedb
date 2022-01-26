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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.concurrent.atomic.*;

public class PerformanceSQLInsert {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10_000_000;

  public static void main(String[] args) {
    new PerformanceSQLInsert().run();
  }

  private void run() {
    final DatabaseFactory factory = new DatabaseFactory(PerformanceTest.DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();

    if (!database.getSchema().existsType(TYPE_NAME)) {
      database.getSchema().createVertexType(TYPE_NAME);
    }

    database.async().setCommitEvery(1);
//    database.asynch().setParallelLevel(2);

    final AtomicLong oks = new AtomicLong();
    final AtomicLong errors = new AtomicLong();

    try {
      final long begin = System.currentTimeMillis();

      for (int i = 0; i < MAX_LOOPS; ++i) {
        database.async().command("SQL", "insert into " + TYPE_NAME + " set id = " + i + ", name = 'Luca'", new AsyncResultsetCallback() {
          @Override
          public void onStart(ResultSet resultset) {
            oks.incrementAndGet();
          }

          @Override
          public void onError(Exception exception) {
            errors.incrementAndGet();
          }
        });

        if (i % 100000 == 0)
          System.out.println(
              "Inserting " + MAX_LOOPS + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + " ok="
                  + oks.get() + " errors=" + errors.get() + ")");
      }

      System.out.println(
          "Inserting " + MAX_LOOPS + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + " ok="
              + oks.get() + " errors=" + errors.get() + ")");

      while (oks.get() < MAX_LOOPS) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          e.printStackTrace();
        }

        System.out.println(
            "Inserted " + MAX_LOOPS + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + " ok="
                + oks.get() + " errors=" + errors.get() + ")");
      }

    } finally {
      database.close();
    }
  }
}
