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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.*;

public class PerformanceParsing {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10000000;

  public static void main(String[] args) throws Exception {
    new PerformanceParsing().run();
  }

  private void run() {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    if (!database.getSchema().existsType(TYPE_NAME)) {
      database.getSchema().createVertexType(TYPE_NAME);
      database.begin();
      final MutableVertex v = database.newVertex(TYPE_NAME);
      v.set("name", "test");
      database.commit();
    }

    database.async().setParallelLevel(4);

    final AtomicLong ok = new AtomicLong();
    final AtomicLong error = new AtomicLong();

    try {
      final long begin = System.currentTimeMillis();

      for (int i = 0; i < MAX_LOOPS; ++i) {

        database.async().query("SQL", "select from " + TYPE_NAME + " limit 1", new AsyncResultsetCallback() {
          @Override
          public void onStart(final ResultSet rs) {
            ok.incrementAndGet();

            while (rs.hasNext()) {
              Result record = rs.next();
              Assertions.assertNotNull(record);
            }
          }

          @Override
          public void onError(Exception exception) {
            error.incrementAndGet();
          }
        });
      }

      System.out.println("Executed " + MAX_LOOPS + " simple queries in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();

      Assertions.assertEquals(MAX_LOOPS, ok.get());
      Assertions.assertEquals(0, error.get());
    }
  }
}
