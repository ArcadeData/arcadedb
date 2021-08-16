/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package performance;

import com.arcadedb.database.*;

import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceScan {
  private static final String USERTYPE_NAME = "Person";
  private static final int    MAX_LOOPS  = 10;

  public static void main(String[] args) throws Exception {
    new PerformanceScan().run();
  }

  private void run() {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    database.async().setParallelLevel(4);

    try {
      for (int i = 0; i < MAX_LOOPS; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        database.async().scanType(USERTYPE_NAME, true, new DocumentCallback() {
          @Override
          public boolean onRecord(final Document record) {
            final ImmutableDocument document = ((ImmutableDocument) record);

            document.get("id");

            if (row.incrementAndGet() % 10000000 == 0)
              System.out.println("- Scanned " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms");

            return true;
          }
        });

        System.out.println("Found " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }
    } finally {
      database.close();
    }
  }
}
