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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Assertions;

import java.io.*;
import java.util.logging.*;

public class PerformanceIndexCompaction {
  public static void main(final String[] args) throws Exception {
    new PerformanceIndexCompaction().run();
  }

  private void run() throws IOException, InterruptedException {
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(5);

    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open(ComponentFile.MODE.READ_WRITE);

    final long begin = System.currentTimeMillis();
    try {
      System.out.println("Compacting all indexes...");

      final long total = database.countType("Device", true);
      final long totalIndexed = countIndexedItems(database);
      LogManager.instance().log(this, Level.INFO, "Total indexes items %d", totalIndexed);

      for (final Index index : database.getSchema().getIndexes())
        Assertions.assertTrue(((IndexInternal) index).compact());

      final long totalIndexed2 = countIndexedItems(database);

      Assertions.assertEquals(total, totalIndexed);
      Assertions.assertEquals(totalIndexed, totalIndexed2);

      System.out.println("Compaction done");

    } finally {
      database.close();
      System.out.println("Compaction finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

  }

  private long countIndexedItems(final Database database) throws IOException {
    long totalIndexed = 0;
    for (final Index index : database.getSchema().getIndexes()) {
      final IndexCursor it = ((RangeIndex) index).iterator(true);
      while (it.hasNext()) {
        it.next();
        ++totalIndexed;
      }
    }
    return totalIndexed;
  }
}
