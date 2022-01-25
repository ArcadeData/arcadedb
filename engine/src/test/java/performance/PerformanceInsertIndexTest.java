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

import com.arcadedb.NullLogger;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.logging.*;

public class PerformanceInsertIndexTest extends TestHelper {
  private static final int    TOT       = 100_000_000;
  private static final String TYPE_NAME = "User";
  private static final int    PARALLEL  = 3;

  public static void main(String[] args) {
//    GlobalConfiguration.FLUSH_ONLY_AT_CLOSE.setValue(true);
//    GlobalConfiguration.PAGE_FLUSH_QUEUE.setValue(100000);
//    GlobalConfiguration.MAX_PAGE_RAM.setValue(16 * 1024);

    PerformanceTest.clean();
    new PerformanceInsertIndexTest().run();
  }

  @Override
  protected String getDatabasePath() {
    return PerformanceTest.DATABASE_PATH;
  }

  @Override
  protected String getPerformanceProfile() {
    LogManager.instance().setLogger(NullLogger.INSTANCE);

    return "high-performance";
  }

  private void run() {
    database.setReadYourWrites(false);
    if (!database.getSchema().existsType(TYPE_NAME)) {
      database.begin();

      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, PARALLEL, Bucket.DEF_PAGE_SIZE);

      type.createProperty("id", Long.class);
      type.createProperty("area", String.class);
      type.createProperty("age", Byte.class);
      type.createProperty("active", Byte.class);

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "id" }, 5000000);

      database.commit();

      long begin = System.currentTimeMillis();

      try {

        database.setReadYourWrites(false);
        database.async().setParallelLevel(PARALLEL);
        database.async().setTransactionUseWAL(false);
        database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);
        database.async().setCommitEvery(10_000);
        database.async().onError(new ErrorCallback() {
          @Override
          public void call(Throwable exception) {
            LogManager.instance().log(this, Level.SEVERE, "ERROR: " + exception, exception);
            System.exit(1);
          }
        });

        int counter = 0;
        for (; counter < TOT; ++counter) {
          final MutableDocument record = database.newDocument(TYPE_NAME);

          record.set("id", counter);
          record.set("area", "78746");
          record.set("age", (byte) 44);
          record.set("active", (byte) 0);

          database.async().createRecord(record, null);

          if (counter % 1_000_000 == 0)
            System.out.println("Written " + counter + " elements in " + (System.currentTimeMillis() - begin) + "ms");
        }

        System.out.println("Inserted " + counter + " elements in " + (System.currentTimeMillis() - begin) + "ms");

      } finally {
        database.async().waitCompletion();
        final long elapsed = System.currentTimeMillis() - begin;
        System.out.println("Insertion finished in " + elapsed + "ms -> " + (TOT / (elapsed / 1000F)) + " ops/sec");
      }
    }

    database.close();
  }
}
