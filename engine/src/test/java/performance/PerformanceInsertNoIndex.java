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
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.util.logging.Level;

public class PerformanceInsertNoIndex extends TestHelper {
  private static final int    TOT       = 10_000_000;
  private static final String TYPE_NAME = "Person";
  private static final int    PARALLEL  = 3;

  public static void main(String[] args) {
    PerformanceTest.clean();
    new PerformanceInsertNoIndex().run();
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

      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, PARALLEL);

      type.createProperty("id", Long.class);
      type.createProperty("name", String.class);
      type.createProperty("surname", String.class);
      type.createProperty("counter", Integer.class);

      database.commit();

      long begin = System.currentTimeMillis();

      try {

        database.setReadYourWrites(false);
        database.async().setParallelLevel(PARALLEL);
        database.async().setTransactionUseWAL(false);
        database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);
        database.async().setCommitEvery(5000);
        database.async().onError(new ErrorCallback() {
          @Override
          public void call(Throwable exception) {
            LogManager.instance().log(this, Level.SEVERE, "ERROR: " + exception, exception);
            System.exit(1);
          }
        });

        long counter = 0;
        for (; counter < TOT; ++counter) {
          final MutableDocument record = database.newDocument(TYPE_NAME);

          record.set("id", counter);
          record.set("name", "Luca" + counter);
          record.set("surname", "Skywalker" + counter);
          record.set("counter", 10);

          database.async().createRecord(record, null);

          if (counter % 1000000 == 0)
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
