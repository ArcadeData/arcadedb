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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;
import performance.PerformanceTest;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class PageManagerStressTest {
  private static final int    TOT       = 1_000_000;
  private static final String TYPE_NAME = "Device";

  @Test
  public void stressPageManagerFlush() {
    PerformanceTest.clean("low-ram");

    final int parallel = 2;

    Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        final DocumentType v = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(parallel).create();

        v.createProperty("id", Long.class);
        v.createProperty("name", String.class);
        v.createProperty("surname", String.class);
        v.createProperty("locali", Integer.class);
        v.createProperty("notes1", String.class);

        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "id" }, 5000000);

        database.commit();
      }
    } finally {
      database.close();
    }

    database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    final long begin = System.currentTimeMillis();

    try {

      database.setReadYourWrites(false);
      database.async().setCommitEvery(5000);
      database.async().setParallelLevel(parallel);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);

      database.async().onError(exception -> {
        System.out.println("ERROR: " + exception);
        exception.printStackTrace();
      });

      long row = 0;
      for (; row < TOT; ++row) {
        final MutableDocument record = database.newDocument(TYPE_NAME);

        final String randomString = UUID.randomUUID().toString();

        record.set("id", row);
        record.set("name", randomString);
        record.set("surname", randomString);
        record.set("locali", 10);
        record.set("notes1",
            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");
        record.set("notes2",
            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");

        database.async().createRecord(record, null);
      }

      final PageManager.PPageManagerStats stats = ((DatabaseInternal) database).getPageManager().getStats();
      assertThat(stats.evictionRuns > 0).isTrue();
      assertThat(stats.pagesEvicted > 0).isTrue();

    } finally {
      database.close();
    }

  }
}
