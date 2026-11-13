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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code TRUNCATE} that runs in its caller's transaction deletes what its scan found, one record at a time. A record a
 * concurrent transaction deleted and committed after the scan found it is already gone, which is what the truncate wants:
 * it used to fail the whole statement with a {@code RecordNotFoundException} for a record the statement never named
 * (issue #11731).
 */
class TruncateConcurrentDeleteIssue11731Test extends TestHelper {

  @Test
  void aRecordAnotherTransactionDeletedAfterTheScanIsSkipped() throws InterruptedException {
    database.transaction(() -> database.getSchema().createDocumentType("T"));
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < rids.length; i++)
        rids[i] = database.newDocument("T").set("v", i).save().getIdentity();
    });

    database.begin();
    final List<Record> found = new ArrayList<>();
    database.scanType("T", false, record -> found.add(record));
    assertThat(found).hasSize(3);

    // ANOTHER TRANSACTION DELETES ONE OF THE RECORDS THE SCAN FOUND AND COMMITS, BEFORE THE TRUNCATE REACHES IT
    final Thread concurrent = new Thread(() -> database.transaction(() -> rids[1].asDocument().delete()));
    concurrent.start();
    concurrent.join();

    final RuntimeException failure = TruncateRecordDeleter.deleteAll(database, 0, callback -> {
      for (final Record record : found)
        if (!callback.test(record))
          return;
    });
    assertThat(failure).isNull();
    database.commit();

    assertThat(database.countType("T", false)).isZero();
  }
}
