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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10379: the writes a statement gives back when it takes back a record it created have a floor, what the transaction had
 * counted when the statement started, so a wrong count of the caller can never lower the safety counter under it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10379RetractionFloorTest extends TestHelper {

  @Test
  void aRetractionNeverTakesTheCounterBelowWhereTheStatementStarted() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.v INTEGER");

    database.begin();
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      database.command("sql", "INSERT INTO T SET v = 1").close();
      final long afterTheCallersWrite = tx.getCompletedRecordWrites();
      assertThat(afterTheCallersWrite).isPositive();

      // a statement that started here and counted two writes can give two back, and never more than it counted
      tx.countCompletedRecordWrite();
      tx.countCompletedRecordWrite();
      tx.retractCompletedRecordWrites(2L, afterTheCallersWrite);
      assertThat(tx.getCompletedRecordWrites()).isEqualTo(afterTheCallersWrite);
      tx.retractCompletedRecordWrites(100L, afterTheCallersWrite);
      assertThat(tx.getCompletedRecordWrites()).isEqualTo(afterTheCallersWrite);
    } finally {
      database.rollback();
    }
  }
}
