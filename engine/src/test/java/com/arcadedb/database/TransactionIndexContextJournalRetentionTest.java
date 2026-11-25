/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The journal that takes a failed statement back is reused across the records of a statement, and a bulk statement grows it by one
 * holder per index entry it queues. The holders must not outlive the statement: a thread that ran one large statement kept them for
 * as long as it lived, and a pool of such threads kept millions of empty holders on the heap (a 500-fact bulk write of Graphiti left
 * 3.3 million, 215 MB, live in a 2.5 GB heap).
 */
class TransactionIndexContextJournalRetentionTest extends TestHelper {

  @Test
  void aBulkStatementDoesNotLeaveItsJournalBehind() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE PROPERTY V.a STRING").close();
    database.command("sql", "CREATE PROPERTY V.b STRING").close();
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON V (b) NOTUNIQUE").close();

    database.begin();
    final TransactionIndexContext indexChanges = ((TransactionContext) ((DatabaseInternal) database).getTransaction()).getIndexChanges();
    // one statement that queues thousands of index entries, in a transaction the caller keeps
    database.command("opencypher", "UNWIND range(1, 3000) AS i CREATE (:V {a: 'a' + i, b: 'b' + i})").close();

    assertThat(indexChanges.recordUndoHolders()).as("holders kept after the statement")
        .isLessThanOrEqualTo(TransactionIndexContext.RECORD_UNDO_RETAINED);
    database.commit();
    assertThat(indexChanges.recordUndoHolders()).as("holders kept after the commit")
        .isLessThanOrEqualTo(TransactionIndexContext.RECORD_UNDO_RETAINED);
  }

  /**
   * A statement that queues more than the journal may hold drops the holders of the records it already indexed, which leaves few holders
   * in an array as long as the limit: the slots of the array are heap too, and go with the statement like the holders do.
   */
  @Test
  void aStatementThatOutgrewTheJournalLimitDoesNotLeaveTheArrayOfItBehind() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE PROPERTY V.a STRING").close();
    database.command("sql", "CREATE PROPERTY V.b STRING").close();
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON V (b) NOTUNIQUE").close();
    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
        4 * TransactionIndexContext.RECORD_UNDO_RETAINED);

    database.begin();
    final TransactionIndexContext indexChanges = ((TransactionContext) ((DatabaseInternal) database).getTransaction()).getIndexChanges();
    // 6,000 entries in one statement, past the limit of 1,024: the journal is abandoned part way
    database.command("opencypher", "UNWIND range(1, 3000) AS i CREATE (:V {a: 'a' + i, b: 'b' + i})").close();

    assertThat(indexChanges.recordUndoCapacity()).as("slots kept after the statement")
        .isLessThanOrEqualTo(TransactionIndexContext.RECORD_UNDO_RETAINED);
    database.commit();
    assertThat(indexChanges.recordUndoCapacity()).as("slots kept after the commit")
        .isLessThanOrEqualTo(TransactionIndexContext.RECORD_UNDO_RETAINED);
  }
}
