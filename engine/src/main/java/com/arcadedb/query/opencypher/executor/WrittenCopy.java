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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;

/**
 * The copy of a record the current transaction wrote, when it wrote one.
 * <p>
 * A row holds the record object it was given when it was produced. A write to the same record made after that, through
 * another row, goes to the transaction's own working copy and leaves the object the row holds as it was. A lookup of the RID
 * is answered from the transaction first; this lines a held object up with that answer without a lookup, and only with a copy
 * the transaction WROTE: a read-only cached copy never replaces what the row holds (issue #11534).
 * <p>
 * Costs two size reads on a transaction that has written nothing, and one hash lookup per record otherwise.
 */
public final class WrittenCopy {
  private WrittenCopy() {
  }

  /**
   * @return the current transaction's written copy of {@code document} when it holds one, otherwise {@code document} itself
   */
  public static Document of(final Document document) {
    if (!(document instanceof BaseRecord record) || !(record.getDatabase() instanceof DatabaseInternal database))
      return document;
    final RID rid = document.getIdentity();
    if (rid == null || !rid.isValid())
      return document;
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx == null || !tx.hasWrittenRecords())
      return document;
    return tx.getWrittenRecord(rid) instanceof Document written ? written : document;
  }
}
