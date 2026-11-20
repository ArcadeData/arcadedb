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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

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

  /**
   * Whether a command can hold only current records ({@link BasicCommandContext#isCurrentRecordsOnly()}): it writes nothing,
   * and it is a top-level command or nested in one that holds only current records.
   *
   * @param readOnly     whether the command writes nothing
   * @param outerContext the context of the enclosing command, null for a top-level one
   */
  public static boolean holdsCurrentRecordsOnly(final boolean readOnly, final CommandContext outerContext) {
    return readOnly && (outerContext == null || outerContext instanceof BasicCommandContext outer && outer.isCurrentRecordsOnly());
  }

  /**
   * Points every record the rows hold - their values, at any depth of a list, a map or a path, and the element a single node
   * or relationship is answered as - at the copy the transaction wrote. Called where a step hands out rows it read to the end
   * first (an aggregation, a sort, the eager barrier): every row before it has made its writes by then, so a record a row held
   * from before a later row wrote it is read as that write left it, as Neo4j reads it after the same pipeline breaker (issue
   * #12565).
   * <p>
   * Only there, not at every property read: the write steps run their rows in batches, so a read made at read time downstream
   * of one would see the writes of the rows after it in the batch, which a streaming plan has not made yet (issue #6362). A
   * row between two breakers reads what it holds.
   * <p>
   * Free for a command that holds only current records, and when the transaction has written nothing.
   *
   * @param context the context of the command the rows belong to
   * @param rows    the rows the step is about to hand out
   */
  public static void resolveRows(final CommandContext context, final Iterable<? extends Result> rows) {
    final TransactionContext tx = writtenTransaction(context);
    if (tx == null)
      return;
    for (final Result row : rows) {
      if (!(row instanceof ResultInternal result))
        continue;
      for (final String name : result.getPropertyNames()) {
        final Object value = result.getProperty(name);
        final Object live = RowAliases.liveValue(value, WrittenCopy::of);
        if (live != value) // NOPMD: liveValue answers the value itself, by identity, when nothing in it was stale
          result.setProperty(name, live);
      }
      if (result.isElement()) {
        final Document element = result.getElement().orElse(null);
        final Document live = element != null ? of(element) : null;
        if (live != element) // NOPMD: by identity, as of() answers an unchanged record
          result.setElement(live);
      }
    }
  }

  /**
   * {@link #resolveRows} for one value an aggregation answers before an expression around it reads it: the records
   * {@code collect(h)} gathered are read by {@code head(collect(h)).v} as the writes of every row left them (issue #12565).
   *
   * @return {@code value}, or a copy of it holding the written copies of the records it holds
   */
  public static Object resolveValue(final Object value, final CommandContext context) {
    return value == null || writtenTransaction(context) == null ? value : RowAliases.liveValue(value, WrittenCopy::of);
  }

  /** The transaction of the command, when the command can hold a record older than a copy the transaction wrote. */
  private static TransactionContext writtenTransaction(final CommandContext context) {
    if (context == null || context instanceof BasicCommandContext basic && basic.isCurrentRecordsOnly())
      return null;
    final DatabaseInternal database = context.getDatabase();
    final TransactionContext tx = database != null ? database.getTransactionIfExists() : null;
    return tx != null && tx.hasWrittenRecords() ? tx : null;
  }
}
