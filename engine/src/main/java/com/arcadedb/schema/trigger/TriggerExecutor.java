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
package com.arcadedb.schema.trigger;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Interface for executing trigger actions (SQL or script-based).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface TriggerExecutor {

  /**
   * Execute the trigger action.
   *
   * @param database  The database instance
   * @param record    The current record being operated on
   * @param oldRecord The original record (for UPDATE events only, null otherwise)
   * @return true to continue the operation, false to abort it
   */
  boolean execute(Database database, Record record, Record oldRecord);

  /**
   * Execute the trigger action for a {@code BEFORE READ} trigger, which is given the RID of the record about to be
   * read and no record at all.
   * <p>
   * Separate from {@link #execute} because that timing structurally cannot supply a record: the hook fires from
   * inside {@code LocalBucket.getRecordInternal}, and materialising the record from there re-enters the very read
   * that fired it. The adapter used to do that and made every read of the triggered type fail with a
   * {@code StackOverflowError}.
   * <p>
   * The default routes to {@link #execute} with no record, so an executor that predates this method still runs its
   * body; the executors shipped here override it to bind the RID under {@code rid}/{@code $rid} instead.
   *
   * @param database The database instance where the read is occurring
   * @param rid      Identity of the record about to be read
   *
   * @return true to continue the read, false to abort it
   */
  default boolean executeBeforeRead(final Database database, final RID rid) {
    return execute(database, null, null);
  }

  /**
   * Clean up any resources held by this executor (e.g., script engines).
   */
  void close();
}
