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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;

/**
 * The heap limits the MongoDB read paths count their buffers against: the elements one buffer may hold
 * ({@code arcadedb.queryMaxHeapElementsAllowedPerOp}) and the heap the buffers of all the queries share
 * ({@code arcadedb.queryMaxHeapRAM}), as the SQL and openCypher operations that buffer rows count theirs. A buffer that is built to be
 * answered (the documents of a legacy query, of an aggregation, of a sort) charges each document it keeps to the limit this makes, and
 * gives the charge back with {@link OperationHeapLimit#release()} when it lets go of them.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class MongoResultHeap {
  private MongoResultHeap() {
  }

  /**
   * @param database  whose configuration sets the cap of elements
   * @param operation what holds the documents, as the refusal names it
   */
  static OperationHeapLimit of(final Database database, final String operation) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return OperationHeapLimit.of(context, "documents", operation);
  }
}
