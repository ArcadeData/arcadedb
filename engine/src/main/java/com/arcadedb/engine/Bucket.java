package com.arcadedb.engine;/*
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

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.Iterator;

/**
 * Bucket interface. It represents a collection of records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface Bucket {
  RID createRecord(Record record, boolean discardRecordAfter);

  void updateRecord(Record record, boolean discardRecordAfter);

  Binary getRecord(RID rid);

  boolean existsRecord(RID rid);

  void deleteRecord(RID rid);

  /**
   * Deletes a record, optionally forcing removal even when a multi-page chunk chain is structurally broken (see
   * {@link com.arcadedb.engine.LocalBucket#deleteRecord(RID, boolean)}). With {@code force=false} this is identical to
   * {@link #deleteRecord(RID)}.
   */
  void deleteRecord(RID rid, boolean force);

  void scan(RawRecordCallback callback, ErrorRecordCallback errorRecordCallback);

  Iterator<Record> iterator();

  Iterator<Record> inverseIterator();

  long count();

  int getFileId();

  String getName();
}
