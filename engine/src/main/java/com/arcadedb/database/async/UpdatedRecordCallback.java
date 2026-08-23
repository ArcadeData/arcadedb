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
package com.arcadedb.database.async;

import com.arcadedb.database.Record;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Interface used in `async().updateRecord()` API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface UpdatedRecordCallback {
  /**
   * Invoked once the update has been applied to the async worker's current batch transaction, which is not yet
   * durable: a later task in the same still-open batch can still fail and roll it back (issue #6470). See
   * {@link DatabaseAsyncExecutor#onOk(OkCallback)} for a signal that fires only once the batch actually commits.
   */
  void call(Record newRecord);
}
