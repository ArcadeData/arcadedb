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
package com.arcadedb.event;

import com.arcadedb.database.Record;

/**
 * Listener to receive events after a delete operation occurs on records (documents, vertices and edges).
 * <p>
 * NOTE: the callback is invoked synchronously. For this reason the execution should be as fast as possible. Even with a fast implementation, using this
 * callback may cause a sensible slowdown of delete operations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
public interface AfterRecordDeleteListener {
  /**
   * Callback invoked right after a record (documents, vertices and edges) has been deleted. You can use this callback to implement a cascade deletion.
   */
  void onAfterDelete(Record record);
}
