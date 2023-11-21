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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.Identifiable;

import java.util.*;
import java.util.concurrent.*;

public class DatabaseAsyncBrowseIterator implements DatabaseAsyncTask {
  public final CountDownLatch                   semaphore;
  public final DocumentCallback                 callback;
  public final Iterator<? extends Identifiable> iterator;

  public DatabaseAsyncBrowseIterator(final CountDownLatch semaphore, final DocumentCallback callback,
      final Iterator<? extends Identifiable> iterator) {
    this.semaphore = semaphore;
    this.callback = callback;
    this.iterator = iterator;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      while (iterator.hasNext()) {
        if (!callback.onRecord(iterator.next().asDocument()))
          break;

        if (async.isShutdown())
          break;
      }
    } finally {
      // UNLOCK THE CALLER THREAD
      semaphore.countDown();
    }
  }

  @Override
  public String toString() {
    return "BrowseIterator(" + iterator + ")";
  }
}
