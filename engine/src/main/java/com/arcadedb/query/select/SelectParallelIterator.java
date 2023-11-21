package com.arcadedb.query.select;/*
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
 */

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.async.DatabaseAsyncBrowseIterator;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.utility.MultiIterator;
import com.conversantmedia.util.concurrent.MultithreadConcurrentQueue;

import java.util.*;
import java.util.concurrent.*;

/**
 * Query iterator returned from queries. Extends the base Java iterator with convenient methods.
 *
 * <h4>Implementation details</h4>
 * <p>
 * The iterator keeps track for the returned records in case multiple indexes have being used. In fact, in case multiple
 * indexes are used, it's much simpler to just return index cursor tha could overlap. In this case, the property
 * `filterOutRecords` keeps track of the returning RIDs to avoid returning duplicates.</p>
 *
 * <p>
 * This iterator creates
 * one task per bucket and execute the fetching in parallel. The fetched records are kept in memory in a list. The list
 * has a pointer to the last element fetched. Once fetched, the element is removed from the list, but the list is not shrunk
 * to avoid wasting CPU to save a few KB.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectParallelIterator<T extends Document> extends SelectIterator<T> {
  private final CountDownLatch                   semaphore;
  private final MultithreadConcurrentQueue<Document> queue;

  protected SelectParallelIterator(final SelectExecutor executor, final Iterator<? extends Identifiable> iterator,
      final boolean enforceUniqueReturn) {
    super(executor, iterator, enforceUniqueReturn);

    if (iterator instanceof MultiIterator) {
      final MultiIterator<? extends Identifiable> it = (MultiIterator<? extends Identifiable>) iterator;
      queue = new MultithreadConcurrentQueue<>(4_096);

      final List<Object> sources = it.getSources();

      final DatabaseInternal database = executor.select.database;
      final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
      final int backPressurePercentage = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_BACK_PRESSURE);

      semaphore = new CountDownLatch(sources.size());

      for (int i = 0; i < sources.size(); i++) {
        final Iterator<? extends Identifiable> source = (Iterator<? extends Identifiable>) sources.get(i);

        //System.out.printf("Iterator %d on %s\n", i, source);

        async.scheduleTask(-1, new DatabaseAsyncBrowseIterator(semaphore, record -> {
          if (filterOutRecords != null && filterOutRecords.contains(record.getIdentity()))
            // ALREADY RETURNED, AVOID DUPLICATES IN THE RESULT SET
            return true;

          if (executor.select.rootTreeElement == null || executor.evaluateWhere(record)) {
            ++returned;

            if (filterOutRecords != null)
              filterOutRecords.add(record.getIdentity());

            while (true) {
              if (queue.offer(record))
                break;
            }
          }
          return true;
        }, source), true, backPressurePercentage);
      }

    } else {
      queue = null;
      semaphore = null;
    }
  }

  @Override
  protected T fetchNext() {
    if (!(iterator instanceof MultiIterator))
      // NO CHANCE TO GO IN PARALLEL, FALL BACK TO THE SERIAL IMPLEMENTATION
      return super.fetchNext();
    else
      ((MultiIterator<? extends Identifiable>) iterator).checkForTimeout();

    while (!queue.isEmpty() || semaphore.getCount() > 0) {
      final T record = (T) queue.poll();
      if (record != null)
        return record;
    }

    // NOT FOUND
    return null;
  }
}
