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
package com.arcadedb.database;

import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.event.BeforeRecordUpdateListener;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordEventsRegistry implements RecordEvents {
  private final List<BeforeRecordCreateListener> beforeCreateListeners = new CopyOnWriteArrayList<>();
  private final List<BeforeRecordReadListener>   beforeReadListeners   = new CopyOnWriteArrayList<>();
  private final List<BeforeRecordUpdateListener> beforeUpdateListeners = new CopyOnWriteArrayList<>();
  private final List<BeforeRecordDeleteListener> beforeDeleteListeners = new CopyOnWriteArrayList<>();
  private final List<AfterRecordCreateListener>  afterCreateListeners  = new CopyOnWriteArrayList<>();
  private final List<AfterRecordReadListener>    afterReadListeners    = new CopyOnWriteArrayList<>();
  private final List<AfterRecordUpdateListener>  afterUpdateListeners  = new CopyOnWriteArrayList<>();
  private final List<AfterRecordDeleteListener>  afterDeleteListeners  = new CopyOnWriteArrayList<>();
  private final Object                           changeLock            = new Object();
  /**
   * The BEFORE READ listeners of a whole database: the ones of its own registry and of the registry of every type of its
   * schema, which all share this one counter (issue #9997). A read asks it before finding the type of the record it is
   * about to hand out, a checked {@code getSchema()} and a bucket-to-type lookup per record, and skips both while it is
   * zero. Exact, not sticky: an unregistered listener, a dropped type and a type a schema reload replaced all give their
   * count back, so a database returns to the fast path once its last listener is gone.
   */
  private final AtomicInteger                    databaseBeforeReadListeners;
  // Guarded by changeLock. Set when this registry's type left the schema: its listeners are no longer counted
  private       boolean                          detached;

  /** The registry of a database: the root of the count its types' registries share. */
  public RecordEventsRegistry() {
    this(new AtomicInteger());
  }

  private RecordEventsRegistry(final AtomicInteger databaseBeforeReadListeners) {
    this.databaseBeforeReadListeners = databaseBeforeReadListeners;
  }

  /**
   * A registry for a type of this database, whose BEFORE READ listeners are counted in the database's total. The contract
   * that keeps the total exact: whoever retires the type (a drop, a schema reload replacing it) calls
   * {@link #detachFromDatabase()} on its registry. A registry discarded without it keeps its listeners counted, which can
   * only keep the database on the dispatch path, never skip a listener.
   */
  public RecordEventsRegistry newTypeRegistry() {
    return new RecordEventsRegistry(databaseBeforeReadListeners);
  }

  /**
   * Whether a BEFORE READ listener is registered on the database this registry belongs to, or on any of its types: false
   * means no read of the database has anyone to tell. One volatile read.
   */
  public boolean hasBeforeReadListenersInDatabase() {
    return databaseBeforeReadListeners.get() > 0;
  }

  /**
   * Called when the type owning this registry leaves the schema (dropped, or replaced by a schema reload): no read reaches
   * its listeners any more, so they stop counting in the database's total. The listeners themselves are left in place, and
   * a listener registered afterwards through a reference to the retired type is not counted either: no read reaches it.
   */
  public void detachFromDatabase() {
    synchronized (changeLock) {
      if (detached)
        return;
      detached = true;
      if (!beforeReadListeners.isEmpty())
        databaseBeforeReadListeners.addAndGet(-beforeReadListeners.size());
    }
  }

  @Override
  public RecordEventsRegistry registerListener(final BeforeRecordCreateListener listener) {
    synchronized (changeLock) {
      if (!beforeCreateListeners.contains(listener))
        beforeCreateListeners.add(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry registerListener(final BeforeRecordReadListener listener) {
    synchronized (changeLock) {
      if (!beforeReadListeners.contains(listener)) {
        // COUNTED BEFORE IT IS VISIBLE: A READ THAT FINDS THE COUNT AT ZERO HAS NO LISTENER TO MISS
        if (!detached)
          databaseBeforeReadListeners.incrementAndGet();
        beforeReadListeners.add(listener);
      }
    }
    return this;
  }

  @Override
  public RecordEventsRegistry registerListener(final BeforeRecordUpdateListener listener) {
    synchronized (changeLock) {
      if (!beforeUpdateListeners.contains(listener))
        beforeUpdateListeners.add(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry registerListener(final BeforeRecordDeleteListener listener) {
    synchronized (changeLock) {
      if (!beforeDeleteListeners.contains(listener))
        beforeDeleteListeners.add(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry registerListener(final AfterRecordCreateListener listener) {
    synchronized (changeLock) {
      if (!afterCreateListeners.contains(listener))
        afterCreateListeners.add(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry registerListener(final AfterRecordReadListener listener) {
    synchronized (changeLock) {
      if (!afterReadListeners.contains(listener))
        afterReadListeners.add(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry registerListener(final AfterRecordUpdateListener listener) {
    synchronized (changeLock) {
      if (!afterUpdateListeners.contains(listener))
        afterUpdateListeners.add(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry registerListener(final AfterRecordDeleteListener listener) {
    synchronized (changeLock) {
      if (!afterDeleteListeners.contains(listener))
        afterDeleteListeners.add(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry unregisterListener(final BeforeRecordReadListener listener) {
    synchronized (changeLock) {
      if (beforeReadListeners.remove(listener) && !detached)
        databaseBeforeReadListeners.decrementAndGet();
    }
    return this;
  }

  @Override
  public RecordEventsRegistry unregisterListener(final BeforeRecordCreateListener listener) {
    synchronized (changeLock) {
      beforeCreateListeners.remove(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry unregisterListener(final BeforeRecordUpdateListener listener) {
    synchronized (changeLock) {
      beforeUpdateListeners.remove(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry unregisterListener(final BeforeRecordDeleteListener listener) {
    synchronized (changeLock) {
      beforeDeleteListeners.remove(listener);
    }
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final AfterRecordCreateListener listener) {
    synchronized (changeLock) {
      afterCreateListeners.remove(listener);
    }
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final AfterRecordReadListener listener) {
    synchronized (changeLock) {
      afterReadListeners.remove(listener);
    }
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final AfterRecordUpdateListener listener) {
    synchronized (changeLock) {
      afterUpdateListeners.remove(listener);
    }
    return this;
  }

  @Override
  public RecordEventsRegistry unregisterListener(final AfterRecordDeleteListener listener) {
    synchronized (changeLock) {
      afterDeleteListeners.remove(listener);
    }
    return this;
  }

  public boolean onBeforeCreate(final Record record) {
    if (beforeCreateListeners.isEmpty())
      return true;

    for (BeforeRecordCreateListener listener : beforeCreateListeners)
      if (!listener.onBeforeCreate(record))
        return false;

    return true;
  }

  /**
   * Whether anything is listening, so a caller can skip the re-entrancy bookkeeping around the dispatch entirely
   * when nothing would fire. O(1) on the copy-on-write list, and the overwhelmingly common answer is false.
   */
  public boolean hasBeforeReadListeners() {
    return !beforeReadListeners.isEmpty();
  }

  /** See {@link #hasBeforeReadListeners()}. */
  public boolean hasAfterReadListeners() {
    return !afterReadListeners.isEmpty();
  }

  public boolean onBeforeRead(final RID rid) {
    if (beforeReadListeners.isEmpty())
      return true;

    for (BeforeRecordReadListener listener : beforeReadListeners)
      if (!listener.onBeforeRead(rid))
        return false;

    return true;
  }

  public boolean onBeforeUpdate(final Record record) {
    if (beforeUpdateListeners.isEmpty())
      return true;

    for (BeforeRecordUpdateListener listener : beforeUpdateListeners)
      if (!listener.onBeforeUpdate(record))
        return false;

    return true;
  }

  public boolean onBeforeDelete(final Record record) {
    if (beforeDeleteListeners.isEmpty())
      return true;

    for (BeforeRecordDeleteListener listener : beforeDeleteListeners)
      if (!listener.onBeforeDelete(record))
        return false;

    return true;
  }

  public void onAfterCreate(final Record record) {
    if (afterCreateListeners.isEmpty())
      return;

    for (AfterRecordCreateListener listener : afterCreateListeners)
      listener.onAfterCreate(record);
  }

  public Record onAfterRead(Record record) {
    if (afterReadListeners.isEmpty())
      return record;

    for (AfterRecordReadListener listener : afterReadListeners) {
      record = listener.onAfterRead(record);
      if (record == null) return null;
    }
    return record;
  }

  public void onAfterUpdate(final Record record) {
    if (afterUpdateListeners.isEmpty())
      return;

    for (AfterRecordUpdateListener listener : afterUpdateListeners)
      listener.onAfterUpdate(record);
  }

  public void onAfterDelete(final Record record) {
    if (afterDeleteListeners.isEmpty())
      return;

    for (AfterRecordDeleteListener listener : afterDeleteListeners)
      listener.onAfterDelete(record);
  }
}
