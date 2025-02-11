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

import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.event.BeforeRecordUpdateListener;

import java.util.*;
import java.util.concurrent.*;

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
      if (!beforeReadListeners.contains(listener))
        beforeReadListeners.add(listener);
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
      beforeReadListeners.remove(listener);
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
