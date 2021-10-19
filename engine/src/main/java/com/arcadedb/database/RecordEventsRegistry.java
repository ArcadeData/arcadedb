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
 */
package com.arcadedb.database;

import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;

import java.util.*;

public class RecordEventsRegistry implements RecordEvents {
  private final List<BeforeRecordCreateListener>        beforeCreateListeners = new ArrayList<>();
  private final List<BeforeRecordUpdateListener> beforeUpdateListeners = new ArrayList<>();
  private final List<BeforeRecordDeleteListener> beforeDeleteListeners = new ArrayList<>();
  private final List<AfterRecordCreateListener>  afterCreateListeners  = new ArrayList<>();
  private final List<AfterRecordUpdateListener> afterUpdateListeners = new ArrayList<>();
  private final List<AfterRecordDeleteListener> afterDeleteListeners = new ArrayList<>();

  @Override
  public synchronized RecordEventsRegistry registerListener(final BeforeRecordCreateListener listener) {
    if (!beforeCreateListeners.contains(listener))
      beforeCreateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry registerListener(final BeforeRecordUpdateListener listener) {
    if (!beforeUpdateListeners.contains(listener))
      beforeUpdateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry registerListener(final BeforeRecordDeleteListener listener) {
    if (!beforeDeleteListeners.contains(listener))
      beforeDeleteListeners.add(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry registerListener(final AfterRecordCreateListener listener) {
    if (!afterCreateListeners.contains(listener))
      afterCreateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry registerListener(final AfterRecordUpdateListener listener) {
    if (!afterUpdateListeners.contains(listener))
      afterUpdateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry registerListener(final AfterRecordDeleteListener listener) {
    if (!afterDeleteListeners.contains(listener))
      afterDeleteListeners.add(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final BeforeRecordCreateListener listener) {
    beforeCreateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final BeforeRecordUpdateListener listener) {
    beforeUpdateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final BeforeRecordDeleteListener listener) {
    beforeDeleteListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final AfterRecordCreateListener listener) {
    afterCreateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final AfterRecordUpdateListener listener) {
    afterUpdateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized RecordEventsRegistry unregisterListener(final AfterRecordDeleteListener listener) {
    afterDeleteListeners.remove(listener);
    return this;
  }

  public boolean onBeforeCreate(final Record record) {
    if (beforeCreateListeners.isEmpty())
      return true;

    for (int i = 0; i < beforeCreateListeners.size(); i++) {
      if (!beforeCreateListeners.get(i).onBeforeCreate(record))
        return false;
    }
    return true;
  }

  public boolean onBeforeUpdate(final Record record) {
    if (beforeUpdateListeners.isEmpty())
      return true;

    for (int i = 0; i < beforeUpdateListeners.size(); i++) {
      if (!beforeUpdateListeners.get(i).onBeforeUpdate(record))
        return false;
    }
    return true;
  }

  public boolean onBeforeDelete(final Record record) {
    if (beforeDeleteListeners.isEmpty())
      return true;

    for (int i = 0; i < beforeDeleteListeners.size(); i++) {
      if (!beforeDeleteListeners.get(i).onBeforeDelete(record))
        return false;
    }
    return true;
  }

  public void onAfterCreate(final Record record) {
    if (afterCreateListeners.isEmpty())
      return;

    for (int i = 0; i < afterCreateListeners.size(); i++)
      afterCreateListeners.get(i).onAfterCreate(record);
  }

  public void onAfterUpdate(final Record record) {
    if (afterUpdateListeners.isEmpty())
      return;

    for (int i = 0; i < afterUpdateListeners.size(); i++)
      afterUpdateListeners.get(i).onAfterUpdate(record);
  }

  public void onAfterDelete(final Record record) {
    if (afterDeleteListeners.isEmpty())
      return;

    for (int i = 0; i < afterDeleteListeners.size(); i++)
      afterDeleteListeners.get(i).onAfterDelete(record);
  }
}
