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

import com.arcadedb.event.DatabaseEventAfterCreateListener;
import com.arcadedb.event.DatabaseEventAfterDeleteListener;
import com.arcadedb.event.DatabaseEventAfterUpdateListener;
import com.arcadedb.event.DatabaseEventBeforeCreateListener;
import com.arcadedb.event.DatabaseEventBeforeDeleteListener;
import com.arcadedb.event.DatabaseEventBeforeUpdateListener;

import java.util.*;

public class DatabaseEventsRegistry implements DatabaseEvents {
  private final List<DatabaseEventBeforeCreateListener> beforeCreateListeners = new ArrayList<>();
  private final List<DatabaseEventBeforeUpdateListener> beforeUpdateListeners = new ArrayList<>();
  private final List<DatabaseEventBeforeDeleteListener> beforeDeleteListeners = new ArrayList<>();
  private final List<DatabaseEventAfterCreateListener>  afterCreateListeners  = new ArrayList<>();
  private final List<DatabaseEventAfterUpdateListener>  afterUpdateListeners  = new ArrayList<>();
  private final List<DatabaseEventAfterDeleteListener>  afterDeleteListeners  = new ArrayList<>();

  protected DatabaseEventsRegistry() {
  }

  @Override
  public synchronized DatabaseEventsRegistry registerListener(final DatabaseEventBeforeCreateListener listener) {
    if (!beforeCreateListeners.contains(listener))
      beforeCreateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry registerListener(final DatabaseEventBeforeUpdateListener listener) {
    if (!beforeUpdateListeners.contains(listener))
      beforeUpdateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry registerListener(final DatabaseEventBeforeDeleteListener listener) {
    if (!beforeDeleteListeners.contains(listener))
      beforeDeleteListeners.add(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry registerListener(final DatabaseEventAfterCreateListener listener) {
    if (!afterCreateListeners.contains(listener))
      afterCreateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry registerListener(final DatabaseEventAfterUpdateListener listener) {
    if (!afterUpdateListeners.contains(listener))
      afterUpdateListeners.add(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry registerListener(final DatabaseEventAfterDeleteListener listener) {
    if (!afterDeleteListeners.contains(listener))
      afterDeleteListeners.add(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry unregisterListener(final DatabaseEventBeforeCreateListener listener) {
    beforeCreateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry unregisterListener(final DatabaseEventBeforeUpdateListener listener) {
    beforeUpdateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry unregisterListener(final DatabaseEventBeforeDeleteListener listener) {
    beforeDeleteListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry unregisterListener(final DatabaseEventAfterCreateListener listener) {
    afterCreateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry unregisterListener(final DatabaseEventAfterUpdateListener listener) {
    afterUpdateListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized DatabaseEventsRegistry unregisterListener(final DatabaseEventAfterDeleteListener listener) {
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
