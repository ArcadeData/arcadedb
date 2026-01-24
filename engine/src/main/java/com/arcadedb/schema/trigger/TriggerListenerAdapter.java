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
package com.arcadedb.schema.trigger;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.schema.Trigger;

/**
 * Adapter that bridges Trigger instances to the event listener system.
 * Implements all 8 listener interfaces and delegates to the appropriate TriggerExecutor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TriggerListenerAdapter implements
    BeforeRecordCreateListener, BeforeRecordReadListener, BeforeRecordUpdateListener, BeforeRecordDeleteListener,
    AfterRecordCreateListener, AfterRecordReadListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final Trigger trigger;
  private final TriggerExecutor executor;
  private final Database database;

  public TriggerListenerAdapter(final Database database, final Trigger trigger, final TriggerExecutor executor) {
    this.database = database;
    this.trigger = trigger;
    this.executor = executor;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public void cleanup() {
    if (executor != null) {
      executor.close();
    }
  }

  @Override
  public boolean onBeforeCreate(final Record record) {
    if (trigger.getTiming() == Trigger.TriggerTiming.BEFORE &&
        trigger.getEvent() == Trigger.TriggerEvent.CREATE &&
        matchesType(record)) {
      return executor.execute(database, record, null);
    }
    return true;
  }

  @Override
  public boolean onBeforeRead(final RID rid) {
    if (trigger.getTiming() == Trigger.TriggerTiming.BEFORE &&
        trigger.getEvent() == Trigger.TriggerEvent.READ) {
      // For BEFORE READ, we receive only the RID, so we need to load the record
      // This is a limitation noted in the implementation plan
      final Record record = rid.asDocument();
      if (matchesType(record)) {
        return executor.execute(database, record, null);
      }
    }
    return true;
  }

  @Override
  public boolean onBeforeUpdate(final Record record) {
    if (trigger.getTiming() == Trigger.TriggerTiming.BEFORE &&
        trigger.getEvent() == Trigger.TriggerEvent.UPDATE &&
        matchesType(record)) {
      // Note: oldRecord is not available in the current event system
      // This could be enhanced in the future
      return executor.execute(database, record, null);
    }
    return true;
  }

  @Override
  public boolean onBeforeDelete(final Record record) {
    if (trigger.getTiming() == Trigger.TriggerTiming.BEFORE &&
        trigger.getEvent() == Trigger.TriggerEvent.DELETE &&
        matchesType(record)) {
      return executor.execute(database, record, null);
    }
    return true;
  }

  @Override
  public void onAfterCreate(final Record record) {
    if (trigger.getTiming() == Trigger.TriggerTiming.AFTER &&
        trigger.getEvent() == Trigger.TriggerEvent.CREATE &&
        matchesType(record)) {
      executor.execute(database, record, null);
    }
  }

  @Override
  public Record onAfterRead(final Record record) {
    if (trigger.getTiming() == Trigger.TriggerTiming.AFTER &&
        trigger.getEvent() == Trigger.TriggerEvent.READ &&
        matchesType(record)) {
      executor.execute(database, record, null);
    }
    return record;
  }

  @Override
  public void onAfterUpdate(final Record record) {
    if (trigger.getTiming() == Trigger.TriggerTiming.AFTER &&
        trigger.getEvent() == Trigger.TriggerEvent.UPDATE &&
        matchesType(record)) {
      // Note: oldRecord is not available in the current event system
      executor.execute(database, record, null);
    }
  }

  @Override
  public void onAfterDelete(final Record record) {
    if (trigger.getTiming() == Trigger.TriggerTiming.AFTER &&
        trigger.getEvent() == Trigger.TriggerEvent.DELETE &&
        matchesType(record)) {
      executor.execute(database, record, null);
    }
  }

  /**
   * Check if the record matches the trigger's type.
   */
  private boolean matchesType(final Record record) {
    if (record instanceof Document) {
      final String typeName = ((Document) record).getTypeName();
      return typeName != null && typeName.equals(trigger.getTypeName());
    }
    return false;
  }
}
