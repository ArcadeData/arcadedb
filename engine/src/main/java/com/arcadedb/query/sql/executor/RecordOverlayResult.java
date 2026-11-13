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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * A projected row laid over the record it was projected from, for evaluating an {@code UNWIND <operand> AS x} that reads
 * both (issue #11563): a name the row holds - a projection alias, an UNWIND item already written, a column the
 * projection wrote as {@code null} - answers from the row, and any other name answers from the record, as do the record
 * itself ({@code @rid}, {@code @type}, {@code @this}) and what reads it ({@code out()}). A name the projection excluded
 * ({@code !name}) answers {@code null}, as it does on the row.
 * <p>
 * Read-only, and short-lived: it is built for one operand evaluation and never reaches a result set. The row's map is
 * shared, not copied, when the row is a plain projected row.
 */
final class RecordOverlayResult extends ResultInternal {
  private final Set<String> hiddenNames;

  RecordOverlayResult(final Database database, final Result row, final Document record, final Set<String> hiddenNames) {
    super(database);
    if (row.getClass() == ResultInternal.class && ((ResultInternal) row).content != null
        && (((ResultInternal) row).tombstones == null || ((ResultInternal) row).tombstones.isEmpty()))
      this.content = ((ResultInternal) row).content;
    else {
      final Set<String> names = row.getPropertyNames();
      this.content = new LinkedHashMap<>(names.size() * 2);
      for (final String name : names)
        this.content.put(name, row.getProperty(name));
    }
    this.element = record;
    this.hiddenNames = hiddenNames;
  }

  private boolean answeredByRow(final String name) {
    return content.containsKey(name) || (hiddenNames != null && hiddenNames.contains(name));
  }

  @Override
  public <T> T getProperty(final String name) {
    return (T) toPropertyValue(answeredByRow(name) ? content.get(name) : element.get(name));
  }

  @Override
  public <T> T getProperty(final String name, final Object defaultValue) {
    if (answeredByRow(name))
      return content.containsKey(name) ? (T) toPropertyValue(content.get(name)) : (T) defaultValue;
    return (T) toPropertyValue(element.getIfPresent(name, defaultValue));
  }

  @SuppressWarnings("PMD.CompareObjectsWithEquals") // identity against the caller's sentinel, as in ResultInternal
  @Override
  public Object getPropertyIfPresent(final String name, final Object absentValue) {
    if (answeredByRow(name))
      return content.containsKey(name) ? toPropertyValue(content.get(name)) : absentValue;
    final Object result = element.getIfPresent(name, absentValue);
    return result == absentValue ? absentValue : toPropertyValue(result);
  }

  @Override
  public Record getElementProperty(final String name) {
    Object result = answeredByRow(name) ? content.get(name) : element.get(name);
    if (result instanceof Result r)
      result = r.getRecord().orElse(null);
    if (result instanceof RID rid)
      result = rid.getRecord();
    return result instanceof Record r ? r : null;
  }

  @Override
  public boolean hasProperty(final String name) {
    if (answeredByRow(name))
      return content.containsKey(name);
    return element.has(name);
  }

  @Override
  public ResultInternal setProperty(final String name, final Object value) {
    throw new UnsupportedOperationException("The row an UNWIND operand is evaluated on is read-only");
  }

  @Override
  public void removeProperty(final String name) {
    throw new UnsupportedOperationException("The row an UNWIND operand is evaluated on is read-only");
  }
}
