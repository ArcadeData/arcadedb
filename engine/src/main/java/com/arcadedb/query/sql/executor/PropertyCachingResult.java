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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * A row of a scan read through a cache of its record's properties, for the expressions of one row that read the same
 * properties over and over: the arguments of the aggregates of a GROUP BY (issue #9496), where TPC-H Q1 reads seven
 * properties twelve times per row.
 * <p>
 * Reading a property of a record off its bytes walks the record header from the start until it meets the property, then
 * decodes the value. This view walks the header once per row for every property the expressions read - it learns them
 * from the first rows - and decodes each value once: a value that cannot change under its reader (a number, a string,
 * a date...) is kept for the rest of the row, a mutable one (a list, a map, an embedded document) is decoded again on
 * every read, from the position the walk found, so a reader that changes it does not change what the next reader sees.
 * <p>
 * Everything else is the row's: the view only answers {@link #getPropertyIfPresent}, which is how an expression reads a
 * property, and passes every other call to the row. It caches only a plain row over an immutable record with nothing
 * set on it - the row of a scan - and passes everything through for any other one.
 * <p>
 * One instance serves the rows of one evaluator, one after the other, and is not thread-safe. The caller must make sure
 * the view itself is never kept beyond its row - no expression it evaluates on the view may hand the row out - since the
 * next row reuses it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PropertyCachingResult implements Result {
  // THE PROPERTIES A ROW CAN CACHE: MORE NAMES THAN THAT ARE READ FROM THE ROW AS THEY ALWAYS WERE
  private static final int    MAX_PROPERTIES    = 64;
  // A DICTIONARY ID PAST THIS IS NOT INDEXED BY ID: ITS PROPERTY IS READ ON ITS OWN, AND STILL KEPT FOR THE ROW
  private static final int    MAX_DICTIONARY_ID = 1 << 16;
  private static final Object NOT_READ          = new Object();
  private static final Object ABSENT            = new Object();

  private final Database   database;
  private       String[]   names        = new String[8];
  // THE DICTIONARY ID OF EACH NAME, -1 WHEN THE DICTIONARY DID NOT KNOW IT WHEN IT WAS LEARNED
  private       int[]      nameIds      = new int[8];
  private       int        count        = 0;
  // HOW MANY OF THE NAMES HAVE A DICTIONARY ID: THE PROPERTIES THE WALK OF A HEADER LOOKS FOR
  private       int        idCount      = 0;
  // INDEX IN names BY DICTIONARY ID, -1 FOR A PROPERTY NOT CACHED
  private       int[]      slotByNameId = new int[0];
  // PER ROW
  private       Object[]   values       = new Object[8];
  private       int[]      positions    = new int[8];
  private       ResultInternal row;
  private       ImmutableDocument document;
  private       Binary     located;
  // THE NAMES THE WALK OF THIS ROW'S HEADER LOCATED: ONE LEARNED AFTER IT IS READ ON ITS OWN UNTIL THE NEXT ROW
  private       int        locatedCount;
  private       boolean    walked;

  PropertyCachingResult(final Database database) {
    this.database = database;
  }

  /**
   * Points this view at {@code row} and answers it, or answers {@code row} itself when its properties cannot be cached:
   * not a plain row over an immutable record with nothing set on it.
   */
  Result of(final Result row) {
    if (row == null || row.getClass() != ResultInternal.class)
      return row;
    final ResultInternal internal = (ResultInternal) row;
    if (!(internal.element instanceof ImmutableDocument immutable) || internal.tombstones != null
        || internal.content != null && !internal.content.isEmpty())
      return row;

    this.row = internal;
    this.document = immutable;
    this.located = null;
    this.walked = false;
    Arrays.fill(values, 0, count, NOT_READ);
    return this;
  }

  @Override
  public Object getPropertyIfPresent(final String name, final Object absentValue) {
    int index = indexOf(name);
    if (index < 0) {
      index = learn(name);
      if (index < 0)
        return row.getPropertyIfPresent(name, absentValue);
    }

    Object value = values[index];
    if (value == NOT_READ) {
      value = read(index, name);
      if (isImmutable(value))
        values[index] = value;
    }
    return value == ABSENT ? absentValue : value;
  }

  private int indexOf(final String name) {
    // THE NAMES AN EXPRESSION READS ARE THE STRINGS OF ITS AST, SO THE SAME INSTANCE COMES BACK ROW AFTER ROW
    for (int i = 0; i < count; i++)
      if (names[i] == name)
        return i;
    for (int i = 0; i < count; i++)
      if (names[i].equals(name))
        return i;
    return -1;
  }

  /** Starts caching {@code name}, or answers -1 when no more names can be cached. */
  private int learn(final String name) {
    if (name == null || count >= MAX_PROPERTIES)
      return -1;

    if (count == names.length) {
      final int newLength = names.length * 2;
      names = Arrays.copyOf(names, newLength);
      nameIds = Arrays.copyOf(nameIds, newLength);
      values = Arrays.copyOf(values, newLength);
      positions = Arrays.copyOf(positions, newLength);
    }

    final Dictionary dictionary = database.getSchema().getDictionary();
    final int nameId = dictionary.getIdByName(name, false);
    final int index = count++;
    names[index] = name;
    values[index] = NOT_READ;
    if (nameId >= 0 && nameId < MAX_DICTIONARY_ID) {
      nameIds[index] = nameId;
      ++idCount;
      if (nameId >= slotByNameId.length) {
        final int oldLength = slotByNameId.length;
        slotByNameId = Arrays.copyOf(slotByNameId, Math.max(nameId + 1, oldLength * 2));
        Arrays.fill(slotByNameId, oldLength, slotByNameId.length, -1);
      }
      slotByNameId[nameId] = index;
    } else
      nameIds[index] = -1;
    return index;
  }

  /** The value of the property at {@code index} as the row answers it, or {@link #ABSENT}. */
  private Object read(final int index, final String name) {
    final Object raw;
    if (nameIds[index] < 0)
      // A NAME THE DICTIONARY DID NOT KNOW WHEN IT WAS LEARNED, OR WITH AN ID TOO LARGE TO INDEX: ASK THE RECORD
      raw = document.getIfPresent(name, ABSENT);
    else {
      if (!walked) {
        walked = true;
        located = document.locateProperties(slotByNameId, positions, idCount);
        locatedCount = count;
      }
      if (located == null || index >= locatedCount)
        raw = document.getIfPresent(name, ABSENT);
      else if (positions[index] < 0)
        raw = ABSENT;
      else
        raw = document.getPropertyAt(located, name, positions[index], ABSENT);
    }
    // WHAT ResultInternal.getPropertyIfPresent() ANSWERS FOR A ROW WITH NOTHING SET ON IT
    return raw == ABSENT ? ABSENT : ResultInternal.toPropertyValue(raw);
  }

  /** Whether {@code value} cannot be changed by whoever reads it, so one decoded instance can serve the whole row. */
  private static boolean isImmutable(final Object value) {
    return value == null || value == ABSENT || value instanceof String || value instanceof Number || value instanceof Boolean
        || value instanceof RID || value instanceof TemporalAccessor || value instanceof UUID || value instanceof Character;
  }

  // EVERYTHING ELSE IS THE ROW'S

  @Override
  public <T> T getProperty(final String name) {
    return row.getProperty(name);
  }

  @Override
  public <T> T getProperty(final String name, final Object defaultValue) {
    return row.getProperty(name, defaultValue);
  }

  @Override
  public Record getElementProperty(final String name) {
    return row.getElementProperty(name);
  }

  @Override
  public Type getPropertyType(final String name) {
    return row.getPropertyType(name);
  }

  @Override
  public Set<String> getPropertyNames() {
    return row.getPropertyNames();
  }

  @Override
  public Optional<RID> getIdentity() {
    return row.getIdentity();
  }

  @Override
  public boolean isElement() {
    return row.isElement();
  }

  @Override
  public Optional<Document> getElement() {
    return row.getElement();
  }

  @Override
  public Document toElement() {
    return row.toElement();
  }

  @Override
  public Optional<Record> getRecord() {
    return row.getRecord();
  }

  @Override
  public boolean isRecord() {
    return row.isRecord();
  }

  @Override
  public boolean isProjection() {
    return row.isProjection();
  }

  @Override
  public Object getMetadata(final String key) {
    return row.getMetadata(key);
  }

  @Override
  public Set<String> getMetadataKeys() {
    return row.getMetadataKeys();
  }

  @Override
  public JSONObject toJSON() {
    return row.toJSON();
  }

  @Override
  public Database getDatabase() {
    return row.getDatabase();
  }

  @Override
  public boolean hasProperty(final String varName) {
    return row.hasProperty(varName);
  }

  @Override
  public Map<String, Object> toMap() {
    return row.toMap();
  }

  @Override
  public boolean equals(final Object obj) {
    return row.equals(obj instanceof PropertyCachingResult other ? other.row : obj);
  }

  @Override
  public int hashCode() {
    return row.hashCode();
  }

  @Override
  public String toString() {
    return row.toString();
  }
}
