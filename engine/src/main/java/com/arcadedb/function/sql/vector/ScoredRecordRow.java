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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Document;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * One output row of a vector search function ({@code vector.neighbors}, {@code vector.fuse}, {@code vector.rerank}, ...):
 * the record, its {@code @rid} and {@code @type}, every property of the record, and the row's score under
 * {@code scoreKey}.
 * <p>
 * It answers what the {@code LinkedHashMap} these functions used to build answers - same values, same order, the score
 * and {@code @rid}/{@code @type} overriding a record property of the same name, the {@code record} key aside (see below) -
 * but reads the record's properties on demand instead of copying all of them up front (issue #10129). A consumer that only wants the record,
 * the RID or the score, such as {@code vector.rerank} over {@code vector.fuse}, therefore no longer pays for copying
 * every property (an embedding and a passage of text, typically) of every candidate. {@code expand()} wraps the row as
 * the content of a {@code ResultInternal} without copying it, so the outer query reads the properties it projects
 * straight off the record.
 * <p>
 * Enumerating the row ({@link #entrySet()}, {@link #size()}, {@code SELECT *}, JSON serialization) or writing to it
 * builds the eager copy once and answers from it afterwards, so a write lands on the row and never on the record.
 * Until then the property values are those of the record at the time they are read, not at the time the row was made.
 * <p>
 * The {@code record} key is answered by {@link #get} and {@link #containsKey} but never enumerated (issue #10898): the
 * flattened properties, {@code @rid} and {@code @type} already are the record, so {@code SELECT expand(...)} and the JSON of a
 * row carried every property twice - the embedding and the passage, the largest ones, included. A projection, a
 * {@code record.name} path, a re-ranker and any caller that asks for the key still get the record. A record property named
 * {@code record}, or a value written under the key, is a real entry and is enumerated.
 */
final class ScoredRecordRow extends AbstractMap<String, Object> {
  static final         String RECORD = "record";
  private static final String RID    = "@rid";
  private static final String TYPE   = "@type";

  private final Document                      record;
  private final String                        scoreKey;
  private final Object                        score;
  private       LinkedHashMap<String, Object> materialized;
  // set once the `record` key was removed from the row: it no longer answers the record
  private       boolean                       recordKeyRemoved;

  ScoredRecordRow(final Document record, final String scoreKey, final float score) {
    this.record = record;
    this.scoreKey = scoreKey;
    this.score = score;
  }

  /**
   * The record this row was built from, whatever a write to the row's {@code record} key did since.
   */
  Document getRecord() {
    return record;
  }

  @Override
  public Object get(final Object key) {
    if (materialized != null) {
      final Object value = materialized.get(key);
      return value != null || materialized.containsKey(key) || !isRecordKey(key) ? value : record;
    }
    if (!(key instanceof String name))
      return null;
    if (name.equals(scoreKey))
      return score;
    if (name.equals(RID))
      return record.getIdentity();
    if (name.equals(TYPE))
      return record.getTypeName();
    if (name.equals(RECORD))
      // a property of the same name wins over the record, as it did in the eager copy
      return record.has(RECORD) ? record.get(RECORD) : record;
    return record.get(name);
  }

  @Override
  public boolean containsKey(final Object key) {
    if (materialized != null)
      return materialized.containsKey(key) || isRecordKey(key);
    if (!(key instanceof String name))
      return false;
    return name.equals(scoreKey) || name.equals(RID) || name.equals(TYPE) || name.equals(RECORD) || record.has(name);
  }

  @Override
  public boolean isEmpty() {
    // never empty: the record, @rid, @type and the score are always there
    return materialized != null && materialized.isEmpty();
  }

  @Override
  public int size() {
    return materialize().size();
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    return materialize().entrySet();
  }

  @Override
  public Set<String> keySet() {
    return materialize().keySet();
  }

  @Override
  public Collection<Object> values() {
    return materialize().values();
  }

  @Override
  public Object put(final String key, final Object value) {
    return materialize().put(key, value);
  }

  @Override
  public Object remove(final Object key) {
    final LinkedHashMap<String, Object> copy = materialize();
    if (!RECORD.equals(key))
      return copy.remove(key);
    // whether the key held a real entry or only answered the record, it answers nothing once removed
    final boolean answeredTheRecord = isRecordKey(key) && !copy.containsKey(RECORD);
    recordKeyRemoved = true;
    return answeredTheRecord ? record : copy.remove(key);
  }

  @Override
  public void putAll(final Map<? extends String, ?> m) {
    materialize().putAll(m);
  }

  @Override
  public void clear() {
    materialize().clear();
    recordKeyRemoved = true;
  }

  /**
   * True for the {@code record} key while it still answers the record without being an entry of the eager copy.
   */
  private boolean isRecordKey(final Object key) {
    return !recordKeyRemoved && RECORD.equals(key);
  }

  private LinkedHashMap<String, Object> materialize() {
    if (materialized == null) {
      final Set<String> names = record.getPropertyNames();
      final LinkedHashMap<String, Object> copy = new LinkedHashMap<>(names.size() + 5);
      for (final String prop : names)
        copy.put(prop, record.get(prop));
      copy.put(RID, record.getIdentity());
      copy.put(TYPE, record.getTypeName());
      copy.put(scoreKey, score);
      materialized = copy;
    }
    return materialized;
  }
}
