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

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An upstream candidate row with its score replaced: what {@code vector.boost} and {@code vector.discover} emit. It answers
 * exactly what the copy those functions used to build answers - every key of the source row, a {@link Result}'s identity as
 * {@code @rid}, and the new score under {@code scoreKey} overriding a key of the same name - but reads the source row on demand
 * instead of copying it (issue #10170). Over a {@link ScoredRecordRow} that means the record's properties are read only when
 * asked for, and a downstream {@code vector.rerank} or {@code vector.mmr} still finds the carried {@code record}.
 * <p>
 * Enumerating the row ({@link #entrySet()}, {@link #size()}, {@code SELECT *}, JSON serialization) or writing to it builds the
 * eager copy once and answers from it afterwards, so a write lands on this row and never on the upstream one. A source that is a
 * plain map, which its holder could still change, is copied at once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ScoreOverrideRow extends AbstractMap<String, Object> {
  private static final String RID_KEY = "@rid";

  private final Object                        source;
  private final String                        scoreKey;
  private final Object                        score;
  private       LinkedHashMap<String, Object> materialized;

  /**
   * @param source a {@link Map} or a {@link Result} row
   */
  ScoreOverrideRow(final Object source, final String scoreKey, final float score) {
    if (!(source instanceof Map<?, ?>) && !(source instanceof Result))
      throw new IllegalArgumentException("A scored row is a map or a result, got " + (source == null ? "null" : source.getClass()));
    this.source = source;
    this.scoreKey = scoreKey;
    this.score = score;
    // A plain map row is copied now, as it always was: whoever holds it can still change it, and the score computed from its
    // values must not end up next to other values, or another @rid. Only the rows backed by a record (the lazy output of the
    // vector functions) and the read-only results are read on demand
    if (source instanceof Map<?, ?> && !(source instanceof ScoredRecordRow) && !(source instanceof ScoreOverrideRow))
      materialize();
  }

  /**
   * The row this one re-scores.
   */
  Object getSource() {
    return source;
  }

  @Override
  public Object get(final Object key) {
    if (materialized != null)
      return materialized.get(key);
    if (!(key instanceof String name))
      return null;
    if (name.equals(scoreKey))
      return score;
    if (source instanceof Map<?, ?> map)
      return map.get(name);
    final Result result = (Result) source;
    if (name.equals(RID_KEY)) {
      // the eager copy let the identity overwrite a property of the same name
      final Optional<RID> identity = result.getIdentity();
      if (identity.isPresent())
        return identity.get();
    }
    return result.getProperty(name);
  }

  @Override
  public boolean containsKey(final Object key) {
    if (materialized != null)
      return materialized.containsKey(key);
    if (!(key instanceof String name))
      return false;
    if (name.equals(scoreKey))
      return true;
    if (source instanceof Map<?, ?> map)
      return map.containsKey(name);
    final Result result = (Result) source;
    return result.hasProperty(name) || (name.equals(RID_KEY) && result.getIdentity().isPresent());
  }

  @Override
  public boolean isEmpty() {
    // never empty: the score is always there
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
    return materialize().remove(key);
  }

  @Override
  public void putAll(final Map<? extends String, ?> m) {
    materialize().putAll(m);
  }

  @Override
  public void clear() {
    materialize().clear();
  }

  private LinkedHashMap<String, Object> materialize() {
    if (materialized == null) {
      final LinkedHashMap<String, Object> copy = new LinkedHashMap<>();
      if (source instanceof Map<?, ?> map) {
        for (final Map.Entry<?, ?> e : map.entrySet())
          copy.put(String.valueOf(e.getKey()), e.getValue());
      } else {
        final Result result = (Result) source;
        for (final String prop : result.getPropertyNames())
          copy.put(prop, result.getProperty(prop));
        result.getIdentity().ifPresent(identity -> copy.put(RID_KEY, identity));
      }
      copy.put(scoreKey, score);
      materialized = copy;
    }
    return materialized;
  }
}
