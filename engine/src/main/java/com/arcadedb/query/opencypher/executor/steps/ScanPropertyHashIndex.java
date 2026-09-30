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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Throw-away hash index over one property of a full type scan, built when a chained MATCH with an inline equality
 * on a row-dependent value (e.g. {@code OPTIONAL MATCH (c:T {name: row.x})}) has no real index to use (issue #8695).
 * Without it every outer row re-scans the whole type, so the step costs rows x records.
 * <p>
 * It only narrows the candidates: the caller still runs the complete pattern/WHERE filter on each of them, so the
 * answer cannot differ from the scan's. To keep that true for the comparison rules of
 * {@code InlineProperties.matchesResolvedValue} (equals, or numeric equality across types) the lookup is offered
 * only for a String or an integral number; a stored value that could equal an integral number without being one
 * (a Double, a Float, a BigDecimal) is kept aside and returned for every numeric lookup. A null never matches, so
 * records without the property are not stored at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ScanPropertyHashIndex {
  /** Above this many stored Double/Float/BigDecimal values a numeric lookup would return them all, so it declines instead. */
  static final int MAX_NON_INTEGRAL = 64;

  // value: a RID for a key held by one record (the common case, no list allocated), else an ArrayList<RID>
  private final Map<Object, Object> byKey       = new HashMap<>();
  private final List<RID>           nonIntegral = new ArrayList<>();

  /** Drains {@code scan}, loading each record's {@code propertyName}; {@code guard} enforces the command deadline. The records' order is kept within a key. */
  ScanPropertyHashIndex(final Iterator<Identifiable> scan, final String propertyName, final WorkGuard guard) {
    while (scan.hasNext()) {
      guard.check();
      final Identifiable identifiable = scan.next();
      final Document record;
      try {
        record = identifiable.asDocument();
      } catch (final RecordNotFoundException e) {
        continue;
      }
      final Object value = record.get(propertyName);
      if (value == null)
        continue;

      final RID rid = record.getIdentity();
      if (value instanceof String)
        put(value, rid);
      else if (isIntegral(value))
        put(((Number) value).longValue(), rid);
      else if (value instanceof Number)
        nonIntegral.add(rid);
    }
  }

  @SuppressWarnings("unchecked")
  private void put(final Object key, final RID rid) {
    final Object existing = byKey.putIfAbsent(key, rid);
    if (existing == null)
      return;
    if (existing instanceof List)
      ((List<RID>) existing).add(rid);
    else {
      final List<RID> list = new ArrayList<>(4);
      list.add((RID) existing);
      list.add(rid);
      byKey.put(key, list);
    }
  }

  /** True when {@link #candidates(Object)} can answer for this expected value. */
  static boolean isSupported(final Object expected) {
    return expected instanceof String || isIntegral(expected);
  }

  /**
   * The records that may equal {@code expected} (a supported value), in scan order within a key, or null when the hash
   * cannot answer cheaply - a numeric lookup against a type holding many non-integral numbers - and the caller must scan.
   */
  @SuppressWarnings("unchecked")
  Iterator<Identifiable> candidates(final Object expected) {
    final boolean text = expected instanceof String;
    if (!text && nonIntegral.size() > MAX_NON_INTEGRAL)
      return null;

    final Object exact = byKey.get(text ? expected : (Object) ((Number) expected).longValue());
    if (text || nonIntegral.isEmpty()) {
      if (exact == null)
        return Collections.emptyIterator();
      return exact instanceof List ? (Iterator<Identifiable>) (Iterator<?>) ((List<RID>) exact).iterator()
          : Collections.<Identifiable>singletonList((RID) exact).iterator();
    }

    final List<Identifiable> merged = new ArrayList<>(nonIntegral.size() + 1);
    if (exact instanceof List)
      merged.addAll((List<RID>) exact);
    else if (exact != null)
      merged.add((RID) exact);
    merged.addAll(nonIntegral);
    return merged.iterator();
  }

  private static boolean isIntegral(final Object value) {
    return value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte
        || value instanceof BigInteger;
  }
}
