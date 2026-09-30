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
  private final Map<Object, List<RID>> byKey         = new HashMap<>();
  private final List<RID>              nonIntegral   = new ArrayList<>();

  /** Drains {@code scan}, loading each record's {@code propertyName}. The records' order is kept within a key. */
  ScanPropertyHashIndex(final Iterator<Identifiable> scan, final String propertyName) {
    while (scan.hasNext()) {
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
        byKey.computeIfAbsent(value, k -> new ArrayList<>(2)).add(rid);
      else if (isIntegral(value))
        byKey.computeIfAbsent(((Number) value).longValue(), k -> new ArrayList<>(2)).add(rid);
      else if (value instanceof Number)
        nonIntegral.add(rid);
    }
  }

  /** True when {@link #candidates(Object)} can answer for this expected value. */
  static boolean isSupported(final Object expected) {
    return expected instanceof String || isIntegral(expected);
  }

  /** The records that may equal {@code expected} (a supported value), in scan order within a key. */
  Iterator<Identifiable> candidates(final Object expected) {
    final List<RID> exact = byKey.get(expected instanceof String ? expected : (Object) ((Number) expected).longValue());
    if (expected instanceof String || nonIntegral.isEmpty())
      return exact == null ? Collections.emptyIterator() : cast(exact);

    final List<Identifiable> merged = new ArrayList<>(nonIntegral.size() + (exact != null ? exact.size() : 0));
    if (exact != null)
      merged.addAll(exact);
    merged.addAll(nonIntegral);
    return merged.iterator();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Iterator<Identifiable> cast(final List<RID> list) {
    return (Iterator) list.iterator();
  }

  private static boolean isIntegral(final Object value) {
    return value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte
        || value instanceof BigInteger;
  }
}
