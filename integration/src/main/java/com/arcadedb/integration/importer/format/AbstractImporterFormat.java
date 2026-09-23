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
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.Database;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;

public abstract class AbstractImporterFormat implements FormatImporter {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"' };

  protected IndexCursor lookupRecord(final Database database, final String typeName, final String typeIdProperty, final Object id) {
    return database.lookupByKey(typeName, typeIdProperty, id);
  }

  /**
   * The total, non-polymorphic record count across every schema type of the given kind (e.g. every
   * {@code VertexType} or every {@code EdgeType}), used by a format whose underlying reader (TinkerPop's own
   * GraphML/GraphSON reader) gives it no per-record callback to count through as it writes. Taken before and after
   * such a read, the delta is what that read actually created - accurate on a database that already held data
   * before the import started, unlike a raw total (issue #8054).
   * <p>
   * {@code countType(name, false)}, not polymorphic: a polymorphic count would add a subtype's records into both
   * its own total and its supertype's, double-counting them.
   * <p>
   * Assumes single-writer: a write to the same kind of record from outside this import, landing between the
   * before and after snapshot, is indistinguishable from one this read made and pollutes the delta. Fine for the
   * common case of a freshly created database nobody else touches during the import, but not fine if the import
   * is joining a caller-owned transaction (issue #8073) against a database the caller keeps using concurrently.
   * <p>
   * Also undercounts a {@code LIGHTWEIGHT} edge type: {@code countType(name, false)} sums bucket entries, and a
   * lightweight edge has none - {@code GraphEngine.newEdge}'s lightweight branch never calls {@code edge.save()}.
   * An import that creates edges of a type already declared (or later made) {@code LIGHTWEIGHT} undercounts them
   * in the delta this method feeds.
   */
  protected static long countRecordsOfKind(final Database database, final Class<? extends DocumentType> kind) {
    long total = 0;
    for (final DocumentType type : database.getSchema().getTypes())
      if (kind.isInstance(type))
        total += database.countType(type.getName(), false);
    return total;
  }

  protected String getStringContent(final String value) {
    return getStringContent(value, STRING_CONTENT_SKIP);
  }

  protected String getStringContent(final String value, final char[] chars) {
    if (value.length() > 1) {
      final char begin = value.charAt(0);

      for (int i = 0; i < chars.length - 1; i += 2) {
        if (begin == chars[i]) {
          final char end = value.charAt(value.length() - 1);
          if (end == chars[i + 1])
            return value.substring(1, value.length() - 1);
        }
      }
    }
    return value;
  }

}
