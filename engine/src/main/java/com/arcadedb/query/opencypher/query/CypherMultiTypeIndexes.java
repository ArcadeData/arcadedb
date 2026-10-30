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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement.IndexKind;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

/**
 * Remembers the vector and full-text indexes that Cypher created over several labels or relationship types (issue #10914):
 * {@code CREATE FULLTEXT INDEX docs FOR (n:Article|Post) ON EACH [n.title, n.body]}.
 * <p>
 * An ArcadeDB index belongs to one type, so such a statement builds one index per type and records here the logical name that
 * ties them together. The record is a custom value of every labelled type ({@value #KEY_PREFIX}{@code <name>}), which persists
 * with the schema and replicates with it. The procedures that take an index name ({@code db.index.fulltext.queryNodes},
 * {@code db.index.vector.queryNodes}, the {@code SEARCH} clause), {@code SHOW INDEXES} and {@code DROP INDEX} resolve the name
 * through {@link #resolve}.
 * <p>
 * The entry holds the kind, the labels, the properties, the per-type indexes the logical index spans ({@code indexes}) and,
 * among them, the ones this statement created ({@code owned}): dropping the logical index drops only those, never an index
 * that was there before it.
 * <p>
 * <b>Scores.</b> The vector side merges cleanly when the per-type indexes share a similarity function, which the statement
 * enforces. The full-text side merges the per-type answers by score, but BM25 statistics (document frequency, average length)
 * are those of each per-type index, so a score is relative to its own type and not exactly the score one Neo4j index spanning
 * all the labels would give.
 * <p>
 * <b>SQL.</b> The per-type indexes are ordinary indexes, so SQL searches each type through its own index name
 * ({@code SEARCH_INDEX('Post[title,body]', ...)}, {@code vector.neighbors('Post[e]', ...)}) and what SQL writes is in the index
 * the logical name resolves to. The logical name itself is not known to SQL: a query across the types is one SQL query per type.
 */
public final class CypherMultiTypeIndexes {
  static final String KEY_PREFIX = "cypher.multiindex.";

  /**
   * A logical index as recorded.
   *
   * @param name         the name the index is queried and dropped by
   * @param kind         {@code VECTOR} or {@code FULLTEXT}
   * @param relationship true for an index over relationship types
   * @param labels       the labels or relationship types it spans, in the order the statement named them
   * @param properties   the indexed properties
   * @param indexes      the names of the per-type indexes it spans, one per label
   * @param owned        the names of the per-type indexes the statement created
   */
  public record Entry(String name, IndexKind kind, boolean relationship, List<String> labels, List<String> properties,
                      List<String> indexes, List<String> owned) {
  }

  private CypherMultiTypeIndexes() {
  }

  /** The record of the logical index with that name, or null when none has it. Names are unique across the database. */
  public static Entry find(final Schema schema, final String name) {
    if (name == null)
      return null;
    final String key = KEY_PREFIX + name;
    for (final DocumentType type : schema.getTypes()) {
      final Entry entry = read(name, type.getCustomValue(key));
      if (entry != null)
        return entry;
    }
    return null;
  }

  /** Every logical index of the database, in no particular order. */
  public static List<Entry> entries(final Schema schema) {
    final List<Entry> entries = new ArrayList<>();
    final Set<String> seen = new HashSet<>();
    for (final DocumentType type : schema.getTypes())
      for (final String key : type.getCustomKeys()) {
        if (!key.startsWith(KEY_PREFIX))
          continue;
        final String name = key.substring(KEY_PREFIX.length());
        if (seen.contains(name))
          continue;
        final Entry entry = read(name, type.getCustomValue(key));
        if (entry != null) {
          seen.add(name);
          entries.add(entry);
        }
      }
    return entries;
  }

  /**
   * The per-type indexes the logical index of that name spans, or null when the name is not a logical index. An index of the
   * entry that was dropped by other means is skipped.
   */
  public static List<TypeIndex> resolve(final Schema schema, final String name) {
    final Entry entry = find(schema, name);
    if (entry == null)
      return null;
    final List<TypeIndex> indexes = new ArrayList<>(entry.indexes().size());
    for (final String indexName : entry.indexes())
      if (schema.getIndexByName(indexName) instanceof TypeIndex typeIndex)
        indexes.add(typeIndex);
    return indexes;
  }

  /** The names of the per-type indexes some logical index created, which {@code SHOW INDEXES} lists under the logical name. */
  static List<String> ownedIndexNames(final Schema schema) {
    final List<String> owned = new ArrayList<>();
    for (final Entry entry : entries(schema))
      owned.addAll(entry.owned());
    return owned;
  }

  static void register(final Schema schema, final Entry entry) {
    final String json = new JSONObject().put("kind", entry.kind().name()).put("relationship", entry.relationship())
        .put("labels", new JSONArray(entry.labels())).put("properties", new JSONArray(entry.properties()))
        .put("indexes", new JSONArray(entry.indexes())).put("owned", new JSONArray(entry.owned())).toString();
    for (final String label : entry.labels())
      schema.getType(label).setCustomValue(KEY_PREFIX + entry.name(), json);
  }

  /** Forgets the logical index, from every type that holds its record. The per-type indexes are not touched. */
  static void remove(final Schema schema, final String name) {
    final String key = KEY_PREFIX + name;
    for (final DocumentType type : schema.getTypes())
      if (type.getCustomValue(key) != null)
        type.setCustomValue(key, null);
  }

  private static Entry read(final String name, final Object value) {
    if (value == null)
      return null;
    try {
      final JSONObject json = new JSONObject(value.toString());
      return new Entry(name, IndexKind.valueOf(json.getString("kind")), json.getBoolean("relationship", false),
          strings(json.getJSONArray("labels", new JSONArray())), strings(json.getJSONArray("properties", new JSONArray())),
          strings(json.getJSONArray("indexes", new JSONArray())), strings(json.getJSONArray("owned", new JSONArray())));
    } catch (final RuntimeException e) {
      // unreadable: not a logical index
      LogManager.instance().log(CypherMultiTypeIndexes.class, Level.WARNING, "Unreadable record of logical index '%s': %s", name,
          e.getMessage());
      return null;
    }
  }

  private static List<String> strings(final JSONArray array) {
    final List<String> list = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      list.add(array.getString(i));
    return list;
  }
}
