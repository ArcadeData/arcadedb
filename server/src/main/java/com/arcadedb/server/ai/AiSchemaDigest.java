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
package com.arcadedb.server.ai;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.info.SchemaInfo;
import com.arcadedb.server.security.DatabaseUserContext;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The compact schema summary the AI Assistant sends with EVERY question (portal repository, {@code docs/AI-ASSISTANT.md}, section 13),
 * and the detail of one type the {@code get_type} tool returns when the summary is not enough.
 * <p>
 * Plain text, one line per type, because braces, quotes and indentation cost tokens and a line of words costs the least:
 * <pre>
 * Database shop: 212 types, 40 shown in full
 * vertex Customer ~1.2M rows, 8 buckets, extends Party "A buyer": id STRING!, name STRING!, email STRING, +4 more | idx: id UNIQUE; email; name+city
 * Others (172): Log ~10M, Session ~3M, Tmp1 ~0, ...
 * </pre>
 * It is BOUNDED ({@link #MAX_CHARS}), whatever the schema: types come largest first, each shows at most {@link #MAX_PROPERTIES}
 * properties, and the types that do not fit are listed by name and size only. So a schema of 500 types with 8 buckets each costs
 * the model what a small one does.
 * <ul>
 *   <li><b>Row counts</b> are the buckets' own counters (an atomic read, no I/O, never a scan). A bucket whose counter is not known
 *   yet (it is rebuilt lazily after a crash) makes the figure {@code ~?}: the model can run {@code SELECT count(*)} if it needs it.
 *   The count is of the type's OWN records, as the line says; a parent's polymorphic total is not summed.</li>
 *   <li><b>Markers:</b> {@code !} after a property type means mandatory, {@code nn} not null, {@code ro} read-only. Indexes list their
 *   properties joined with {@code +}, then {@code UNIQUE}, or the index type when it is not a plain LSM tree ({@code FULL_TEXT}...).</li>
 *   <li><b>Untrusted text.</b> Names and descriptions are the customer's data and end up in a prompt, so they are cut to a length,
 *   lose control, bidirectional, zero-width and private-use characters, and cannot break the line format.</li>
 *   <li><b>Permissions:</b> a type the user may not read is neither listed nor counted.</li>
 * </ul>
 */
public final class AiSchemaDigest {
  /** The whole summary stays under this many characters. */
  public static final int MAX_CHARS = 40_000;
  /** What is kept aside for the "Others" line, so a long list of small types cannot push the big ones out. */
  static final int OTHERS_CHARS = 8_000;
  static final int MAX_PROPERTIES = 12;
  static final int MAX_INDEXES = 8;
  static final int MAX_PARENTS = 4;
  static final int MAX_NAME = 64;
  static final int MAX_DESCRIPTION = 120;
  /** The most of one {@code get_type} answer. */
  static final int MAX_TYPE_CHARS = 40_000;
  static final int MAX_TYPE_PROPERTIES = 300;
  static final int MAX_TYPE_BUCKETS = 200;

  private static final int MAX_CACHED = 64;

  /** How long a built summary is reused. Mutable and package-private only so a test can shorten it. */
  static volatile long cacheTtlMs = 45_000L;

  private record Cached(String digest, long builtAt) {
  }

  /** Small and recent: a conversation asks several questions in a few minutes, and nothing needs to invalidate it. */
  private static final Map<String, Cached> CACHE = new LinkedHashMap<>(16, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(final Map.Entry<String, Cached> eldest) {
      return size() > MAX_CACHED;
    }
  };

  private AiSchemaDigest() {
  }

  /** One type as the summary sees it. */
  private record Entry(DocumentType type, long rows) {
  }

  // ---- the summary --------------------------------------------------------------------------------------------------------------

  /**
   * The summary of a database for an authenticated user, reused for {@link #cacheTtlMs}. The principal is bound while the schema is
   * read, so the engine's per-user gates apply, and the cache is per user because what a user may read differs.
   *
   * @throws IllegalArgumentException when the database does not exist
   * @throws SecurityException        when the user may not access it
   */
  public static String forUser(final ArcadeDBServer server, final ServerSecurityUser user, final String databaseName) {
    if (!server.existsDatabase(databaseName))
      throw new IllegalArgumentException("Database '" + databaseName + "' does not exist");
    if (!user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");

    final String key = user.getName() + '\u0000' + databaseName;
    final long now = System.currentTimeMillis();
    synchronized (CACHE) {
      final Cached cached = CACHE.get(key);
      if (cached != null && now - cached.builtAt() < cacheTtlMs)
        return cached.digest();
    }
    final DatabaseInternal database = server.getDatabase(databaseName);
    final String digest = DatabaseUserContext.runAs(database, user, () -> build(database, databaseName));
    synchronized (CACHE) {
      CACHE.put(key, new Cached(digest, now));
    }
    return digest;
  }

  /** Forgets every cached summary (tests). */
  static void clearCache() {
    synchronized (CACHE) {
      CACHE.clear();
    }
  }

  /** Builds the summary. Performs no authorization of the database itself; {@link #forUser} does. */
  public static String build(final Database database, final String databaseName) {
    final Schema schema = database.getSchema();
    final List<Entry> entries = new ArrayList<>();
    for (final DocumentType type : schema.getTypes()) {
      if (!readable(database, type))
        continue;
      entries.add(new Entry(type, ownRows(type)));
    }
    // Largest first (unknown last), then by name: deterministic
    entries.sort(Comparator.comparingLong(Entry::rows).reversed().thenComparing(e -> e.type().getName()));

    final StringBuilder lines = new StringBuilder(8192);
    final int fullBudget = MAX_CHARS - OTHERS_CHARS - 600;
    int shown = 0;
    int index = 0;
    for (; index < entries.size(); index++) {
      final String line = line(entries.get(index));
      if (lines.length() + line.length() + 1 > fullBudget)
        break;
      lines.append(line).append('\n');
      shown++;
    }

    final StringBuilder out = new StringBuilder(lines.length() + 1024);
    out.append("Database ").append(clean(databaseName, MAX_NAME)).append(": ").append(entries.size()).append(" types, ").append(shown)
        .append(" shown in full\n");
    out.append("~N rows = own records of that type (~? unknown); ! mandatory, nn not null, ro read-only. For everything about a type, "
        + "call get_type(name).\n");
    out.append(lines);

    if (index < entries.size()) {
      final StringBuilder others = new StringBuilder(OTHERS_CHARS);
      final int remaining = entries.size() - index;
      others.append("Others (").append(remaining).append("): ");
      final int budget = Math.min(OTHERS_CHARS, MAX_CHARS - out.length() - 80);
      int listed = 0;
      for (int i = index; i < entries.size(); i++) {
        final Entry e = entries.get(i);
        final String item = (listed > 0 ? ", " : "") + clean(e.type().getName(), MAX_NAME) + " ~" + count(e.rows());
        if (others.length() + item.length() > budget)
          break;
        others.append(item);
        listed++;
      }
      if (listed < remaining)
        others.append(listed > 0 ? ", " : "").append("+").append(remaining - listed).append(" more");
      out.append(others).append('\n');
    }
    return out.toString();
  }

  private static String line(final Entry entry) {
    final DocumentType type = entry.type();
    final StringBuilder sb = new StringBuilder(256);
    sb.append(kind(type)).append(' ').append(clean(type.getName(), MAX_NAME)).append(" ~").append(count(entry.rows())).append(" rows, ")
        .append(type.getBuckets(false).size()).append(" buckets");

    final List<DocumentType> parents = type.getSuperTypes();
    if (!parents.isEmpty()) {
      sb.append(", extends ");
      for (int i = 0; i < parents.size() && i < MAX_PARENTS; i++)
        sb.append(i > 0 ? "," : "").append(clean(parents.get(i).getName(), MAX_NAME));
      if (parents.size() > MAX_PARENTS)
        sb.append(",+").append(parents.size() - MAX_PARENTS);
    }
    final String description = description(type.getCustomValue("description"));
    if (!description.isEmpty())
      sb.append(" \"").append(description).append('"');

    final Collection<TypeIndex> typeIndexes = type.getAllIndexes(false);
    final List<String> indexes = indexes(typeIndexes);
    appendProperties(sb, type, typeIndexes);
    if (!indexes.isEmpty()) {
      sb.append(" | idx: ");
      for (int i = 0; i < indexes.size() && i < MAX_INDEXES; i++)
        sb.append(i > 0 ? "; " : "").append(indexes.get(i));
      if (indexes.size() > MAX_INDEXES)
        sb.append("; +").append(indexes.size() - MAX_INDEXES).append(" more");
    }
    return sb.toString();
  }

  /** The properties most useful for writing a query first: the indexed ones, then the mandatory ones, then by name. */
  private static void appendProperties(final StringBuilder sb, final DocumentType type, final Collection<TypeIndex> typeIndexes) {
    final Set<String> indexed = new HashSet<>();
    for (final TypeIndex index : typeIndexes)
      indexed.addAll(index.getPropertyNames());

    final List<Property> properties = new ArrayList<>();
    for (final Property p : type.getProperties())
      if (!p.isHidden())
        properties.add(p);
    if (properties.isEmpty())
      return;
    properties.sort(Comparator.<Property>comparingInt(p -> indexed.contains(p.getName()) ? 0 : p.isMandatory() ? 1 : 2)
        .thenComparing(Property::getName));

    sb.append(": ");
    final int shown = Math.min(properties.size(), MAX_PROPERTIES);
    for (int i = 0; i < shown; i++) {
      final Property p = properties.get(i);
      if (i > 0)
        sb.append(", ");
      sb.append(clean(p.getName(), MAX_NAME)).append(' ').append(typeName(p));
      if (p.isMandatory())
        sb.append('!');
      if (p.isNotNull())
        sb.append(" nn");
      if (p.isReadonly())
        sb.append(" ro");
    }
    if (properties.size() > shown)
      sb.append(", +").append(properties.size() - shown).append(" more");
  }

  private static String typeName(final Property p) {
    final String base = p.getType().name();
    final String of = p.getOfType();
    return of == null || of.isEmpty() ? base : base + "<" + clean(of, MAX_NAME) + ">";
  }

  /** {@code id UNIQUE}, {@code name+city}, {@code body FULL_TEXT}: sorted and without repeats. */
  private static List<String> indexes(final Collection<TypeIndex> typeIndexes) {
    final Set<String> out = new TreeSet<>();
    for (final TypeIndex index : typeIndexes) {
      final StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (final String name : index.getPropertyNames()) {
        sb.append(first ? "" : "+").append(clean(name, MAX_NAME));
        first = false;
      }
      if (sb.isEmpty())
        continue;
      if (index.isUnique())
        sb.append(" UNIQUE");
      if (index.getType() != Schema.INDEX_TYPE.LSM_TREE)
        sb.append(' ').append(index.getType().name());
      out.add(sb.toString());
    }
    return new ArrayList<>(out);
  }

  private static String kind(final DocumentType type) {
    if (type instanceof VertexType)
      return "vertex";
    if (type instanceof EdgeType)
      return "edge";
    if (type instanceof LocalTimeSeriesType)
      return "timeseries";
    return "doc";
  }

  /** Whether the bound user may read the type (always true when nobody is bound, as in an embedded or test run). */
  private static boolean readable(final Database database, final DocumentType type) {
    if (!(database instanceof DatabaseInternal internal))
      return true;
    try {
      internal.checkPermissionsOnType(type.getName(), SecurityDatabaseUser.ACCESS.READ_RECORD);
      return true;
    } catch (final SecurityException e) {
      return false;
    }
  }

  /**
   * The records of the type's own buckets from their counters: no I/O and never a scan. -1 when a counter is not known yet, and for a
   * time series (its samples live in an engine of their own, not in buckets).
   */
  static long ownRows(final DocumentType type) {
    if (type instanceof LocalTimeSeriesType)
      return -1L;
    long total = 0L;
    for (final Bucket bucket : type.getBuckets(false)) {
      if (!(bucket instanceof LocalBucket local))
        return -1L;
      final long cached = local.getCachedRecordCount();
      if (cached < 0L)
        return -1L;
      total += cached;
    }
    return total;
  }

  /** {@code 0}, {@code 950}, {@code 12K}, {@code 1.2M}, {@code 3.4B}, or {@code ?}. */
  static String count(final long rows) {
    if (rows < 0L)
      return "?";
    if (rows < 1_000L)
      return Long.toString(rows);
    final double value;
    final char unit;
    if (rows < 1_000_000L) {
      value = rows / 1_000d;
      unit = 'K';
    } else if (rows < 1_000_000_000L) {
      value = rows / 1_000_000d;
      unit = 'M';
    } else {
      value = rows / 1_000_000_000d;
      unit = 'B';
    }
    final String text = value >= 100d ? String.format(Locale.ROOT, "%.0f", value) : String.format(Locale.ROOT, "%.1f", value);
    return (text.endsWith(".0") ? text.substring(0, text.length() - 2) : text) + unit;
  }

  // ---- untrusted text -----------------------------------------------------------------------------------------------------------

  /**
   * A name or a description made safe for one line of a prompt: control characters (and the line and paragraph separators) become a
   * space, bidirectional overrides and isolates, zero-width characters, other format characters and private-use characters are removed,
   * runs of spaces collapse, and the result is cut to {@code maxChars} characters (never in the middle of a surrogate pair).
   */
  static String clean(final String text, final int maxChars) {
    if (text == null || text.isEmpty())
      return "";
    final StringBuilder sb = new StringBuilder(Math.min(text.length(), maxChars));
    boolean space = true; // also trims the start
    int chars = 0;
    for (int i = 0; i < text.length() && chars < maxChars; ) {
      final int cp = text.codePointAt(i);
      i += Character.charCount(cp);
      final int type = Character.getType(cp);
      if (Character.isISOControl(cp) || cp == 0x2028 || cp == 0x2029 || Character.isWhitespace(cp) || type == Character.SPACE_SEPARATOR) {
        if (!space) {
          sb.append(' ');
          chars++;
        }
        space = true;
        continue;
      }
      if (type == Character.FORMAT || type == Character.PRIVATE_USE || type == Character.SURROGATE || type == Character.UNASSIGNED)
        continue;
      sb.appendCodePoint(cp);
      chars++;
      space = false;
    }
    int end = sb.length();
    while (end > 0 && sb.charAt(end - 1) == ' ')
      end--;
    sb.setLength(end);
    return sb.toString();
  }

  /** A description is shown inside double quotes, so the quotes it contains are turned into apostrophes. */
  private static String description(final Object value) {
    return value == null ? "" : clean(value.toString(), MAX_DESCRIPTION).replace('"', '\'');
  }

  // ---- one type in full (the get_type tool) -----------------------------------------------------------------------------------

  /**
   * Everything about ONE type as JSON for the model: kind, parents, description, record counts (per bucket too), the properties it has
   * (inherited ones included) with their constraints, and the indexes. Performs no authorization; the caller does.
   *
   * @throws IllegalArgumentException when the type does not exist, naming close matches
   */
  public static JSONObject typeDetail(final Database database, final String typeName) {
    final Schema schema = database.getSchema();
    if (typeName == null || typeName.isBlank())
      throw new IllegalArgumentException("get_type requires a 'name' argument");
    if (!schema.existsType(typeName))
      throw new IllegalArgumentException(
          "Type '" + clean(typeName, MAX_NAME) + "' does not exist." + similar(database, typeName));
    final DocumentType type = schema.getType(typeName);
    if (!readable(database, type))
      throw new IllegalArgumentException("Type '" + clean(typeName, MAX_NAME) + "' does not exist or is not readable by this user.");

    final JSONObject json = SchemaInfo.typeToJSON(type);
    json.put("category", kind(type));
    final String description = description(type.getCustomValue("description"));
    if (!description.isEmpty())
      json.put("description", description);

    final long own = ownRows(type);
    if (own >= 0L)
      json.put("rows", own);
    final JSONArray buckets = new JSONArray();
    final List<Bucket> typeBuckets = type.getBuckets(false);
    for (int i = 0; i < typeBuckets.size() && i < MAX_TYPE_BUCKETS; i++) {
      final Bucket bucket = typeBuckets.get(i);
      final JSONObject b = new JSONObject().put("name", bucket.getName());
      if (bucket instanceof LocalBucket local && local.getCachedRecordCount() >= 0L)
        b.put("rows", local.getCachedRecordCount());
      buckets.put(b);
    }
    json.put("buckets", buckets);
    json.put("bucketCount", typeBuckets.size());

    // Every property the type has, inherited ones too (the summary lists a type's own properties only)
    final Set<String> ownNames = new HashSet<>();
    for (final Property p : type.getProperties())
      ownNames.add(p.getName());
    final JSONArray properties = new JSONArray();
    final Collection<? extends Property> all = type.getPolymorphicProperties();
    int n = 0;
    for (final Property p : all) {
      if (p.isHidden())
        continue;
      if (n++ >= MAX_TYPE_PROPERTIES) {
        json.put("propertiesTruncated", all.size() - MAX_TYPE_PROPERTIES);
        break;
      }
      final JSONObject pj = new JSONObject().put("name", clean(p.getName(), MAX_NAME)).put("type", p.getType().name());
      if (p.getOfType() != null)
        pj.put("ofType", clean(p.getOfType(), MAX_NAME));
      if (p.isMandatory())
        pj.put("mandatory", true);
      if (p.isNotNull())
        pj.put("notNull", true);
      if (p.isReadonly())
        pj.put("readonly", true);
      if (p.getDefaultValueDefinition() != null)
        pj.put("default", clean(p.getDefaultValueDefinition().toString(), MAX_DESCRIPTION));
      if (p.getMin() != null)
        pj.put("min", clean(p.getMin(), MAX_NAME));
      if (p.getMax() != null)
        pj.put("max", clean(p.getMax(), MAX_NAME));
      if (p.getRegexp() != null)
        pj.put("regexp", clean(p.getRegexp(), MAX_DESCRIPTION));
      final String propertyDescription = description(p.getCustomValue("description"));
      if (!propertyDescription.isEmpty())
        pj.put("description", propertyDescription);
      if (!ownNames.contains(p.getName()))
        pj.put("inherited", true);
      properties.put(pj);
    }
    json.put("properties", properties);

    // Cut what is left of an unusually large answer instead of handing the model a half-JSON
    if (json.toString().length() > MAX_TYPE_CHARS) {
      json.remove("buckets");
      json.put("bucketsOmitted", true);
    }
    return json;
  }

  /** {@code " Did you mean: A, B?"} for the few types whose names are close to the one asked for, or an empty string. */
  private static String similar(final Database database, final String typeName) {
    final String wanted = typeName.toLowerCase(Locale.ROOT);
    final Set<String> found = new TreeSet<>();
    for (final DocumentType t : database.getSchema().getTypes()) {
      final String name = t.getName();
      final String lower = name.toLowerCase(Locale.ROOT);
      if ((lower.contains(wanted) || wanted.contains(lower)) && readable(database, t))
        found.add(clean(name, MAX_NAME));
      if (found.size() >= 5)
        break;
    }
    return found.isEmpty() ? "" : " Did you mean: " + String.join(", ", found) + "?";
  }
}
