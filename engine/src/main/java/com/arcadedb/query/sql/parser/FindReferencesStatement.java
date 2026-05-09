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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * AST node for the OrientDB-compatible {@code FIND REFERENCES} statement.
 * <p>
 * Syntax: {@code FIND REFERENCES <rid|(<sub-query>)> [<class-or-bucket-list>]}
 * <p>
 * Scans the database for records that contain a link (direct or nested inside collections, maps, embedded documents)
 * pointing to one of the input RIDs and returns one row per (rid, referrer) pair.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FindReferencesStatement extends SimpleExecStatement {

  // Exactly one of `rid` / `subQuery` is set.
  protected Rid              rid;
  protected Statement        subQuery;
  // Optional restriction list: class names and bucket names. Null/empty = scan everything.
  protected List<Identifier> classes = new ArrayList<>();
  protected List<Identifier> buckets = new ArrayList<>();

  public FindReferencesStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeSimple(final CommandContext context) {
    final Database database = context.getDatabase();

    final Set<RID> targetRids = collectTargetRids(context);

    final InternalResultSet output = new InternalResultSet();
    if (targetRids.isEmpty())
      return output;

    final Set<String> typesToScan = resolveTypesToScan(database);

    for (final String typeName : typesToScan) {
      final Iterator<Record> iter = database.iterateType(typeName, false);
      while (iter.hasNext()) {
        final Record record = iter.next();
        if (!(record instanceof Document doc))
          continue;

        final Map<RID, Set<String>> matches = new LinkedHashMap<>();
        // Walk each property of the record; don't pass the record itself, otherwise the outer document
        // would be treated as a link to itself by scan().
        for (final String name : doc.getPropertyNames())
          scan(doc.get(name), name + ".", matches, targetRids);

        // Edges store @out / @in outside of getPropertyNames(); inspect them explicitly so that
        // FIND REFERENCES on a vertex returns the edges that link to it.
        if (doc instanceof Edge edge) {
          final RID out = edge.getOut();
          if (out != null && targetRids.contains(out))
            matches.computeIfAbsent(out, k -> new LinkedHashSet<>()).add("@out.");
          final RID in = edge.getIn();
          if (in != null && targetRids.contains(in))
            matches.computeIfAbsent(in, k -> new LinkedHashSet<>()).add("@in.");
        }

        if (matches.isEmpty())
          continue;

        for (final Map.Entry<RID, Set<String>> e : matches.entrySet()) {
          final ResultInternal r = new ResultInternal(database);
          r.setProperty("rid", e.getKey());
          r.setProperty("referredBy", doc.getIdentity());
          r.setProperty("fields", new ArrayList<>(e.getValue()));
          output.add(r);
        }
      }
    }

    return output;
  }

  private Set<RID> collectTargetRids(final CommandContext context) {
    final Set<RID> ids = new LinkedHashSet<>();

    if (rid != null) {
      final RID r = rid.toRecordId((Result) null, context);
      if (r != null)
        ids.add(r);
      return ids;
    }

    if (subQuery == null)
      throw new CommandExecutionException("FIND REFERENCES: missing target");

    final ResultSet rs = subQuery.execute(context.getDatabase(), (Map<String, Object>) null, context, false);
    try {
      while (rs.hasNext()) {
        final Result row = rs.next();
        // Prefer the RID of an Identifiable element when present.
        if (row.isElement()) {
          final Record el = row.getElement().get().getRecord();
          if (el != null)
            ids.add(el.getIdentity());
          continue;
        }
        // Otherwise scan all properties looking for any RID/Identifiable value.
        for (final String name : row.getPropertyNames()) {
          final Object val = row.getProperty(name);
          if (val instanceof RID idVal)
            ids.add(idVal);
          else if (val instanceof Identifiable identifiable && identifiable.getIdentity() != null)
            ids.add(identifiable.getIdentity());
        }
      }
    } finally {
      rs.close();
    }
    return ids;
  }

  private Set<String> resolveTypesToScan(final Database database) {
    final Schema schema = database.getSchema();
    final Set<String> result = new LinkedHashSet<>();

    if ((classes == null || classes.isEmpty()) && (buckets == null || buckets.isEmpty())) {
      for (final DocumentType t : schema.getTypes())
        result.add(t.getName());
      return result;
    }

    if (classes != null) {
      for (final Identifier cls : classes) {
        final String name = cls.getStringValue();
        final DocumentType type = schema.getType(name);
        result.add(type.getName());
        for (final DocumentType sub : type.getSubTypes())
          result.add(sub.getName());
      }
    }

    if (buckets != null) {
      for (final Identifier bucket : buckets) {
        final DocumentType t = schema.getTypeByBucketName(bucket.getStringValue());
        if (t == null)
          throw new CommandExecutionException("FIND REFERENCES: bucket '" + bucket.getStringValue() + "' is not associated to any type");
        result.add(t.getName());
      }
    }

    return result;
  }

  private static void scan(final Object value, final String path, final Map<RID, Set<String>> matches, final Set<RID> targets) {
    if (value == null)
      return;

    if (value instanceof RID idVal) {
      if (targets.contains(idVal))
        matches.computeIfAbsent(idVal, k -> new LinkedHashSet<>()).add(path);
      return;
    }

    if (value instanceof Identifiable identifiable && !(value instanceof EmbeddedDocument)) {
      final RID id = identifiable.getIdentity();
      if (id != null && targets.contains(id))
        matches.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(path);
      return;
    }

    if (value instanceof EmbeddedDocument embedded) {
      // Walk into embedded documents (their identity is internal, never a referrer).
      for (final String name : embedded.getPropertyNames())
        scan(embedded.get(name), path + name + ".", matches, targets);
      return;
    }

    if (value instanceof Document doc) {
      for (final String name : doc.getPropertyNames())
        scan(doc.get(name), path + name + ".", matches, targets);
      return;
    }

    if (value instanceof Collection<?> col) {
      for (final Object item : col)
        scan(item, path + "<elem>.", matches, targets);
      return;
    }

    if (value instanceof Map<?, ?> map) {
      for (final Map.Entry<?, ?> e : map.entrySet())
        scan(e.getValue(), path + "<map:" + e.getKey() + ">.", matches, targets);
      return;
    }
    // Scalars and other types are ignored.
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("FIND REFERENCES ");
    if (rid != null) {
      rid.toString(params, builder);
    } else if (subQuery != null) {
      builder.append("(");
      subQuery.toString(params, builder);
      builder.append(")");
    }
    if ((classes != null && !classes.isEmpty()) || (buckets != null && !buckets.isEmpty())) {
      builder.append(" [");
      boolean first = true;
      if (classes != null) {
        for (final Identifier c : classes) {
          if (!first)
            builder.append(", ");
          c.toString(params, builder);
          first = false;
        }
      }
      if (buckets != null) {
        for (final Identifier b : buckets) {
          if (!first)
            builder.append(", ");
          builder.append("bucket:");
          b.toString(params, builder);
          first = false;
        }
      }
      builder.append("]");
    }
  }

  public void validate() throws CommandSQLParsingException {
    if (rid == null && subQuery == null)
      throw new CommandSQLParsingException("FIND REFERENCES requires a RID or a sub-query as target");
  }

  @Override
  public FindReferencesStatement copy() {
    final FindReferencesStatement out = new FindReferencesStatement(-1);
    out.rid = rid == null ? null : rid.copy();
    out.subQuery = subQuery == null ? null : subQuery.copy();
    out.classes = new ArrayList<>(classes);
    out.buckets = new ArrayList<>(buckets);
    return out;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final FindReferencesStatement that = (FindReferencesStatement) o;
    return Objects.equals(rid, that.rid)
        && Objects.equals(subQuery, that.subQuery)
        && Objects.equals(classes, that.classes)
        && Objects.equals(buckets, that.buckets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid, subQuery, classes, buckets);
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  public Rid getRid() {
    return rid;
  }

  public void setRid(final Rid rid) {
    this.rid = rid;
  }

  public Statement getSubQuery() {
    return subQuery;
  }

  public void setSubQuery(final Statement subQuery) {
    this.subQuery = subQuery;
  }

  public List<Identifier> getClasses() {
    return classes;
  }

  public List<Identifier> getBuckets() {
    return buckets;
  }
}
