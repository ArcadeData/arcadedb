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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The passes of a per-record materialized view (issue #11475): a view whose rows map 1:1 to the records of its single source
 * type, each row carrying the RID of its source record in {@link #SOURCE_RID}, indexed UNIQUE.
 * <p>
 * The full pass ({@link #fullPass}) runs the defining query over the whole source and matches the rows to the backing records
 * through that link, where {@code MaterializedViewRefresher.writeSnapshot} matches them by scan order. Matching by scan order
 * would hand the link of one row to another on every pass where the order moved, and every such move is a remove and an add in
 * the unique index: matched by source, a row of an unchanged source keeps its key, and {@code DocumentIndexer.updateDocument}
 * skips the index entirely, as it does for the in-place rewrite of a non-per-record view.
 */
final class MaterializedViewPerRecordRefresher {
  static final String SOURCE_RID     = "_sourceRID";
  /** The parameter the delta pass binds the drained source RIDs to, in the RID-list variant of the derived query. */
  static final String RIDS_PARAMETER = "sourceRids";

  private MaterializedViewPerRecordRefresher() {
  }

  /**
   * Creates the {@link #SOURCE_RID} LINK property and its UNIQUE index on the view's backing type, each only if absent. Called
   * inside the schema frame that creates (or upgrades) the view, so HA replicas receive both with it; the caller holds the async
   * quiescence, which the index build needs (issue #10537).
   */
  static void addLink(final LocalSchema schema, final MaterializedViewImpl view) {
    final String backingTypeName = view.getBackingTypeName();
    final DocumentType backing = schema.getType(backingTypeName);
    if (!backing.existsProperty(SOURCE_RID))
      backing.createProperty(SOURCE_RID, Type.LINK);
    schema.buildTypeIndex(backingTypeName, new String[] { SOURCE_RID }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withIgnoreIfExists(true).create();
  }

  /**
   * The defining query with {@code @rid AS _sourceRID} added to its projection, rendered from a copy of the parsed statement. A
   * query with no projection ({@code SELECT FROM X}) returns the whole record, so it gains {@code *} as well.
   * <p>
   * {@link #SOURCE_RID} is a reserved column of the view (issue #11552). The classifier refuses a query that projects a column of
   * that name or reads a type declaring it through {@code *}, but a schemaless source record can still carry an undeclared field
   * of that name, as can a record of a type that declared it after the view was created: {@code *} copies it into the row. The
   * link always wins over it, because the rendered projection prints {@code *} before every other item and
   * {@code Projection.calculateSingle} writes the items in order, so the link added last overwrites the field. The field's own
   * value is not in the view.
   * <p>
   * The RID-list variant reads the records bound to {@link #RIDS_PARAMETER} instead of the source type, and keeps the WHERE with
   * {@code @this INSTANCEOF '<source>'} added: a RID-list target returns whatever record sits at each RID, whatever its type, and
   * the listener fires for the subtypes of the source too, which {@code INSTANCEOF} accepts.
   */
  static String buildDerivedQuery(final Database database, final String query, final boolean forRidList) {
    final Statement parsed = ((DatabaseInternal) database).getStatementCache().get(query);
    if (!(parsed instanceof SelectStatement select))
      throw new SchemaException("The defining query of a per-record materialized view is not a SELECT: " + query);

    final SelectStatement derived = select.copy();
    final SelectStatement sourceRidProjection = (SelectStatement) ((DatabaseInternal) database).getStatementCache()
        .get("SELECT @rid AS " + SOURCE_RID);
    final ProjectionItem sourceRid = sourceRidProjection.getProjection().getItems().getFirst().copy();

    final Projection projection = derived.getProjection();
    final List<ProjectionItem> items = new ArrayList<>();
    if (projection == null || projection.getItems() == null || projection.getItems().isEmpty()) {
      final ProjectionItem all = new ProjectionItem();
      all.setAll(true);
      items.add(all);
    } else
      items.addAll(projection.getItems());
    items.add(sourceRid);
    derived.projection = new Projection(items, projection != null && projection.isDistinct());

    if (forRidList) {
      if (select.getTarget() == null || select.getTarget().getItem() == null || select.getTarget().getItem().getIdentifier() == null)
        throw new SchemaException("The defining query of a per-record materialized view does not read one type: " + query);
      final String sourceType = select.getTarget().getItem().getIdentifier().getStringValue();
      // Built as text, and safe to: the WHERE is the parsed statement's own, rendered back; the type name is a parsed identifier,
      // put inside a quoted string with its backslashes and quotes escaped, so it cannot close the string; and the RIDs are not
      // in the text at all, but bound to the parameter at each pass. The result is parsed again just below
      final StringBuilder shape = new StringBuilder("SELECT FROM :").append(RIDS_PARAMETER).append(" WHERE ");
      if (select.getWhereClause() != null)
        shape.append('(').append(select.getWhereClause()).append(") AND ");
      shape.append("@this INSTANCEOF '").append(sourceType.replace("\\", "\\\\").replace("'", "\\'")).append('\'');
      final SelectStatement ridListShape = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(shape.toString());
      derived.setTarget(ridListShape.getTarget().copy());
      derived.setWhereClause(ridListShape.getWhereClause().copy());
    }
    return derived.toString();
  }

  /**
   * Brings the backing type in line with the defining query, matching each row to its backing record by {@link #SOURCE_RID}: a
   * row whose source already has a backing record is written over it in place, a row with none is created, and every backing
   * record whose source produced no row (deleted, or no longer matching the WHERE) is deleted. One transaction, under the
   * backing type's write scope, for the same reasons as {@code MaterializedViewRefresher.writeSnapshot}.
   * <p>
   * The records to delete are collected during the scan and deleted after it, never during it: an update whose content no
   * longer fits its slot can relocate a record, and the scan must not meet the same record twice or skip one.
   */
  static void fullPass(final Database database, final MaterializedViewImpl view) {
    final String backingTypeName = view.getBackingTypeName();
    final String derivedQuery = view.derivedQuery(false);

    try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view)) {
      database.transaction(() -> {
        final long count = database.countType(backingTypeName, false);
        final RecordIdentities produced = new RecordIdentities(count > 0 && count < Integer.MAX_VALUE ? (int) count : 16);

        try (final ResultSet rs = database.query("sql", derivedQuery)) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            final RID source = sourceOf(row.getProperty(SOURCE_RID));
            if (source == null)
              continue;
            produced.add(source);
            writeRow(database, backingTypeName, source, row);
          }
        }

        produced.sortAndDeduplicate();

        final RecordIdentities orphans = new RecordIdentities(16);
        database.scanType(backingTypeName, false, record -> {
          final RID source = sourceOf(((Document) record).get(SOURCE_RID));
          if (source == null || !produced.contains(source))
            orphans.add(record.getIdentity());
          return true;
        });
        for (int i = 0; i < orphans.size(); i++)
          database.lookupByRID(orphans.get(i), true).delete();
      }, false);
    }
  }

  /**
   * Re-evaluates the defining query for the given source records only, and brings their rows in line: a record the query still
   * produces gets its row written over in place (or created), a record it no longer produces (deleted, out of the WHERE, or now
   * of another type than the source) loses its row if it had one. One transaction, under the backing type's write scope, as the
   * full pass. Costs O(records given), whatever the size of the view.
   * <p>
   * Columns computed by following links or edges are recomputed only for these records (the accepted behavior change of #11475).
   *
   * @param rids the drained source records; sorted and deduplicated here, so the caller hands over what it will not read again
   */
  static void deltaPass(final Database database, final MaterializedViewImpl view, final RecordIdentities rids) {
    rids.sortAndDeduplicate();
    if (rids.size() == 0)
      return;

    final String backingTypeName = view.getBackingTypeName();
    final String derivedQuery = view.derivedQuery(true);
    final List<RID> targets = new ArrayList<>(rids.size());
    for (int i = 0; i < rids.size(); i++)
      targets.add(rids.get(i));
    final Map<String, Object> parameters = Map.of(RIDS_PARAMETER, targets);

    try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view)) {
      database.transaction(() -> {
        final RecordIdentities produced = new RecordIdentities(rids.size());
        try (final ResultSet rs = database.query("sql", derivedQuery, parameters)) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            final RID source = sourceOf(row.getProperty(SOURCE_RID));
            if (source == null)
              continue;
            produced.add(source);
            writeRow(database, backingTypeName, source, row);
          }
        }
        produced.sortAndDeduplicate();

        for (int i = 0; i < rids.size(); i++) {
          final RID source = rids.get(i);
          if (produced.contains(source))
            continue;
          final IndexCursor existing = database.lookupByKey(backingTypeName, SOURCE_RID, source);
          if (existing.hasNext())
            existing.next().getRecord(true).delete();
        }
      }, false);
    }
  }

  /** Writes a query row over the backing record linked to {@code source}, or into a new one when there is none. */
  private static void writeRow(final Database database, final String backingTypeName, final RID source, final Result row) {
    final MutableDocument doc;
    final IndexCursor existing = database.lookupByKey(backingTypeName, SOURCE_RID, source);
    if (existing.hasNext()) {
      doc = ((Document) existing.next().getRecord(true)).modify();
      // The row comes from the defining query, not from the existing record: no stale-read refusal (#8610)
      doc.clearBasedOnStaleRead();
    } else
      doc = database.newDocument(backingTypeName);
    MaterializedViewRefresher.applyRow(doc, row);
    doc.save();
  }

  private static RID sourceOf(final Object value) {
    return value instanceof Identifiable identifiable ? identifiable.getIdentity() : null;
  }
}
