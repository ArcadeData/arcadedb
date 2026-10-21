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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

/**
 * Points every alias of a row that binds one record at the version of it a write just produced.
 * <p>
 * Cypher has one binding per node per row: {@code MATCH (n {name:'a'}) MATCH (m {name:'a'})} gives {@code n} and
 * {@code m} the same node, so a write through either of them must be observable through both. ArcadeDB gets there
 * by pointing the aliases at the same object, because a row holds record snapshots rather than references - the
 * {@code MutableDocument} a write produced is a different object from the {@code ImmutableDocument} the MATCH put
 * in the row, and an alias left holding the latter keeps answering with the state from before the write.
 * <p>
 * {@code SET} has done this since #5227; {@code REMOVE} did not, so a removed property stayed visible through every
 * other alias of the same node while {@code SET n.p = null} - the same write, spelled differently - answered null
 * through all of them (issue #6328). This is that one helper, shared by both steps, the way
 * {@link LabelReplacements} is the one place that points a row's aliases at a record a label write <i>replaced</i>.
 * <p>
 * The row's own aliases are redirected, and so are the nodes and relationships of the paths it carries: a path variable
 * is another alias of the nodes it walks, and {@code MATCH p = (a)-[:R]->(b) REMOVE b.k RETURN last(nodes(p))} must
 * answer with the {@code b} the REMOVE wrote (issue #10513). A path is short, so the walk is charged only to the rows
 * that carry one. Records nested inside a list or a map are not followed, a deliberate difference from
 * {@link LabelReplacements#redirect}: there the original record is <b>deleted</b>, so a stale reference anywhere in the
 * row is a failure waiting to happen and the descent has to be paid for. Here the original is alive and merely older,
 * which costs a stale read in an unusual shape rather than an error, and a walk of a {@code collect()} list would be
 * charged to every row of every {@code SET} in exchange.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RowAliases {

  private RowAliases() {
  }

  /**
   * After a write produced {@code updated} from {@code original}, points every alias of the row that still binds
   * that record - by RID, which is what makes two aliases the same node - at the updated version.
   *
   * @param row      the row being processed
   * @param original the record the write started from
   * @param updated  the record the write produced
   */
  // Nothing here consults LabelReplacements' map, and nothing needs to: this answers "the row holds an older copy
  // of a record that is still there", which is a different question from "the row holds a record that was deleted
  // and replaced". A label write needs both, and SetStep.applyLabels asks them in that order - propagateUpdate for
  // the aliases of the vertex it is about to move, then LabelReplacements.redirect once it has moved.
  public static void propagateUpdate(final Result row, final Document original, final Document updated) {
    final RID originalRid = original.getIdentity();
    if (originalRid == null)
      return;
    for (final String name : row.getPropertyNames()) {
      final Object value = row.getProperty(name);
      if (value instanceof Document other) {
        if (other != updated && originalRid.equals(other.getIdentity()))
          ((ResultInternal) row).setProperty(name, updated);
      } else if (value instanceof TraversalPath path) {
        final TraversalPath live = path.withRecord(originalRid, updated);
        if (live != path)
          ((ResultInternal) row).setProperty(name, live);
      }
    }
  }
}
