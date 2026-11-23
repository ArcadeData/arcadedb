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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.LongRangeList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests of {@link DeletedReferences}: what a row hands on of the nodes and relationships a statement deleted, held in
 * another alias, a list, a map or a path (issue #12548).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class DeletedReferencesTest extends TestHelper {
  private final Set<RID> deleted = new HashSet<>();
  private Vertex         alive;
  private Vertex         doomed;
  private Edge           relationship;

  @BeforeEach
  void createRecords() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("n", "alive").save();
      final MutableVertex b = database.newVertex("V").set("n", "doomed").save();
      alive = a;
      doomed = b;
      relationship = a.newEdge("E", b).save();
    });
    deleted.clear();
  }

  private DeletedReferences references() {
    return new DeletedReferences(record -> deleted.contains(record.getIdentity()));
  }

  private ResultInternal row(final Object... namesAndValues) {
    final ResultInternal row = new ResultInternal();
    for (int i = 0; i < namesAndValues.length; i += 2)
      row.setProperty((String) namesAndValues[i], namesAndValues[i + 1]);
    return row;
  }

  @Test
  void aRowThatHoldsNothingDeletedReadsWhatItHolds() {
    final List<Object> list = new ArrayList<>(List.of(alive, "text", 1L));
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("node", alive);
    map.put("number", 42L);
    final ResultInternal row = row("alias", alive, "list", list, "map", map, "number", 7L);

    references().mark(row);

    assertThat((Object) row.getProperty("alias")).isSameAs(alive);
    assertThat((Object) row.getProperty("number")).isEqualTo(7L);
    assertThat(row.<List<Object>>getProperty("list")).containsExactly(alive, "text", 1L);
    assertThat(row.<Map<String, Object>>getProperty("map")).containsEntry("node", alive).containsEntry("number", 42L);
  }

  @Test
  void aCollectionOfScalarsIsLeftAsTheObjectTheRowCameWith() {
    final List<Object> list = new ArrayList<>(List.of("text", 1L, 2.5d));
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("k", 2L);
    map.put("s", "text");
    final DeletedReferences references = references();
    final ResultInternal first = row("list", list, "map", map);
    final ResultInternal second = row("list", list, "map", map);

    references.mark(first);
    references.mark(second);

    assertThat((Object) first.getProperty("list")).isSameAs(list);
    assertThat((Object) first.getProperty("map")).isSameAs(map);
    assertThat((Object) second.getProperty("list")).isSameAs(list);
  }

  @Test
  void aDeletedNodeInAnotherAliasReadsAsTheMarker() {
    deleted.add(doomed.getIdentity());
    final ResultInternal row = row("a", alive, "b", doomed);

    references().mark(row);

    assertThat((Object) row.getProperty("a")).isSameAs(alive);
    assertThat((Object) row.getProperty("b")).isSameAs(DeletedEntityMarker.INSTANCE);
  }

  @Test
  void aDeletedRelationshipReadsAsAMarkerThatKeepsItsType() {
    deleted.add(relationship.getIdentity());
    final ResultInternal row = row("r", relationship, "rs", List.of(relationship));

    references().mark(row);

    assertThat((Object) row.getProperty("r")).isInstanceOf(DeletedEntityMarker.class);
    assertThat(row.<DeletedEntityMarker>getProperty("r").getRelationshipType()).isEqualTo("E");
    final Object inList = row.<List<Object>>getProperty("rs").get(0);
    assertThat(inList).isInstanceOf(DeletedEntityMarker.class);
    assertThat(((DeletedEntityMarker) inList).getRelationshipType()).isEqualTo("E");
  }

  @Test
  void aListReadsTheDeletedElementsAsMarkersAndKeepsItsSize() {
    deleted.add(doomed.getIdentity());
    final List<Object> list = List.of(alive, doomed, "text");
    final ResultInternal row = row("list", list);

    references().mark(row);

    final List<Object> read = row.getProperty("list");
    assertThat(read).hasSize(3);
    assertThat(read.get(0)).isSameAs(alive);
    assertThat(read.get(1)).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(read.get(2)).isEqualTo("text");
    // every way of reading the list answers the same
    assertThat(new ArrayList<>(read)).containsExactly(alive, DeletedEntityMarker.INSTANCE, "text");
    assertThat(read.toArray()).containsExactly(alive, DeletedEntityMarker.INSTANCE, "text");
    assertThat(read.contains(DeletedEntityMarker.INSTANCE)).isTrue();
    assertThat(read.indexOf("text")).isEqualTo(2);
    // and the list the row came with is untouched
    assertThat(list.get(1)).isSameAs(doomed);
  }

  @Test
  void aViewIsLiveSoARowServedBeforeALaterDeletionReadsIt() {
    final List<Object> list = new ArrayList<>(List.of(alive, doomed));
    final ResultInternal row = row("list", list);
    references().mark(row);
    final List<Object> read = row.getProperty("list");
    assertThat(read.get(1)).isSameAs(doomed);

    deleted.add(doomed.getIdentity());

    assertThat(read.get(1)).isSameAs(DeletedEntityMarker.INSTANCE);
  }

  @Test
  void aMapReadsTheDeletedValuesAsMarkers() {
    deleted.add(doomed.getIdentity());
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("survivor", alive);
    map.put("dead", doomed);
    final ResultInternal row = row("map", map);

    references().mark(row);

    final Map<String, Object> read = row.getProperty("map");
    assertThat(read).hasSize(2);
    assertThat(read.get("survivor")).isSameAs(alive);
    assertThat(read.get("dead")).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(read.containsKey("dead")).isTrue();
    assertThat(read.keySet()).containsExactly("survivor", "dead");
    assertThat(read.values()).containsExactly(alive, DeletedEntityMarker.INSTANCE);
    assertThat(read.entrySet()).hasSize(2);
  }

  @Test
  void nestedCollectionsAndPathsAreReadThroughTheSameCheck() {
    deleted.add(doomed.getIdentity());
    final Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("items", List.of(alive, doomed));
    final TraversalPath path = new TraversalPath(alive);
    path.addStep(relationship, (Vertex) doomed);
    final ResultInternal row = row("nested", List.of(inner), "path", path);

    references().mark(row);

    final List<Object> nested = row.getProperty("nested");
    @SuppressWarnings("unchecked")
    final Map<String, Object> readInner = (Map<String, Object>) nested.get(0);
    @SuppressWarnings("unchecked")
    final List<Object> readItems = (List<Object>) readInner.get("items");
    assertThat(readItems.get(0)).isSameAs(alive);
    assertThat(readItems.get(1)).isSameAs(DeletedEntityMarker.INSTANCE);

    final TraversalPath readPath = row.getProperty("path");
    assertThat(readPath.getNodesOrDeleted()).containsExactly(alive, DeletedEntityMarker.INSTANCE);
    assertThat(readPath.getRelationshipsOrDeleted()).containsExactly(relationship);
  }

  @Test
  void aLazyRangeIsLeftAsItIs() {
    final LongRangeList range = new LongRangeList(0L, 1L, 1_000_000);
    final ResultInternal row = row("range", range);

    references().mark(row);

    assertThat((Object) row.getProperty("range")).isSameAs(range);
  }

  @Test
  void theRowsThatShareACollectionShareItsView() {
    deleted.add(doomed.getIdentity());
    final List<Object> shared = new ArrayList<>(List.of(alive, doomed));
    final DeletedReferences references = references();
    final ResultInternal first = row("list", shared);
    final ResultInternal second = row("list", shared);

    references.mark(first);
    references.mark(second);

    assertThat((Object) second.getProperty("list")).isSameAs(first.getProperty("list"));
  }

  @Test
  void theRowsThatShareAPathShareItsCheckedCopy() {
    deleted.add(doomed.getIdentity());
    final TraversalPath path = new TraversalPath(alive);
    path.addStep(relationship, (Vertex) doomed);
    final DeletedReferences references = references();
    final ResultInternal first = row("path", path);
    final ResultInternal second = row("path", path);

    references.mark(first);
    references.mark(second);

    assertThat((Object) first.getProperty("path")).isNotSameAs(path);
    assertThat((Object) second.getProperty("path")).isSameAs(first.getProperty("path"));
  }

  @Test
  void aViewIsNotWrappedTwiceByTheSameCheck() {
    deleted.add(doomed.getIdentity());
    final DeletedReferences references = references();
    final ResultInternal row = row("list", new ArrayList<>(List.of(alive, doomed)));

    references.mark(row);
    final Object view = row.getProperty("list");
    references.mark(row);

    assertThat((Object) row.getProperty("list")).isSameAs(view);
  }

  @Test
  void aCheckAddedByAnotherStepIsAskedToo() {
    final Set<RID> other = new HashSet<>();
    final List<Object> list = new ArrayList<>(List.of(alive, doomed));
    final ResultInternal row = row("list", list);

    references().mark(row);
    new DeletedReferences(record -> other.contains(record.getIdentity())).mark(row);
    other.add(alive.getIdentity());
    deleted.add(doomed.getIdentity());

    final List<Object> read = row.getProperty("list");
    assertThat(read.get(0)).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(read.get(1)).isSameAs(DeletedEntityMarker.INSTANCE);
  }

  @Test
  void anUnrelatedValueIsNotTouched() {
    final Document document = alive;
    final ResultInternal row = row("text", "x", "nothing", null, "number", 1L, "doc", document);

    references().mark(row);

    assertThat((Object) row.getProperty("text")).isEqualTo("x");
    assertThat((Object) row.getProperty("nothing")).isNull();
    assertThat((Object) row.getProperty("doc")).isSameAs(document);
  }
}
