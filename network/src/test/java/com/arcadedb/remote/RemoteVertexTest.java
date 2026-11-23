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
package com.arcadedb.remote;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RemoteVertexTest {

  private RemoteDatabase mockDatabase;
  private Vertex         mockVertex;
  private Identifiable   mockToVertex;
  private RemoteVertex   remoteVertex;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockVertex = mock(Vertex.class);
    mockToVertex = mock(Identifiable.class);

    when(mockVertex.getIdentity()).thenReturn(new RID(1, 0));
    when(mockToVertex.getIdentity()).thenReturn(new RID(2, 0));

    // Return an incomplete result so command() is called but edge construction fails.
    // Mockito records the command() call before the exception, so ArgumentCaptor still works.
    final Result mockResult = mock(Result.class);
    when(mockResult.getEdge()).thenReturn(Optional.empty());
    final ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(mockResult);

    when(mockDatabase.command(eq("sql"), any(String.class), any(Map.class))).thenReturn(mockResultSet);

    remoteVertex = new RemoteVertex(mockVertex, mockDatabase);
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgeDoesNotInterpolateStringPropertyIntoSql() {
    // String values with apostrophes must be passed as SQL parameters, not interpolated.
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, "lastName", "O'Brien"))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).doesNotContain("O'Brien");
    assertThat(sqlCaptor.getValue()).contains(":p0");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsEntry(":p0", "O'Brien");
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgePassesNonStringPropertyAsParameter() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, "score", 42))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).contains(":p0");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsEntry(":p0", 42);
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgePassesMultiplePropertiesAsParameters() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, "name", "O'Brien", "since", 2023))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    final String sql = sqlCaptor.getValue();
    assertThat(sql).doesNotContain("O'Brien");
    assertThat(sql).contains("`name` = :p0");
    assertThat(sql).contains("`since` = :p1");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsEntry(":p0", "O'Brien");
    assertThat(params).containsEntry(":p1", 2023);
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgeWithMapPropertiesUsesParameterBinding() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, Map.of("label", "it's a test")))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).doesNotContain("it's a test");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsValue("it's a test");
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgeWithNoPropertiesCallsCommandWithEmptyParams() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).doesNotContain("set");
    assertThat(paramsCaptor.getValue()).isEmpty();
  }

  @Test
  void getVerticesWithLimitAndSkipEmitsSqlWithSkipLimit() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.hasNext()).thenReturn(false);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(emptyRs);

    remoteVertex.getVertices(Vertex.DIRECTION.OUT, 100, 50, "Knows");

    final String sql = sqlCaptor.getValue();
    assertThat(sql).contains("out(").contains(":t0").doesNotContain("'Knows'");
    assertThat(sql).contains("SKIP 50");
    assertThat(sql).contains("LIMIT 100");
  }

  @Test
  void getVerticesDefaultUsesDefaultPageSize() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.hasNext()).thenReturn(false);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(emptyRs);

    // iterator is lazy - must consume it to trigger the first page fetch
    remoteVertex.getVertices(Vertex.DIRECTION.OUT, "Knows").iterator().hasNext();

    final String sql = sqlCaptor.getValue();
    assertThat(sql).doesNotContain("SKIP");
    assertThat(sql).contains("LIMIT " + RemoteVertex.DEFAULT_PAGE_SIZE);
  }

  @Test
  void getVerticesPagedFetchesMultiplePagesUntilLastPageIsSmaller() {
    // Two full pages of size 2, then one partial page of size 1: each page of edges is followed by the query of its vertices.
    // A page resumes after the last edge RID of the previous one, never at an offset (issue #12191).
    final ResultSet edges1 = edgeResultSetOf(edgeResult(1, 1), edgeResult(2, 2));
    final ResultSet vertices1 = resultSetOf(vertexResult(1), vertexResult(2));
    final ResultSet edges2 = edgeResultSetOf(edgeResult(3, 3), edgeResult(4, 4));
    final ResultSet vertices2 = resultSetOf(vertexResult(3), vertexResult(4));
    final ResultSet edges3 = edgeResultSetOf(edgeResult(5, 5));
    final ResultSet vertices3 = resultSetOf(vertexResult(5));

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class)))
        .thenReturn(edges1, vertices1, edges2, vertices2, edges3, vertices3);

    final List<Vertex> collected = new ArrayList<>();
    for (final Vertex v : remoteVertex.getVerticesPaged(Vertex.DIRECTION.OUT, 2, "Knows"))
      collected.add(v);

    assertThat(collected).extracting(Vertex::getIdentity)
        .containsExactly(new RID(1, 1), new RID(1, 2), new RID(1, 3), new RID(1, 4), new RID(1, 5));

    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(6);
    assertThat(queries.get(0)).contains("outE(").doesNotContain("SKIP").doesNotContain("WHERE").contains("LIMIT 2");
    assertThat(queries.get(1)).contains("select from [#1:1, #1:2]");
    assertThat(queries.get(2)).contains("WHERE @rid > #3:2").doesNotContain("SKIP").contains("LIMIT 2");
    assertThat(queries.get(3)).contains("select from [#1:3, #1:4]");
    assertThat(queries.get(4)).contains("WHERE @rid > #3:4").doesNotContain("SKIP").contains("LIMIT 2");
    assertThat(queries.get(5)).contains("select from [#1:5]");
  }

  @Test
  void getEdgesWithLimitAndSkipEmitsSqlWithSkipLimit() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.hasNext()).thenReturn(false);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(emptyRs);

    remoteVertex.getEdges(Vertex.DIRECTION.BOTH, 200, 0, "Knows");

    final String sql = sqlCaptor.getValue();
    assertThat(sql).contains("bothE(").contains(":t0").doesNotContain("'Knows'");
    assertThat(sql).doesNotContain("SKIP");
    assertThat(sql).contains("LIMIT 200");
  }

  @Test
  void getEdgesPagedFetchesMultiplePages() {
    final Result e1 = edgeResult(1);
    final Result e2 = edgeResult(2);

    final ResultSet page1 = edgeResultSetOf(e1, e2);
    final ResultSet page2 = edgeResultSetOf();

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class)))
        .thenReturn(page1)
        .thenReturn(page2);

    final List<Edge> collected = new ArrayList<>();
    for (final Edge e : remoteVertex.getEdgesPaged(Vertex.DIRECTION.OUT, 2))
      collected.add(e);

    assertThat(collected).hasSize(2);
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries.get(0)).contains("outE(").doesNotContain("SKIP").contains("ORDER BY @rid LIMIT 2");
    assertThat(queries.get(1)).contains("WHERE @rid > #3:2").doesNotContain("SKIP").contains("LIMIT 2");
  }

  @Test
  void getEdgesPagedResumesLightweightEdgesAtTheirFarVertexAndSkipsOnlyTheirTwins() {
    // every lightweight edge of a bucket prints as #3:-1, so the bound alone cannot advance past them: the cursor is that RID,
    // the vertex at the other end of the last one read and the number of its twins (same endpoints) already read
    final ResultSet page1 = edgeResultSetOf(edgeResult(-1, 1), edgeResult(-1, 1));
    final ResultSet page2 = edgeResultSetOf(edgeResult(-1, 1), edgeResult(5));
    final ResultSet page3 = edgeResultSetOf(edgeResult(6));

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(page1, page2, page3);

    final List<Edge> collected = new ArrayList<>();
    for (final Edge e : remoteVertex.getEdgesPaged(Vertex.DIRECTION.OUT, 2))
      collected.add(e);

    assertThat(collected).hasSize(5);
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(3);
    assertThat(queries.get(1)).contains("WHERE @rid > #3:-1 AND (@rid >= #3:0 OR @in >= #1:1) ORDER BY @rid SKIP 2 LIMIT 2");
    assertThat(queries.get(2)).contains("WHERE @rid > #3:5 ORDER BY @rid LIMIT 2");
  }

  @Test
  void getEdgesPagedCountsOnlyTheTwinsOfTheLastLightweightEdgeAcrossPages() {
    // the first page ends on the vertex 2 after an edge to the vertex 1: only the edges to the vertex 2 are skipped, and they
    // add up while the next pages keep ending on it
    final ResultSet page1 = edgeResultSetOf(edgeResult(-1, 1), edgeResult(-1, 2));
    final ResultSet page2 = edgeResultSetOf(edgeResult(-1, 2), edgeResult(-1, 2));
    final ResultSet page3 = edgeResultSetOf(edgeResult(-1, 4));

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(page1, page2, page3);

    final List<Edge> collected = new ArrayList<>();
    for (final Edge e : remoteVertex.getEdgesPaged(Vertex.DIRECTION.OUT, 2))
      collected.add(e);

    assertThat(collected).hasSize(5);
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(3);
    assertThat(queries.get(0)).doesNotContain("WHERE");
    assertThat(queries.get(1)).contains("WHERE @rid > #3:-1 AND (@rid >= #3:0 OR @in >= #1:2) ORDER BY @rid SKIP 1 LIMIT 2");
    assertThat(queries.get(2)).contains("WHERE @rid > #3:-1 AND (@rid >= #3:0 OR @in >= #1:2) ORDER BY @rid SKIP 3 LIMIT 2");
  }

  @Test
  void getEdgesPagedBothReadsTheOutgoingThenTheIncomingEdges() {
    final ResultSet outEdges = edgeResultSetOf(edgeResult(1));
    final ResultSet inEdges = edgeResultSetOf(edgeResult(2));
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(outEdges, inEdges);

    final List<Edge> collected = new ArrayList<>();
    for (final Edge e : remoteVertex.getEdgesPaged(Vertex.DIRECTION.BOTH, 2))
      collected.add(e);

    assertThat(collected).extracting(Edge::getIdentity).containsExactly(new RID(3, 1), new RID(3, 2));
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(2);
    assertThat(queries.get(0)).contains("outE(").doesNotContain("WHERE");
    assertThat(queries.get(1)).contains("inE(").doesNotContain("WHERE");
  }

  @Test
  void pagedIterationRefusesANonPositivePageSize() {
    assertThatThrownBy(() -> remoteVertex.getEdgesPaged(Vertex.DIRECTION.OUT, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> remoteVertex.getVerticesPaged(Vertex.DIRECTION.OUT, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getVerticesPagedFetchesTheNextEdgePageWhenEveryVertexOfAPageWasDeleted() {
    // the first page of edges is full, but both of its vertices were deleted before their query ran
    final ResultSet edges1 = edgeResultSetOf(edgeResult(1, 1), edgeResult(2, 2));
    final ResultSet vertices1 = resultSetOf();
    final ResultSet edges2 = edgeResultSetOf(edgeResult(3, 3));
    final ResultSet vertices2 = resultSetOf(vertexResult(3));

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class)))
        .thenReturn(edges1, vertices1, edges2, vertices2);

    final List<Vertex> collected = new ArrayList<>();
    for (final Vertex v : remoteVertex.getVerticesPaged(Vertex.DIRECTION.OUT, 2))
      collected.add(v);

    assertThat(collected).extracting(Vertex::getIdentity).containsExactly(new RID(1, 3));
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(4);
    assertThat(queries.get(1)).contains("select from [#1:1, #1:2]");
    assertThat(queries.get(2)).contains("outE(").contains("WHERE @rid > #3:2").contains("LIMIT 2");
    assertThat(queries.get(3)).contains("select from [#1:3]");
  }

  @Test
  void getVerticesPagedEndsCleanlyWhenTheEdgeCountIsAMultipleOfThePageSize() {
    // a full page of edges, then an empty one: no vertex query for the empty page, and the iterator ends
    final ResultSet edges1 = edgeResultSetOf(edgeResult(1, 1), edgeResult(2, 2));
    final ResultSet vertices1 = resultSetOf(vertexResult(1), vertexResult(2));
    final ResultSet edges2 = edgeResultSetOf();

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(edges1, vertices1, edges2);

    final Iterator<Vertex> it = remoteVertex.getVerticesPaged(Vertex.DIRECTION.OUT, 2).iterator();
    final List<Vertex> collected = new ArrayList<>();
    while (it.hasNext())
      collected.add(it.next());

    assertThat(collected).extracting(Vertex::getIdentity).containsExactly(new RID(1, 1), new RID(1, 2));
    assertThat(it.hasNext()).isFalse();
    assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(3);
    assertThat(queries.get(2)).contains("WHERE @rid > #3:2").contains("LIMIT 2");
  }

  @Test
  void getEdgesPagedEndsCleanlyWhenTheEdgeCountIsAMultipleOfThePageSize() {
    final ResultSet page1 = edgeResultSetOf(edgeResult(1), edgeResult(2));
    final ResultSet page2 = edgeResultSetOf(edgeResult(3), edgeResult(4));
    final ResultSet page3 = edgeResultSetOf();
    when(mockDatabase.queryUncounted(eq("sql"), any(String.class), any(Map.class))).thenReturn(page1, page2, page3);

    final Iterator<Edge> it = remoteVertex.getEdgesPaged(Vertex.DIRECTION.OUT, 2).iterator();
    final List<Edge> collected = new ArrayList<>();
    while (it.hasNext())
      collected.add(it.next());

    assertThat(collected).extracting(Edge::getIdentity)
        .containsExactly(new RID(3, 1), new RID(3, 2), new RID(3, 3), new RID(3, 4));
    assertThat(it.hasNext()).isFalse();
    assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    verify(mockDatabase, times(3)).queryUncounted(eq("sql"), any(String.class), any(Map.class));
  }

  @Test
  void getVerticesPagedSplitsALargePageOfVertexRidsIntoBoundedQueries() {
    final int pageSize = RemoteVertex.MAX_RIDS_PER_QUERY + 1;
    final Result[] edgeResults = new Result[pageSize];
    for (int i = 0; i < pageSize; i++)
      edgeResults[i] = edgeResult(i, i);

    final ResultSet edges1 = edgeResultSetOf(edgeResults);
    final ResultSet vertices1 = resultSetOf(vertexResult(0));
    final ResultSet vertices2 = resultSetOf(vertexResult(pageSize - 1));
    final ResultSet edges2 = edgeResultSetOf();

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.queryUncounted(eq("sql"), sqlCaptor.capture(), any(Map.class))).thenReturn(edges1, vertices1, vertices2, edges2);

    final List<Vertex> collected = new ArrayList<>();
    for (final Vertex v : remoteVertex.getVerticesPaged(Vertex.DIRECTION.OUT, pageSize))
      collected.add(v);

    assertThat(collected).extracting(Vertex::getIdentity).containsExactly(new RID(1, 0), new RID(1, pageSize - 1));
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(4);
    assertThat(queries.get(1)).startsWith("select from [#1:0, ").endsWith(", #1:" + (pageSize - 2) + "]");
    assertThat(queries.get(1).split(",")).hasSize(RemoteVertex.MAX_RIDS_PER_QUERY);
    assertThat(queries.get(2)).isEqualTo("select from [#1:" + (pageSize - 1) + "]");
    assertThat(queries.get(3)).contains("WHERE @rid > #3:" + (pageSize - 1));
  }

  // --- helpers ---

  private Result vertexResult(final int bucketPos) {
    final Vertex v = mock(Vertex.class);
    when(v.getIdentity()).thenReturn(new RID(1, bucketPos));
    final Result r = mock(Result.class);
    when(r.getVertex()).thenReturn(Optional.of(v));
    return r;
  }

  private Result edgeResult(final int bucketPos) {
    final Edge e = mock(Edge.class);
    when(e.getIdentity()).thenReturn(new RID(3, bucketPos));
    final Result r = mock(Result.class);
    when(r.getEdge()).thenReturn(Optional.of(e));
    return r;
  }

  /** An outgoing edge #3:bucketPos whose incoming vertex is #1:inVertexPos. */
  private Result edgeResult(final int bucketPos, final int inVertexPos) {
    final Result r = edgeResult(bucketPos);
    when(r.getEdge().get().getIn()).thenReturn(new RID(1, inVertexPos));
    return r;
  }

  private ResultSet resultSetOf(final Result... results) {
    final ResultSet rs = mock(ResultSet.class);
    final boolean[] hasNextAnswers = new boolean[results.length + 1];
    for (int i = 0; i < results.length; i++)
      hasNextAnswers[i] = true;
    hasNextAnswers[results.length] = false;

    final Boolean[] hasNextBoxed = new Boolean[hasNextAnswers.length];
    for (int i = 0; i < hasNextAnswers.length; i++)
      hasNextBoxed[i] = hasNextAnswers[i];

    if (hasNextBoxed.length > 1) {
      final Boolean first = hasNextBoxed[0];
      final Boolean[] rest = Arrays.copyOfRange(hasNextBoxed, 1, hasNextBoxed.length);
      when(rs.hasNext()).thenReturn(first, rest);
    } else {
      when(rs.hasNext()).thenReturn(false);
    }

    if (results.length > 0) {
      final Result first = results[0];
      final Result[] rest = Arrays.copyOfRange(results, 1, results.length);
      when(rs.next()).thenReturn(first, rest);
    }
    return rs;
  }

  private ResultSet edgeResultSetOf(final Result... results) {
    return resultSetOf(results);
  }
}
