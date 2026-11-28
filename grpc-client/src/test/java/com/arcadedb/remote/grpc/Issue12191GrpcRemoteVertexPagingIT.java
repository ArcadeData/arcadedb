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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.remote.RemoteImmutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12191: the paging of the neighbourhood of a vertex ({@code RemoteVertex}) is shared by the HTTP and the gRPC driver, and
 * it used SKIP over an edge list that shrinks while the caller deletes, so deleting the edges or the vertices while iterating
 * removed only part of them. The gRPC driver answers the paged queries on its own wire, so it is exercised here too, with a page
 * size small enough to need several pages.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class Issue12191GrpcRemoteVertexPagingIT extends BaseGrpcClientServerTest {
  private static final String VERTEX    = "V12191G";
  private static final String EDGE      = "E12191G";
  private static final int    LEAVES    = 45;
  private static final int    PAGE_SIZE = 10;

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    // HERE AND NOT IN A SEPARATE @AfterEach: THE ORDER OF TWO OF THEM IN ONE CLASS IS UNDEFINED, AND super.endTest() DROPS THE DATABASE
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void openAndPopulate() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "create vertex type " + VERTEX);
    grpc.command("sql", "create edge type " + EDGE);
    grpc.command("sql", "create vertex " + VERTEX + " set name = 'hub'");
    final StringBuilder leaves = new StringBuilder("INSERT INTO " + VERTEX + " (name, i) VALUES ");
    for (int i = 0; i < LEAVES; i++)
      leaves.append(i > 0 ? ", " : "").append("('leaf', ").append(i).append(')');
    grpc.command("sql", leaves.toString());
    grpc.command("sql",
        "create edge " + EDGE + " from (select from " + VERTEX + " where name = 'hub') to (select from " + VERTEX
            + " where name = 'leaf')");
    assertThat(getServerDatabase(0, getDatabaseName()).countType(EDGE, true)).isEqualTo(LEAVES);
  }

  private RemoteImmutableVertex hub() {
    return (RemoteImmutableVertex) grpc.query("sql", "select from " + VERTEX + " where name = 'hub'").next().getVertex().get();
  }

  @Test
  void deletingTheEdgesWhileIteratingDeletesThemAll() {
    int n = 0;
    for (final Edge e : hub().getEdgesPaged(Vertex.DIRECTION.OUT, PAGE_SIZE)) {
      e.delete();
      ++n;
    }
    assertThat(n).isEqualTo(LEAVES);
    assertThat(getServerDatabase(0, getDatabaseName()).countType(EDGE, true)).isZero();
  }

  @Test
  void deletingTheVerticesWhileIteratingDeletesThemAll() {
    int n = 0;
    for (final Vertex v : hub().getVerticesPaged(Vertex.DIRECTION.OUT, PAGE_SIZE)) {
      assertThat(v.getString("name")).isEqualTo("leaf");
      v.delete();
      ++n;
    }
    assertThat(n).isEqualTo(LEAVES);
    assertThat(getServerDatabase(0, getDatabaseName()).countType(VERTEX, true)).isEqualTo(1);
  }

  @Test
  void lightweightEdgesDeletedBetweenPagesDoNotHideLiveOnes() {
    // the lightweight edges of a bucket share one RID: a page resumes by the vertex at the other end of the last one read
    final Database embeddedDatabase = getServerDatabase(0, getDatabaseName());
    final int pageSize = 3;
    for (final boolean vertices : new boolean[] { false, true }) {
      final List<RID> leaves = new ArrayList<>();
      final RID lightHub = createLightweightHub(embeddedDatabase, leaves);

      final RemoteImmutableVertex remoteHub = (RemoteImmutableVertex) grpc.lookupByRID(lightHub).asVertex();
      final Iterator<?> it = vertices ?
          remoteHub.getVerticesPaged(Vertex.DIRECTION.OUT, pageSize).iterator() :
          remoteHub.getEdgesPaged(Vertex.DIRECTION.OUT, pageSize).iterator();
      final List<RID> read = new ArrayList<>();
      for (int i = 0; i < pageSize; i++)
        read.add(farEndpoint(it.next()));

      // another client deletes the first edge already read
      final RID firstRead = read.getFirst();
      embeddedDatabase.transaction(() -> {
        for (final Edge edge : embeddedDatabase.lookupByRID(lightHub, true).asVertex().getEdges(Vertex.DIRECTION.OUT, EDGE))
          if (edge.getIn().equals(firstRead)) {
            edge.delete();
            return;
          }
        throw new AssertionError("No lightweight edge to " + firstRead);
      });

      while (it.hasNext())
        read.add(farEndpoint(it.next()));

      assertThat(read).as("%s of the lightweight hub", vertices ? "vertices" : "edges").containsExactlyInAnyOrderElementsOf(leaves);
    }
  }

  /** A hub with a lightweight edge to each of eight new leaves, which {@code leaves} receives in RID order. */
  private static RID createLightweightHub(final Database embeddedDatabase, final List<RID> leaves) {
    final RID[] hub = new RID[1];
    embeddedDatabase.transaction(() -> {
      final MutableVertex hubVertex = embeddedDatabase.newVertex(VERTEX).set("name", "lightHub").save();
      hub[0] = hubVertex.getIdentity();
      final List<MutableVertex> leafVertices = new ArrayList<>();
      for (int i = 0; i < 8; i++)
        leafVertices.add(embeddedDatabase.newVertex(VERTEX).set("name", "lightLeaf").save());
      leafVertices.sort(Comparator.comparing(MutableVertex::getIdentity));
      for (final MutableVertex leaf : leafVertices) {
        leaves.add(leaf.getIdentity());
        hubVertex.newLightEdge(EDGE, leaf);
      }
    });
    return hub[0];
  }

  /** The vertex at the other end of an OUT edge or, for a vertex, the vertex itself. */
  private static RID farEndpoint(final Object element) {
    return element instanceof Vertex vertex ? vertex.getIdentity() : ((Edge) element).getIn();
  }
}
