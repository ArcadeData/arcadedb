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
package com.arcadedb.server.grpc;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.FileUtils;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13011: {@code ArcadeDbGrpcService.convertResultToGrpcRecord} and {@code convertToGrpcRecord} asked
 * {@code builder.getPropertiesMap().containsKey(name)} before adding each record attribute ({@code @rid}, {@code @type},
 * {@code @cat}, {@code @out}, {@code @in}). On a builder that call copies every property put so far into a new map, so an edge row
 * copied its whole property map five times (three in {@code convertToGrpcRecord}) only to answer five presence questions.
 * <p>
 * Measured by allocation, which is exact per thread, and not by time, which is not. A vertex and an edge that hold the same
 * properties are converted: the edge answers two more attributes ({@code @out}, {@code @in}), so it asked the question twice more
 * and cost two more copies of the map. The unit is what one such copy costs on this JVM, measured here on a builder that holds
 * the same properties: the edge may not cost even one copy more than the vertex.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13011GrpcRecordAttributeAllocationTest {

  private static final String DB_PATH    = "./target/databases/Issue13011GrpcRecordAttributeAllocationTest";
  private static final int    PROPERTIES = 200;
  private static final int    WARM_UP    = 300;
  private static final int    CALLS      = 300;

  private ArcadeDbGrpcService service;
  private Database            database;
  private Vertex              vertex;
  private Edge                edge;
  /** The last converted object: kept, so the JIT cannot drop the allocations it is asked to measure. */
  private Object              kept;

  @BeforeEach
  void createAnEdgeWithManyProperties() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().createVertexType("V13011");
    database.getSchema().createEdgeType("E13011");

    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex from = database.newVertex("V13011");
      for (int i = 0; i < PROPERTIES; i++)
        from.set("p" + i, 1_000 + i);
      from.save();
      final MutableVertex to = database.newVertex("V13011").save();
      final MutableEdge created = from.newEdge("E13011", to);
      for (int i = 0; i < PROPERTIES; i++)
        created.set("p" + i, 1_000 + i);
      created.save();
      rids[0] = from.getIdentity();
      rids[1] = created.getIdentity();
    });
    vertex = rids[0].asVertex(true);
    edge = rids[1].asEdge(true);
  }

  @AfterEach
  void dropTheDatabase() {
    if (database != null)
      database.drop();
    database = null;
    service = null;
    vertex = null;
    edge = null;
    kept = null;
  }

  @Test
  void anEdgeRowDoesNotCopyItsPropertiesToAddItsEndpoints() {
    final GrpcRecord record = service.convertResultToGrpcRecord(new ResultInternal(edge), database, null);
    assertEdgeAttributes(record);
    assertThat(service.convertResultToGrpcRecord(new ResultInternal(vertex), database, null).getPropertiesMap().get("@cat")
        .getStringValue()).isEqualTo("v");

    final long perCopy = bytesOfOneCopyOfTheBuilderMap(record);
    final long asVertex = allocatedBytesPerCall(() -> service.convertResultToGrpcRecord(new ResultInternal(vertex), database, null));
    final long asEdge = allocatedBytesPerCall(() -> service.convertResultToGrpcRecord(new ResultInternal(edge), database, null));

    assertCostsLessThanOneCopyMore(asEdge, asVertex, perCopy);
  }

  @Test
  void anEdgeLookedUpByRidDoesNotCopyItsPropertiesToAddItsEndpoints() {
    final GrpcRecord record = service.convertToGrpcRecord(edge, database);
    assertEdgeAttributes(record);
    assertThat(service.convertToGrpcRecord(vertex, database).getPropertiesMap().get("@cat").getStringValue()).isEqualTo("v");

    final long perCopy = bytesOfOneCopyOfTheBuilderMap(record);
    final long asVertex = allocatedBytesPerCall(() -> service.convertToGrpcRecord(vertex, database));
    final long asEdge = allocatedBytesPerCall(() -> service.convertToGrpcRecord(edge, database));

    assertCostsLessThanOneCopyMore(asEdge, asVertex, perCopy);
  }

  private static void assertCostsLessThanOneCopyMore(final long asEdge, final long asVertex, final long perCopy) {
    assertThat(perCopy).as("a copy of a map of %d properties allocates", PROPERTIES).isGreaterThan(PROPERTIES * 16L);
    assertThat(asEdge - asVertex)
        .as("bytes an edge of %d properties costs (%d) more than a vertex with the same properties (%d): one copy of the builder map is %d",
            PROPERTIES, asEdge, asVertex, perCopy)
        .isLessThan(perCopy);
  }

  private void assertEdgeAttributes(final GrpcRecord record) {
    final Map<String, GrpcValue> properties = record.getPropertiesMap();
    assertThat(properties.get("@cat").getStringValue()).isEqualTo("e");
    assertThat(properties.get("@out").getLinkValue().getRid()).isEqualTo(edge.getOut().toString());
    assertThat(properties.get("@in").getLinkValue().getRid()).isEqualTo(edge.getIn().toString());
    assertThat(properties.get("p7").getInt32Value()).isEqualTo(1_007);
    assertThat(record.getRid()).isEqualTo(edge.getIdentity().toString());
    assertThat(record.getType()).isEqualTo("E13011");
  }

  /** What {@code Builder.getPropertiesMap()} allocates on a builder that holds the properties of {@code record}. */
  private long bytesOfOneCopyOfTheBuilderMap(final GrpcRecord record) {
    final GrpcRecord.Builder builder = GrpcRecord.newBuilder();
    record.getPropertiesMap().forEach(builder::putProperties);
    return allocatedBytesPerCall(builder::getPropertiesMap);
  }

  /** Bytes the calling thread allocates per call, warmed up and averaged over {@link #CALLS}. */
  private long allocatedBytesPerCall(final Supplier<Object> call) {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();

    for (int i = 0; i < WARM_UP; i++)
      kept = call.get();

    final long before = threads.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < CALLS; i++)
      kept = call.get();
    return (threads.getThreadAllocatedBytes(threadId) - before) / CALLS;
  }
}
