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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import com.google.protobuf.Int32Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11067 (#11044, #11048): a value a projection reaches through a graph function, a list, a map or a nested result is
 * encoded like a top-level one.
 * <ul>
 * <li>#11044: {@code out()}, {@code in()}, {@code outE()}, {@code inE()} return a lazy {@code Iterable} that is not a
 * {@code Collection}, so the encoder sent it as its {@code toString()}: "com.arcadedb.graph.GraphEngine$10@63e44da5", a
 * class name that names no record and changes on every call, where HTTP answers the neighbour records;</li>
 * <li>#11048: the list, map, array and result arms recursed without the request's projection settings, so a record
 * inside them ({@code both()} returns a {@code List}) was always sent as a bare link, and {@code soft_limit_bytes} never
 * saw its bytes.</li>
 * </ul>
 * Each query is checked against the embedded engine's own answer: the RIDs the gRPC value names are the RIDs the
 * engine's value holds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11067GrpcTraversalProjectionIT extends BaseGrpcServerTest {
  private static final String VERTEX = "TV11067";
  private static final String EDGE   = "TE11067";

  private ManagedChannel                               channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private String                                       ridA;
  private String                                       ridB;
  private String                                       ridEdge;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void graphOfTwoVerticesAndOneEdge() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE " + VERTEX);
    database.command("sql", "CREATE EDGE TYPE " + EDGE);
    database.transaction(() -> {
      final MutableVertex a = database.newVertex(VERTEX).set("name", "a").save();
      final MutableVertex b = database.newVertex(VERTEX).set("name", "b").save();
      ridA = a.getIdentity().toString();
      ridB = b.getIdentity().toString();
      ridEdge = a.newEdge(EDGE, b).getIdentity().toString();
    });

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
    headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
    stub = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void everyTraversalFunctionAnswersTheRecordsItReaches() {
    assertThat(rids(project("out('" + EDGE + "')", "a", null))).containsExactly(ridB);
    assertThat(rids(project("in('" + EDGE + "')", "b", null))).containsExactly(ridA);
    assertThat(rids(project("both('" + EDGE + "')", "a", null))).containsExactly(ridB);
    assertThat(rids(project("out()", "a", null))).containsExactly(ridB);
    assertThat(rids(project("outE()", "a", null))).containsExactly(ridEdge);
    assertThat(rids(project("inE()", "b", null))).containsExactly(ridEdge);
    assertThat(rids(project("bothE()", "a", null))).containsExactly(ridEdge);
  }

  @Test
  void aTraversalReachesTheRecordsNotItsRids() {
    // both() returns a List, which the encoder did list: each element went out as a bare link where HTTP sends the record
    final GrpcValue value = project("both('" + EDGE + "')", "a", null);
    assertThat(value.getListValue().getValuesCount()).isEqualTo(1);
    final GrpcValue neighbour = value.getListValue().getValues(0);
    assertThat(neighbour.getKindCase()).isEqualTo(GrpcValue.KindCase.BYTES_VALUE);
    assertThat(neighbour.getLogicalType()).isEqualTo("json");
    assertThat(json(neighbour).getString("name")).isEqualTo("b");

    // The same record under the MAP encoding: the request's encoding reaches the element
    final GrpcValue asMap = project("out('" + EDGE + "')", "a", projection(ProjectionEncoding.PROJECTION_AS_MAP, 0));
    final GrpcMap entries = asMap.getListValue().getValues(0).getMapValue();
    assertThat(entries.getEntriesMap().get("name").getStringValue()).isEqualTo("b");
    assertThat(entries.getEntriesMap().get("@rid").getLinkValue().getRid()).isEqualTo(ridB);
  }

  @Test
  void anEdgelessVertexAnswersAnEmptyList() {
    // HTTP answers []: a class name for "no neighbours" could not be told from one for "one neighbour"
    final GrpcValue value = project("out('" + EDGE + "')", "b", null);
    assertThat(value.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(value.getListValue().getValuesCount()).isZero();
  }

  @Test
  void aTraversalNestedInAMapOrAListIsEncodedToo() {
    final GrpcValue inMap = project("{ 'k': out('" + EDGE + "') }", "a", null);
    assertThat(rids(inMap.getMapValue().getEntriesMap().get("k"))).containsExactly(ridB);

    final GrpcValue inList = project("[ out('" + EDGE + "') ]", "a", null);
    assertThat(inList.getListValue().getValuesCount()).isEqualTo(1);
    assertThat(rids(inList.getListValue().getValues(0))).containsExactly(ridB);
  }

  @Test
  void aNestedResultRowIsTheRecordItWraps() {
    final GrpcValue value = project("(SELECT FROM " + VERTEX + " WHERE name = 'b')", "a", null);
    assertThat(rids(value)).containsExactly(ridB);
  }

  @Test
  void theSoftLimitBoundsARecordInsideAList() {
    // A record the budget cannot hold goes out as its link (the JSON encoding's fallback): the budget sees a record nested
    // in a list, which it never did, since the list arm dropped the config the budget lives on
    final GrpcValue bounded = project("both('" + EDGE + "')", "a", projection(ProjectionEncoding.PROJECTION_AS_JSON, 1));
    final GrpcValue neighbour = bounded.getListValue().getValues(0);
    assertThat(neighbour.getKindCase()).isEqualTo(GrpcValue.KindCase.LINK_VALUE);
    assertThat(neighbour.getLinkValue().getRid()).isEqualTo(ridB);
  }

  @Test
  void noValueIsSentAsTheToStringOfAnEngineClass() {
    for (final String expression : List.of("out()", "in()", "both()", "outE()", "inE()", "bothE()", "out('" + EDGE + "')",
        "in('" + EDGE + "')", "outE('" + EDGE + "')", "inE('" + EDGE + "')"))
      for (final String name : List.of("a", "b"))
        assertThat(project(expression, name, null).getKindCase()).as(expression + " of " + name)
            .isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  private GrpcValue project(final String expression, final String name, final ProjectionSettings settings) {
    final ExecuteQueryRequest.Builder request = ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setQuery("SELECT " + expression + " AS v FROM " + VERTEX + " WHERE name = '" + name + "'");
    if (settings != null)
      request.setProjectionSettings(settings);
    final ExecuteQueryResponse response = stub.executeQuery(request.build());
    assertThat(response.getResults(0).getRecordsCount()).isEqualTo(1);
    return response.getResults(0).getRecords(0).getPropertiesMap().get("v");
  }

  private static ProjectionSettings projection(final ProjectionEncoding encoding, final int softLimitBytes) {
    final ProjectionSettings.Builder settings = ProjectionSettings.newBuilder().setIncludeProjections(true)
        .setProjectionEncoding(encoding);
    if (softLimitBytes > 0)
      settings.setSoftLimitBytes(Int32Value.of(softLimitBytes));
    return settings.build();
  }

  /** The RIDs a list of records names, whichever of the three encodings carries each record. */
  private static List<String> rids(final GrpcValue list) {
    assertThat(list.getKindCase()).as("a list, not %s", list).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    final List<String> rids = new ArrayList<>();
    for (final GrpcValue element : list.getListValue().getValuesList())
      rids.add(switch (element.getKindCase()) {
        case BYTES_VALUE -> json(element).getString("@rid");
        case MAP_VALUE -> element.getMapValue().getEntriesMap().get("@rid").getLinkValue().getRid();
        case LINK_VALUE -> element.getLinkValue().getRid();
        default -> throw new AssertionError("not a record: " + element);
      });
    return rids;
  }

  private static JSONObject json(final GrpcValue value) {
    return new JSONObject(value.getBytesValue().toString(StandardCharsets.UTF_8));
  }
}
