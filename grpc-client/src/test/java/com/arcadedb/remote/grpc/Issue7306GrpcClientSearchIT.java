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
import com.arcadedb.query.search.FullTextSearchOperation;
import com.arcadedb.query.search.VectorSearchLeg;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.FullTextSearchRequest;
import com.arcadedb.server.grpc.FullTextSearchResponse;
import com.arcadedb.server.grpc.HybridSearchRequest;
import com.arcadedb.server.grpc.HybridSearchResponse;
import com.arcadedb.server.grpc.VectorSearchRequest;
import com.arcadedb.server.grpc.VectorSearchResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7306, gRPC client half. The RPCs exist on the wire, but until {@code RemoteGrpcDatabase} exposes them
 * they are as unreachable to a Java application as they were before the proto had them - the same reason the HTTP
 * routes are driven through {@code RemoteDatabase} in {@code Issue7306RemoteClientIT}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class Issue7306GrpcClientSearchIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;
  private static final int HTTP_PORT = 2480;

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndSeed() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE DOCUMENT TYPE C7306Doc IF NOT EXISTS BUCKETS 1");
    grpc.command("sql", "CREATE PROPERTY C7306Doc.title IF NOT EXISTS STRING");
    grpc.command("sql", "CREATE PROPERTY C7306Doc.content IF NOT EXISTS STRING");
    grpc.command("sql", "CREATE PROPERTY C7306Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
    grpc.command("sql", """
        CREATE INDEX IF NOT EXISTS ON C7306Doc (embedding) LSM_VECTOR
        METADATA { dimensions: 3, similarity: 'COSINE' }
        """);
    grpc.command("sql", "CREATE INDEX IF NOT EXISTS ON C7306Doc (content) FULL_TEXT");
    grpc.command("sql", "DELETE FROM C7306Doc");
    grpc.command("sql",
        "INSERT INTO C7306Doc SET title = 'near', content = 'flywheel bearing', embedding = [1.0, 0.0, 0.0]");
    grpc.command("sql",
        "INSERT INTO C7306Doc SET title = 'far', content = 'rank fusion', embedding = [0.0, 1.0, 0.0]");
  }

  @AfterEach
  void closeClient() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void vectorSearchReachesTheRpcAndRanksByDistance() {
    final VectorSearchResponse response = grpc.vectorSearch(VectorSearchRequest.newBuilder()
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setIndexName("C7306Doc[embedding]")
        .setK(2)
        .build());

    assertThat(response.getCount()).isEqualTo(2);
    assertThat(response.getResults(0).getRecord().getPropertiesMap().get("title").getStringValue())
        .isEqualTo("near");
    assertThat(response.getResults(0).getScore()).isLessThan(response.getResults(1).getScore());
  }

  /**
   * The database and credentials are this connection's, not the request's. A request that names another database
   * must not be able to address it through this handle, or the driver would be a way around the connection's own
   * authorization.
   */
  @Test
  void theConnectionsDatabaseOverridesWhateverTheRequestNames() {
    final VectorSearchResponse response = grpc.vectorSearch(VectorSearchRequest.newBuilder()
        .setDatabase("some-other-database-that-does-not-exist")
        .setIndexName("C7306Doc[embedding]")
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(1)
        .build());

    assertThat(response.getCount()).isEqualTo(1);
  }

  @Test
  void fullTextSearchReachesTheRpc() {
    final FullTextSearchResponse response = grpc.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setIndexName("C7306Doc[content]")
        .setQueryText("flywheel")
        .build());

    assertThat(response.getCount()).isEqualTo(1);
    assertThat(response.getResults(0).getRecord().getPropertiesMap().get("title").getStringValue())
        .isEqualTo("near");
  }

  @Test
  void hybridSearchReachesTheRpc() {
    final HybridSearchResponse response = grpc.hybridSearch(HybridSearchRequest.newBuilder()
        .setVectorIndexName("C7306Doc[embedding]")
        .addAllQueryVector(List.of(0.0f, 1.0f, 0.0f))
        .setFulltextIndexName("C7306Doc[content]")
        .setFulltextQuery("flywheel")
        .setK(2)
        .build());

    assertThat(response.getFused()).isTrue();
    assertThat(response.getCount()).isEqualTo(2);
  }

  /**
   * INVALID_ARGUMENT is the server saying the request was malformed and naming the field. Flattening it into the
   * generic remote-failure type would leave a caller unable to tell "you asked wrong" from "the server broke",
   * which is the distinction that decides whether retrying is worth anything.
   */
  @Test
  void aBoundViolationArrivesAsAnIllegalArgumentCarryingTheServersMessage() {
    assertThatThrownBy(() -> grpc.vectorSearch(VectorSearchRequest.newBuilder()
        .setIndexName("C7306Doc[embedding]")
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(1)
        .setEfSearch(VectorSearchLeg.MAX_EF_SEARCH + 1)
        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'efSearch' must be between 1 and " + VectorSearchLeg.MAX_EF_SEARCH);

    assertThatThrownBy(() -> grpc.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setIndexName("C7306Doc[content]")
        .setQueryText("flywheel")
        .setLimit(FullTextSearchOperation.MAX_LIMIT + 1)
        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'limit' must be between 1 and " + FullTextSearchOperation.MAX_LIMIT);
  }
}
