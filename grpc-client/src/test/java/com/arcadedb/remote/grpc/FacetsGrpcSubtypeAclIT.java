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
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityTestAccess;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Facets over gRPC never count the records of a bucket the caller may not read. The bucket ACL is enforced in the engine and keyed on the
 * current user, so this holds only while every gRPC path binds the caller's user before it runs the query: a path that computed the facets
 * as root would count the hidden subtype. {@code Image extends Asset}, the COLUMNAR index on {@code Asset.brand} projects Image's buckets
 * too, and brand-3 and brand-4 exist only in Image. The restricted user's group reads every type but {@code Image}.
 */
class FacetsGrpcSubtypeAclIT extends BaseGrpcClientServerTest {
  private static final String SUPER    = "Asset";
  private static final String SUB      = "Image";
  private static final String USER     = "facets-grpc-no-images";
  private static final String GROUP    = "noImagesGrpc";
  private static final String PASSWORD = "facetsgrpc17";
  private static final String QUERY    = "SELECT FROM " + SUPER + " FACETS brand TERMS TOP 100 LIMIT 0";

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.command("sql", "CREATE DOCUMENT TYPE " + SUPER + " BUCKETS 2");
    db.command("sql", "CREATE DOCUMENT TYPE " + SUB + " EXTENDS " + SUPER + " BUCKETS 2");
    db.command("sql", "CREATE PROPERTY " + SUPER + ".brand STRING");
    db.command("sql", "CREATE INDEX ON " + SUPER + " (brand) COLUMNAR");
    db.transaction(() -> {
      for (int i = 0; i < 200; i++)
        db.newDocument(SUPER).set("brand", "brand-" + (i % 3)).save();
      // brand-3 and brand-4 exist only in Image
      for (int i = 0; i < 150; i++)
        db.newDocument(SUB).set("brand", "brand-" + (2 + i % 3)).save();
    });
    FacetOracle.awaitCaughtUp(db);
  }

  @BeforeEach
  void createRestrictedUser() {
    FacetOracle.awaitCaughtUp(getServerDatabase(0, getDatabaseName()));
    final ServerSecurity security = getServer(0).getSecurity();
    ServerSecurityTestAccess.databaseGroups(security, getDatabaseName()).put(GROUP,
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
                .put(SUB, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();
    if (security.existsUser(USER))
      security.dropUser(USER);
    security.createUser(new JSONObject()
        .put("name", USER)
        .put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP))));
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(USER))
      security.dropUser(USER);
    super.endTest();
  }

  @Test
  void restrictedUserCountsReadableBucketsOnlyOverGrpc() {
    final Map<Object, Long> visible = groupByOwnRecords();
    final FacetResults unary;
    final FacetResults streamed;
    final RemoteGrpcServer server = new RemoteGrpcServer("localhost", getServerGrpcPort(), USER, PASSWORD, true, List.of());
    final RemoteGrpcDatabase database = new RemoteGrpcDatabase(server, "localhost", getServerGrpcPort(), getServerHttpPort(),
        getDatabaseName(), USER, PASSWORD);
    try {
      try (final ResultSet rs = database.query("sql", QUERY)) {
        unary = rs.getFacets();
      }
      try (final ResultSet rs = database.queryStream("sql", QUERY, 7, StreamQueryRequest.RetrievalMode.CURSOR)) {
        while (rs.hasNext())
          rs.next();
        streamed = rs.getFacets();
      }
    } finally {
      database.close();
      server.close();
    }

    assertThat(counts(unary.get("brand"))).as("unary").isEqualTo(visible);
    assertThat(counts(streamed.get("brand"))).as("CURSOR stream").isEqualTo(visible);
    // the control: root counts the hidden subtype, so the restricted answer is not equal by accident
    final Map<Object, Long> all;
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", QUERY)) {
      all = counts(rs.getFacets().get("brand"));
    }
    assertThat(all).containsKeys("brand-3", "brand-4");
    assertThat(all).isNotEqualTo(visible);
  }

  /** Root's GROUP BY over Asset's own records, the oracle of what the restricted user may count. */
  private Map<Object, Long> groupByOwnRecords() {
    final Map<Object, Long> counts = new HashMap<>();
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql",
        "SELECT brand AS v, count(*) AS c FROM " + SUPER + " WHERE @type = '" + SUPER + "' GROUP BY brand")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        counts.put(row.getProperty("v"), ((Number) row.getProperty("c")).longValue());
      }
    }
    assertThat(counts).containsOnlyKeys("brand-0", "brand-1", "brand-2");
    return counts;
  }

  private static Map<Object, Long> counts(final FacetResult result) {
    assertThat(result).isNotNull();
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        counts.put(bucket.value(), bucket.count());
    return counts;
  }
}
