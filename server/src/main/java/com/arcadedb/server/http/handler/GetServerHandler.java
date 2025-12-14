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
package com.arcadedb.server.http.handler;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.DefaultServerMetrics;
import com.arcadedb.server.monitor.ServerMetrics;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class GetServerHandler extends AbstractServerHttpHandler {
  private static final DefaultServerMetrics           profilerRateMetrics = new DefaultServerMetrics();
  private static final DefaultServerMetrics           httpRateMetrics     = new DefaultServerMetrics();
  private static final ConcurrentHashMap<String, Long>   prevProfilerCounts  = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Double> prevHttpCounts      = new ConcurrentHashMap<>();

  private static final Set<String> RATE_TRACKED_PROFILER_METRICS = Set.of(
      "writeTx", "readTx", "txRollbacks", "queries", "concurrentModificationExceptions"
  );

  public GetServerHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final JSONObject response = new JSONObject().put("user", user != null ? user.getName() : null)
                                                .put("version", Constants.getVersion())
                                                .put("serverName", httpServer.getServer().getServerName())
                                                .put("languages", QueryEngineManager.getInstance().getAvailableLanguages());

    final String mode = getQueryParameter(exchange, "mode", "default");

    if ("basic".equals(mode)) {
      // JUST RETURN BASIC SERVER DATA (name and version)
    } else if ("default".equals(mode)) {
      exportMetrics(response);
      exportSettings(response);
    } else if ("cluster".equals(mode)) {
      exportCluster(exchange, response);
    }

    Metrics.counter("http.server-info").increment();

    return new ExecutionResponse(200, response.toString());
  }

  private void exportCluster(final HttpServerExchange exchange, final JSONObject response) {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha != null) {
      final JSONObject haJSON = new JSONObject();
      response.put("ha", haJSON);

      haJSON.put("clusterName", ha.getClusterName());
      haJSON.put("leader", ha.getLeaderName());
      haJSON.put("electionStatus", ha.getElectionStatus().toString());
      haJSON.put("network", ha.getStats());

      if (!ha.isLeader()) {
        // ASK TO THE LEADER THE NETWORK COMPOSITION
        HttpURLConnection connection;
        try {
          connection = (HttpURLConnection) new URL(
              "http://" + ha.getLeader().getRemoteHTTPAddress() + "/api/v1/server?mode=cluster").openConnection();
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        try {
          connection.setRequestMethod("GET");
          connection.setRequestProperty("Authorization", exchange.getRequestHeaders().get("Authorization").getFirst());
          connection.connect();

          JSONObject leaderResponse = new JSONObject(readResponse(connection));
          final JSONObject network = leaderResponse.getJSONObject("ha").getJSONObject("network");
          haJSON.getJSONObject("network").put("replicas", network.getJSONArray("replicas"));

        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          connection.disconnect();
        }
      }

      final JSONArray databases = new JSONArray();

      for (String dbName : httpServer.getServer().getDatabaseNames()) {
        final ServerDatabase db = httpServer.getServer().getDatabase(dbName);
        final ReplicatedDatabase rdb = ((ReplicatedDatabase) db.getWrappedDatabaseInstance());

        final JSONObject databaseJSON = new JSONObject();
        databaseJSON.put("name", rdb.getName());
        databaseJSON.put("quorum", rdb.getQuorum());
        databases.put(databaseJSON);
      }

      haJSON.put("databases", databases);

      final String leaderServer = ha.isLeader() ?
          ha.getServer().getHttpServer().getListeningAddress() :
          ha.getLeader().getRemoteHTTPAddress();
      final String replicaServers = ha.getReplicaServersHTTPAddressesList();

      haJSON.put("leaderAddress", leaderServer);
      haJSON.put("replicaAddresses", replicaServers);

      LogManager.instance()
          .log(this, Level.FINE, "Returning configuration leaderServer=%s replicaServers=[%s]", leaderServer, ha.getCluster());
    }
  }

  private void exportMetrics(final JSONObject response) {
    final JSONObject metricsJSON = new JSONObject();
    response.put("metrics", metricsJSON);

    final JSONObject profilerJSON = Profiler.INSTANCE.toJSON();
    metricsJSON.put("profiler", profilerJSON);

    // UPDATE PROFILER RATE METRICS FOR THE 4 KEY METRICS
    for (final String metricName : RATE_TRACKED_PROFILER_METRICS) {
      if (!profilerJSON.has(metricName))
        continue;

      final JSONObject entry = profilerJSON.getJSONObject(metricName);
      final long currentCount = entry.getLong("count", 0);
      final Long prevCount = prevProfilerCounts.put(metricName, currentCount);

      if (prevCount != null) {
        final long delta = currentCount - prevCount;
        if (delta > 0)
          profilerRateMetrics.meter(metricName).hits(delta);
      }

      final ServerMetrics.Meter meter = profilerRateMetrics.meter(metricName);
      entry.put("reqPerMinLastMinute", meter.getRequestsPerSecondInLastMinute() * 60F);
      entry.put("reqPerMinSinceLastTime", meter.getRequestsPerSecondSinceLastAsked() * 60F);
    }

    // HTTP METERS WITH PROPER RATE TRACKING
    final JSONObject metersJSON = new JSONObject();
    metricsJSON.put("meters", metersJSON);

    final MeterRegistry registry = Metrics.globalRegistry;

    registry.getMeters().stream()
        .filter(meter -> meter.getId().getName().startsWith("http."))
        .forEach(meter -> {
          final String name = meter.getId().getName();
          final double currentCount = meter.measure().iterator().next().getValue();
          final Double prevCount = prevHttpCounts.put(name, currentCount);

          if (prevCount != null) {
            final long delta = Math.round(currentCount - prevCount);
            if (delta > 0)
              httpRateMetrics.meter(name).hits(delta);
          }

          final ServerMetrics.Meter rateMeter = httpRateMetrics.meter(name);
          metersJSON.put(name,
              new JSONObject().put("count", currentCount)
                  .put("reqPerMinLastMinute", rateMeter.getRequestsPerSecondInLastMinute() * 60F)
                  .put("reqPerMinSinceLastTime", rateMeter.getRequestsPerSecondSinceLastAsked() * 60F)
          );
        });

    int serverEventsSummaryErrors = 0;
    int serverEventsSummaryWarnings = 0;
    int serverEventsSummaryInfo = 0;
    int serverEventsSummaryHints = 0;

    final JSONArray events = httpServer.getServer().getEventLog().getCurrentEvents();
    for (int i = 0; i < events.length(); i++) {
      final JSONObject event = events.getJSONObject(i);
      switch (event.getString("type")) {
      case "ERROR" -> serverEventsSummaryErrors++;
      case "WARNING" -> serverEventsSummaryWarnings++;
      case "INFO" -> serverEventsSummaryInfo++;
      case "HINT" -> serverEventsSummaryHints++;
      }
    }

    final JSONObject eventsJSON = new JSONObject();
    eventsJSON.put("errors", serverEventsSummaryErrors);
    eventsJSON.put("warnings", serverEventsSummaryWarnings);
    eventsJSON.put("info", serverEventsSummaryInfo);
    eventsJSON.put("hints", serverEventsSummaryHints);

    metricsJSON.put("events", eventsJSON);

  }

  private void exportSettings(final JSONObject response) {
    final ContextConfiguration srvCfg = httpServer.getServer().getConfiguration();
    final Set<String> contextKeys = srvCfg.getContextKeys();

    final List<Map<String, Object>> settings = new ArrayList<>();
    for (GlobalConfiguration cfg : GlobalConfiguration.values()) {
      if (cfg.getScope() != GlobalConfiguration.SCOPE.DATABASE) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("key", cfg.getKey());
        map.put("value", convertValue(cfg.getKey(), cfg.getValue()));
        map.put("description", cfg.getDescription());
        map.put("overridden", contextKeys.contains(cfg.getKey()));
        map.put("default", convertValue(cfg.getKey(), cfg.getDefValue()));
        settings.add(map);
      }
    }
    response.put("settings", settings);
  }

  private String readResponse(final HttpURLConnection connection) throws IOException {
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    final InputStream in = connection.getInputStream();
    final Scanner scanner = new Scanner(in);

    final StringBuilder buffer = new StringBuilder();

    while (scanner.hasNext()) {
      buffer.append(scanner.next().replace('\n', ' '));
    }

    return buffer.toString();
  }

  private Object convertValue(final String key, Object value) {
    if (key.toLowerCase(Locale.ENGLISH).contains("password"))
      // MASK SENSITIVE DATA
      value = "*****";

    if (key.equals("arcadedb.server.defaultDatabases")) {
      final String defaultDatabases = (String) value;
      if (value != null && !defaultDatabases.isEmpty()) {
        // CREATE DEFAULT DATABASES
        String modified = "";

        final String[] dbs = defaultDatabases.split(";");
        for (final String db : dbs) {
          final int credentialBegin = db.indexOf('[');
          if (credentialBegin < 0) {
            modified += db;
            continue;
          }

          final String dbName = db.substring(0, credentialBegin);
          final int credentialEnd = db.indexOf(']', credentialBegin);
          final String credentials = db.substring(credentialBegin + 1, credentialEnd);

          final String[] credentialPairs = credentials.split(",");
          for (final String credential : credentialPairs) {
            final String[] credentialParts = credential.split(":");
            if (credentialParts.length >= 2) {
              final String userName = credentialParts[0];
              modified += dbName + "[" + userName + ":*****]";
            } else
              modified += dbName + "[" + credentialParts + "]";
          }

          modified += ";";
        }

        if (modified.endsWith(";"))
          modified = modified.substring(0, modified.length() - 1);

        value = modified;
      }
    }

    if (value instanceof Class<?> class1)
      value = class1.getName();

    return value;
  }
}
