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
package com.arcadedb.server.ha.discovery;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.HAServer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

/**
 * HashiCorp Consul-based discovery service implementation.
 * This implementation uses Consul's HTTP API to discover nodes via service catalog
 * and health checks. It supports dynamic service registration and deregistration.
 *
 * <p>Consul is a popular service mesh solution that provides service discovery,
 * health checking, and key-value storage. This discovery mechanism is ideal for
 * deployments across multiple data centers or cloud providers.</p>
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Automatic node registration with health checks</li>
 *   <li>Dynamic node discovery via service catalog</li>
 *   <li>Health-based filtering (only healthy nodes)</li>
 *   <li>Multi-datacenter support</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ConsulDiscovery implements HADiscoveryService {

  private final String consulAddress;
  private final int consulPort;
  private final String serviceName;
  private final String datacenter;
  private final boolean onlyHealthy;

  /**
   * Creates a new Consul discovery service with default settings.
   *
   * @param consulAddress the Consul agent address (e.g., "localhost" or "consul.service.consul")
   * @param consulPort    the Consul HTTP API port (default: 8500)
   * @param serviceName   the service name to register/discover (e.g., "arcadedb")
   */
  public ConsulDiscovery(String consulAddress, int consulPort, String serviceName) {
    this(consulAddress, consulPort, serviceName, null, true);
  }

  /**
   * Creates a new Consul discovery service with custom settings.
   *
   * @param consulAddress the Consul agent address
   * @param consulPort    the Consul HTTP API port
   * @param serviceName   the service name to register/discover
   * @param datacenter    the Consul datacenter (null for default)
   * @param onlyHealthy   if true, only discover nodes passing health checks
   */
  public ConsulDiscovery(String consulAddress, int consulPort, String serviceName, String datacenter, boolean onlyHealthy) {
    if (consulAddress == null || consulAddress.trim().isEmpty()) {
      throw new IllegalArgumentException("Consul address cannot be null or empty");
    }
    if (consulPort <= 0 || consulPort > 65535) {
      throw new IllegalArgumentException("Consul port must be between 1 and 65535");
    }
    if (serviceName == null || serviceName.trim().isEmpty()) {
      throw new IllegalArgumentException("Service name cannot be null or empty");
    }

    this.consulAddress = consulAddress;
    this.consulPort = consulPort;
    this.serviceName = serviceName;
    this.datacenter = datacenter;
    this.onlyHealthy = onlyHealthy;

    LogManager.instance()
        .log(this, Level.INFO, "Initialized Consul discovery: address=%s:%d, service=%s, datacenter=%s, onlyHealthy=%b",
            consulAddress, consulPort, serviceName, datacenter, onlyHealthy);
  }

  @Override
  public Set<HAServer.ServerInfo> discoverNodes(String clusterName) throws DiscoveryException {
    String endpoint = onlyHealthy
        ? String.format("/v1/health/service/%s?passing=true", serviceName)
        : String.format("/v1/catalog/service/%s", serviceName);

    if (datacenter != null && !datacenter.trim().isEmpty()) {
      endpoint += "&dc=" + datacenter;
    }

    LogManager.instance()
        .log(this, Level.INFO, "Discovering nodes via Consul: %s", endpoint);

    Set<HAServer.ServerInfo> discoveredNodes = new HashSet<>();

    try {
      URL url = new URL("http", consulAddress, consulPort, endpoint);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(10000);

      int responseCode = conn.getResponseCode();
      if (responseCode != 200) {
        throw new DiscoveryException(
            String.format("Consul API returned error: %d %s", responseCode, conn.getResponseMessage()));
      }

      StringBuilder response = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          response.append(line);
        }
      }

      JSONArray services = new JSONArray(response.toString());
      for (int i = 0; i < services.length(); i++) {
        JSONObject service = services.getJSONObject(i);
        HAServer.ServerInfo serverInfo = parseConsulService(service, onlyHealthy);
        if (serverInfo != null) {
          discoveredNodes.add(serverInfo);
          LogManager.instance()
              .log(this, Level.INFO, "Discovered Consul node: %s", serverInfo);
        }
      }

      LogManager.instance()
          .log(this, Level.INFO, "Discovered %d nodes via Consul", discoveredNodes.size());

    } catch (Exception e) {
      throw new DiscoveryException("Failed to discover nodes from Consul: " + endpoint, e);
    }

    return discoveredNodes;
  }

  @Override
  public void registerNode(HAServer.ServerInfo self) throws DiscoveryException {
    String endpoint = "/v1/agent/service/register";

    LogManager.instance()
        .log(this, Level.INFO, "Registering node with Consul: %s", self);

    try {
      URL url = new URL("http", consulAddress, consulPort, endpoint);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(10000);

      // Build service registration JSON
      JSONObject registration = new JSONObject();
      registration.put("ID", self.alias());
      registration.put("Name", serviceName);
      registration.put("Address", self.host());
      registration.put("Port", self.port());

      // Add tags for identification
      JSONArray tags = new JSONArray();
      tags.put("arcadedb");
      tags.put("ha");
      tags.put("alias:" + self.alias());
      registration.put("Tags", tags);

      // Add health check
      JSONObject check = new JSONObject();
      check.put("TCP", self.host() + ":" + self.port());
      check.put("Interval", "10s");
      check.put("Timeout", "5s");
      registration.put("Check", check);

      byte[] requestBody = registration.toString().getBytes(StandardCharsets.UTF_8);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(requestBody);
      }

      int responseCode = conn.getResponseCode();
      if (responseCode != 200) {
        throw new DiscoveryException(
            String.format("Failed to register node with Consul: %d %s", responseCode, conn.getResponseMessage()));
      }

      LogManager.instance()
          .log(this, Level.INFO, "Successfully registered node with Consul: %s", self);

    } catch (Exception e) {
      throw new DiscoveryException("Failed to register node with Consul: " + self, e);
    }
  }

  @Override
  public void deregisterNode(HAServer.ServerInfo self) throws DiscoveryException {
    String endpoint = "/v1/agent/service/deregister/" + self.alias();

    LogManager.instance()
        .log(this, Level.INFO, "Deregistering node from Consul: %s", self);

    try {
      URL url = new URL("http", consulAddress, consulPort, endpoint);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(10000);

      int responseCode = conn.getResponseCode();
      if (responseCode != 200) {
        throw new DiscoveryException(
            String.format("Failed to deregister node from Consul: %d %s", responseCode, conn.getResponseMessage()));
      }

      LogManager.instance()
          .log(this, Level.INFO, "Successfully deregistered node from Consul: %s", self);

    } catch (Exception e) {
      throw new DiscoveryException("Failed to deregister node from Consul: " + self, e);
    }
  }

  @Override
  public String getName() {
    return "consul";
  }

  /**
   * Parses a Consul service object into a ServerInfo.
   * Handles both /catalog and /health API responses.
   *
   * @param service     the Consul service JSON object
   * @param isHealthAPI true if parsing from /health API, false for /catalog API
   * @return ServerInfo object or null if parsing fails
   */
  private HAServer.ServerInfo parseConsulService(JSONObject service, boolean isHealthAPI) {
    try {
      JSONObject serviceObj = isHealthAPI ? service.getJSONObject("Service") : service;

      String address = serviceObj.getString("Address");
      int port = serviceObj.getInt("Port");
      String serviceId = serviceObj.getString("ID");

      // Use service ID as alias (which we set to the server alias during registration)
      return new HAServer.ServerInfo(address, port, serviceId);

    } catch (Exception e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Failed to parse Consul service: %s - %s", service, e.getMessage());
      return null;
    }
  }

  @Override
  public String toString() {
    return String.format("ConsulDiscovery{address=%s:%d, service=%s, datacenter=%s, onlyHealthy=%b}",
        consulAddress, consulPort, serviceName, datacenter, onlyHealthy);
  }
}
