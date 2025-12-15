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
import com.arcadedb.server.ha.HAServer;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.logging.Level;

/**
 * Kubernetes DNS-based discovery service implementation.
 * This implementation queries Kubernetes DNS SRV records to discover nodes
 * in a headless service. It's designed for StatefulSets in Kubernetes
 * where each pod gets a stable network identity.
 *
 * <p>This discovery mechanism is ideal for Kubernetes deployments where
 * the cluster uses a headless service for inter-node communication.</p>
 *
 * <p>Example DNS query for service "arcadedb-headless" in namespace "default":</p>
 * <pre>
 * _arcadedb._tcp.arcadedb-headless.default.svc.cluster.local
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class KubernetesDnsDiscovery implements HADiscoveryService {

  private final String serviceName;
  private final String namespace;
  private final String portName;
  private final int defaultPort;
  private final String domain;

  /**
   * Creates a new Kubernetes DNS discovery service.
   *
   * @param serviceName the name of the Kubernetes headless service (e.g., "arcadedb-headless")
   * @param namespace   the Kubernetes namespace (e.g., "default")
   * @param portName    the name of the port in the service definition (e.g., "arcadedb")
   * @param defaultPort the default port to use if SRV records don't specify one
   */
  public KubernetesDnsDiscovery(String serviceName, String namespace, String portName, int defaultPort) {
    this(serviceName, namespace, portName, defaultPort, "cluster.local");
  }

  /**
   * Creates a new Kubernetes DNS discovery service with custom domain.
   *
   * @param serviceName the name of the Kubernetes headless service
   * @param namespace   the Kubernetes namespace
   * @param portName    the name of the port in the service definition
   * @param defaultPort the default port to use if SRV records don't specify one
   * @param domain      the Kubernetes cluster domain (e.g., "cluster.local")
   */
  public KubernetesDnsDiscovery(String serviceName, String namespace, String portName, int defaultPort, String domain) {
    if (serviceName == null || serviceName.trim().isEmpty()) {
      throw new IllegalArgumentException("Service name cannot be null or empty");
    }
    if (namespace == null || namespace.trim().isEmpty()) {
      throw new IllegalArgumentException("Namespace cannot be null or empty");
    }
    if (portName == null || portName.trim().isEmpty()) {
      throw new IllegalArgumentException("Port name cannot be null or empty");
    }
    if (defaultPort <= 0 || defaultPort > 65535) {
      throw new IllegalArgumentException("Default port must be between 1 and 65535");
    }
    if (domain == null || domain.trim().isEmpty()) {
      throw new IllegalArgumentException("Domain cannot be null or empty");
    }

    this.serviceName = serviceName;
    this.namespace = namespace;
    this.portName = portName;
    this.defaultPort = defaultPort;
    this.domain = domain;

    LogManager.instance()
        .log(this, Level.INFO, "Initialized Kubernetes DNS discovery: service=%s, namespace=%s, port=%s, domain=%s",
            serviceName, namespace, portName, domain);
  }

  @Override
  public Set<HAServer.ServerInfo> discoverNodes(String clusterName) throws DiscoveryException {
    // Build DNS query: _portName._tcp.serviceName.namespace.svc.domain
    String dnsQuery = String.format("_%s._tcp.%s.%s.svc.%s",
        portName, serviceName, namespace, domain);

    LogManager.instance()
        .log(this, Level.INFO, "Discovering nodes via Kubernetes DNS SRV query: %s", dnsQuery);

    Set<HAServer.ServerInfo> discoveredNodes = new HashSet<>();

    try {
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
      env.put(Context.PROVIDER_URL, "dns:");

      DirContext ctx = new InitialDirContext(env);
      Attributes attrs = ctx.getAttributes(dnsQuery, new String[]{"SRV"});
      Attribute srvAttr = attrs.get("SRV");

      if (srvAttr == null) {
        LogManager.instance()
            .log(this, Level.WARNING, "No SRV records found for %s", dnsQuery);
        return discoveredNodes;
      }

      NamingEnumeration<?> srvRecords = srvAttr.getAll();
      while (srvRecords.hasMore()) {
        String srvRecord = (String) srvRecords.next();
        HAServer.ServerInfo serverInfo = parseSrvRecord(srvRecord, clusterName);
        if (serverInfo != null) {
          discoveredNodes.add(serverInfo);
          LogManager.instance()
              .log(this, Level.INFO, "Discovered Kubernetes node: %s", serverInfo);
        }
      }

      ctx.close();

      LogManager.instance()
          .log(this, Level.INFO, "Discovered %d nodes via Kubernetes DNS", discoveredNodes.size());

    } catch (NamingException e) {
      throw new DiscoveryException("Failed to query Kubernetes DNS SRV records: " + dnsQuery, e);
    }

    return discoveredNodes;
  }

  @Override
  public void registerNode(HAServer.ServerInfo self) throws DiscoveryException {
    // No-op for Kubernetes DNS - registration is handled by Kubernetes service
    LogManager.instance()
        .log(this, Level.FINE, "Node registration is handled by Kubernetes service: %s", self);
  }

  @Override
  public void deregisterNode(HAServer.ServerInfo self) throws DiscoveryException {
    // No-op for Kubernetes DNS - deregistration is handled by Kubernetes service
    LogManager.instance()
        .log(this, Level.FINE, "Node deregistration is handled by Kubernetes service: %s", self);
  }

  @Override
  public String getName() {
    return "kubernetes";
  }

  /**
   * Parses an SRV record string into a ServerInfo object.
   * SRV record format: "priority weight port target"
   * Example: "0 100 2424 arcadedb-0.arcadedb-headless.default.svc.cluster.local."
   *
   * @param srvRecord    the SRV record string
   * @param clusterName  the cluster name to use as alias prefix
   * @return ServerInfo object or null if parsing fails
   */
  private HAServer.ServerInfo parseSrvRecord(String srvRecord, String clusterName) {
    try {
      String[] parts = srvRecord.split("\\s+");
      if (parts.length < 4) {
        LogManager.instance()
            .log(this, Level.WARNING, "Invalid SRV record format: %s", srvRecord);
        return null;
      }

      int port = Integer.parseInt(parts[2]);
      String target = parts[3];

      // Remove trailing dot if present
      if (target.endsWith(".")) {
        target = target.substring(0, target.length() - 1);
      }

      // Extract pod name from FQDN as alias
      // Example: arcadedb-0.arcadedb-headless.default.svc.cluster.local -> arcadedb-0
      String alias = target.split("\\.")[0];

      return new HAServer.ServerInfo(target, port, alias);

    } catch (Exception e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Failed to parse SRV record: %s - %s", srvRecord, e.getMessage());
      return null;
    }
  }

  @Override
  public String toString() {
    return String.format("KubernetesDnsDiscovery{service=%s, namespace=%s, port=%s, domain=%s}",
        serviceName, namespace, portName, domain);
  }
}
