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
package com.arcadedb.bench.workload.protocol;

import com.arcadedb.bench.server.ServerUnderTest;

import java.util.List;
import java.util.Map;

/**
 * An openCypher client of the server under test over one wire protocol. Rows come back as column name to value, in the Java
 * types the protocol's client library produces: compare them with {@link Parity}. Not thread-safe: one client per thread.
 */
public interface ProtocolClient extends AutoCloseable {
  /** The language name the HTTP and gRPC clients send. */
  String LANGUAGE = "opencypher";
  String USER     = "root";

  /** Runs a read query and returns all its rows. {@code params} may be null or empty. */
  List<Map<String, Object>> query(String cypher, Map<String, Object> params);

  /**
   * Runs the statements in one transaction and commits it; on any failure the transaction is rolled back and the failure
   * thrown. {@code params} is parallel to {@code cyphers} (an element, or the whole list, may be null for no parameters).
   */
  void transaction(List<String> cyphers, List<Map<String, Object>> params);

  @Override
  void close();

  /** Opens a client of {@code database} on {@code server} over {@code protocol}; the protocol's plugin must be enabled. */
  static ProtocolClient open(final Protocol protocol, final ServerUnderTest server, final String database) {
    return switch (protocol) {
      case HTTP -> new HttpProtocolClient(server, database);
      case GRPC -> new GrpcProtocolClient(server, database);
      case BOLT -> new BoltProtocolClient(server, database);
      case POSTGRES -> new PostgresProtocolClient(server, database);
    };
  }

  /** The parameters of statement {@code i} of a transaction, never null. */
  static Map<String, Object> paramsOf(final List<Map<String, Object>> params, final int i) {
    if (params == null || i >= params.size() || params.get(i) == null)
      return Map.of();
    return params.get(i);
  }
}
