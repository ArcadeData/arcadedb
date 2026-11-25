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
package com.arcadedb.bolt.message;

import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltStructureMapper;

import java.io.IOException;
import java.util.Map;

/**
 * RUN message for executing Cypher queries.
 * Signature: 0x10
 * Fields: query (String), parameters (Map), extra (Map)
 */
public class RunMessage extends BoltMessage {
  private final String              query;
  private final Map<String, Object> extra;
  // Until the first getParameters() call of a parsed message these are the parameters as the request decoded them. A
  // message belongs to the connection thread that parsed it, so neither field needs synchronization
  private       Map<String, Object> parameters;
  private       boolean             converted;

  public RunMessage(final String query, final Map<String, Object> parameters, final Map<String, Object> extra) {
    this(query, parameters, extra, true);
  }

  private RunMessage(final String query, final Map<String, Object> parameters, final Map<String, Object> extra,
      final boolean converted) {
    super(RUN);
    this.query = query;
    this.parameters = parameters != null ? parameters : Map.of();
    this.extra = extra != null ? extra : Map.of();
    this.converted = converted || this.parameters.isEmpty();
  }

  /**
   * A RUN as {@link BoltMessage#parse} builds it, from the fields exactly as they were decoded: the parameters are turned
   * into the values the engine binds (temporals, durations, points) by the first {@link #getParameters()}, which only a
   * handler of a request the connection's state accepts calls. A request is parsed before its state is checked, so the
   * conversion, which costs as much as the parameters are large, is not paid for a request that is refused.
   */
  static RunMessage decoded(final String query, final Map<String, Object> parameters, final Map<String, Object> extra) {
    return new RunMessage(query, parameters, extra, false);
  }

  public String getQuery() {
    return query;
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getParameters() {
    if (!converted) {
      parameters = (Map<String, Object>) BoltStructureMapper.fromPackStreamValue(parameters);
      converted = true;
    }
    return parameters;
  }

  public Map<String, Object> getExtra() {
    return extra;
  }

  public String getDatabase() {
    return (String) extra.get("db");
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 3);
    writer.writeString(query);
    writer.writeMap(getParameters());
    writer.writeMap(extra);
  }

  @Override
  public String toString() {
    return "RUN{query='" + query + "', parameters=" + parameters + ", extra=" + extra + "}";
  }
}
