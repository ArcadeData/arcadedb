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

import java.io.IOException;
import java.util.Map;

/**
 * RUN message for executing Cypher queries.
 * Signature: 0x10
 * Fields: query (String), parameters (Map), extra (Map)
 */
public class RunMessage extends BoltMessage {
  private final String              query;
  private final Map<String, Object> parameters;
  private final Map<String, Object> extra;

  public RunMessage(final String query, final Map<String, Object> parameters, final Map<String, Object> extra) {
    super(RUN);
    this.query = query;
    this.parameters = parameters != null ? parameters : Map.of();
    this.extra = extra != null ? extra : Map.of();
  }

  public String getQuery() {
    return query;
  }

  public Map<String, Object> getParameters() {
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
    writer.writeMap(parameters);
    writer.writeMap(extra);
  }

  @Override
  public String toString() {
    return "RUN{query='" + query + "', parameters=" + parameters + ", extra=" + extra + "}";
  }
}
