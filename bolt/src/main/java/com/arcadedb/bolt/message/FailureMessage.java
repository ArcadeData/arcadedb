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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FAILURE response message indicating an error occurred.
 * Signature: 0x7F
 * Fields: metadata (Map containing code and message)
 */
public class FailureMessage extends BoltMessage {
  private final Map<String, Object> metadata;

  public FailureMessage(final String code, final String message) {
    super(FAILURE);
    this.metadata = new LinkedHashMap<>();
    this.metadata.put("code", code);
    this.metadata.put("message", message);
  }

  public FailureMessage(final Map<String, Object> metadata) {
    super(FAILURE);
    this.metadata = metadata != null ? metadata : Map.of();
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public String getCode() {
    return (String) metadata.get("code");
  }

  public String getMessage() {
    return (String) metadata.get("message");
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 1);
    writer.writeMap(metadata);
  }

  @Override
  public String toString() {
    return "FAILURE{code=" + getCode() + ", message=" + getMessage() + "}";
  }

  // Common Neo4j error codes
  public static final String AUTHENTICATION_ERROR = "Neo.ClientError.Security.Unauthorized";
  public static final String SYNTAX_ERROR         = "Neo.ClientError.Statement.SyntaxError";
  public static final String SEMANTIC_ERROR       = "Neo.ClientError.Statement.SemanticError";
  public static final String DATABASE_ERROR       = "Neo.DatabaseError.General.UnknownError";
  public static final String TRANSACTION_ERROR    = "Neo.ClientError.Transaction.TransactionNotFound";
  public static final String FORBIDDEN_ERROR      = "Neo.ClientError.Security.Forbidden";
}
