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

import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Base class for BOLT protocol messages.
 * Each message type has a unique signature byte.
 */
public abstract class BoltMessage {
  // Request message signatures
  public static final byte HELLO    = 0x01;
  public static final byte GOODBYE  = 0x02;
  public static final byte RESET    = 0x0F;
  public static final byte RUN      = 0x10;
  public static final byte BEGIN    = 0x11;
  public static final byte COMMIT   = 0x12;
  public static final byte ROLLBACK = 0x13;
  public static final byte DISCARD  = 0x2F;
  public static final byte PULL     = 0x3F;

  // Additional request signatures for BOLT 4.x
  public static final byte LOGON  = 0x6A;
  public static final byte LOGOFF = 0x6B;
  public static final byte ROUTE  = 0x66;

  // Response message signatures
  public static final byte SUCCESS = 0x70;
  public static final byte RECORD  = 0x71;
  public static final byte IGNORED = 0x7E;
  public static final byte FAILURE = 0x7F;

  protected final byte signature;

  protected BoltMessage(final byte signature) {
    this.signature = signature;
  }

  public byte getSignature() {
    return signature;
  }

  /**
   * Write this message to a PackStream writer.
   */
  public abstract void writeTo(PackStreamWriter writer) throws IOException;

  /**
   * Parse a message from PackStream reader result.
   */
  @SuppressWarnings("unchecked")
  public static BoltMessage parse(final PackStreamReader.StructureValue structure) throws IOException {
    final byte sig = structure.getSignature();
    final List<Object> fields = structure.getFields();

    return switch (sig) {
      case HELLO -> parseHello(fields);
      case GOODBYE -> new GoodbyeMessage();
      case RESET -> new ResetMessage();
      case RUN -> parseRun(fields);
      case BEGIN -> parseBegin(fields);
      case COMMIT -> new CommitMessage();
      case ROLLBACK -> new RollbackMessage();
      case DISCARD -> parseDiscard(fields);
      case PULL -> parsePull(fields);
      case LOGON -> parseLogon(fields);
      case LOGOFF -> new LogoffMessage();
      case ROUTE -> parseRoute(fields);
      default -> throw new IOException("Unknown message signature: 0x" + Integer.toHexString(sig & 0xFF));
    };
  }

  @SuppressWarnings("unchecked")
  private static HelloMessage parseHello(final List<Object> fields) {
    final Map<String, Object> extra = fields.isEmpty() ? Map.of() : (Map<String, Object>) fields.get(0);
    return new HelloMessage(extra);
  }

  @SuppressWarnings("unchecked")
  private static RunMessage parseRun(final List<Object> fields) {
    final String query = (String) fields.get(0);
    final Map<String, Object> parameters = fields.size() > 1 && fields.get(1) != null ? (Map<String, Object>) fields.get(1) : Map.of();
    final Map<String, Object> extra = fields.size() > 2 && fields.get(2) != null ? (Map<String, Object>) fields.get(2) : Map.of();
    return new RunMessage(query, parameters, extra);
  }

  @SuppressWarnings("unchecked")
  private static BeginMessage parseBegin(final List<Object> fields) {
    final Map<String, Object> extra = fields.isEmpty() || fields.get(0) == null ? Map.of() : (Map<String, Object>) fields.get(0);
    return new BeginMessage(extra);
  }

  @SuppressWarnings("unchecked")
  private static DiscardMessage parseDiscard(final List<Object> fields) {
    final Map<String, Object> extra = fields.isEmpty() || fields.get(0) == null ? Map.of() : (Map<String, Object>) fields.get(0);
    return new DiscardMessage(extra);
  }

  @SuppressWarnings("unchecked")
  private static PullMessage parsePull(final List<Object> fields) {
    final Map<String, Object> extra = fields.isEmpty() || fields.get(0) == null ? Map.of() : (Map<String, Object>) fields.get(0);
    return new PullMessage(extra);
  }

  @SuppressWarnings("unchecked")
  private static LogonMessage parseLogon(final List<Object> fields) {
    final Map<String, Object> auth = fields.isEmpty() || fields.get(0) == null ? Map.of() : (Map<String, Object>) fields.get(0);
    return new LogonMessage(auth);
  }

  @SuppressWarnings("unchecked")
  private static RouteMessage parseRoute(final List<Object> fields) {
    final Map<String, Object> routing = fields.isEmpty() || fields.get(0) == null ? Map.of() : (Map<String, Object>) fields.get(0);
    final List<String> bookmarks = fields.size() > 1 && fields.get(1) != null ? (List<String>) fields.get(1) : List.of();
    final String database = fields.size() > 2 ? (String) fields.get(2) : null;
    return new RouteMessage(routing, bookmarks, database);
  }

  public static String signatureName(final byte sig) {
    return switch (sig) {
      case HELLO -> "HELLO";
      case GOODBYE -> "GOODBYE";
      case RESET -> "RESET";
      case RUN -> "RUN";
      case BEGIN -> "BEGIN";
      case COMMIT -> "COMMIT";
      case ROLLBACK -> "ROLLBACK";
      case DISCARD -> "DISCARD";
      case PULL -> "PULL";
      case SUCCESS -> "SUCCESS";
      case RECORD -> "RECORD";
      case IGNORED -> "IGNORED";
      case FAILURE -> "FAILURE";
      case LOGON -> "LOGON";
      case LOGOFF -> "LOGOFF";
      case ROUTE -> "ROUTE";
      default -> "UNKNOWN(0x" + Integer.toHexString(sig & 0xFF) + ")";
    };
  }
}
