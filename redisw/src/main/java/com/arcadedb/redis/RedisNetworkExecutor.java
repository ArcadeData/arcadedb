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
package com.arcadedb.redis;

import com.arcadedb.Constants;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.NumberUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class RedisNetworkExecutor extends Thread {
  private static final String              DEFAULT_BUCKET = "default";
  private final        ArcadeDBServer      server;
  private final        ChannelBinaryServer channel;
  private volatile     boolean             shutdown       = false;
  private              int                 posInBuffer    = 0;
  private final        StringBuilder       value          = new StringBuilder();
  private final        byte[]              buffer         = new byte[32 * 1024];
  private              int                 bytesRead      = 0;
  private final        Map<String, Object> defaultBucket  = new ConcurrentHashMap<>();

  public RedisNetworkExecutor(final ArcadeDBServer server, final Socket socket) throws IOException {
    setName(Constants.PRODUCT + "-redis/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {
        executeCommand(parseNext());

        replyToClient(value);

      } catch (EOFException | SocketException e) {
        LogManager.instance().log(this, Level.FINE, "Redis wrapper: Error on reading request", e);
        close();
      } catch (SocketTimeoutException e) {
        // IGNORE IT
      } catch (IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Error on reading request", e);
      }
    }
  }

  public void replyToClient(final StringBuilder response) throws IOException {
    LogManager.instance().log(this, Level.FINE, "Redis wrapper: Sending response back to the client '%s'...", response);

    final byte[] buffer = response.toString().getBytes(DatabaseFactory.getDefaultCharset());

    channel.outStream.write(buffer);
    channel.flush();

    response.setLength(0);
  }

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }

  public String getURL() {
    return channel.getLocalSocketAddress();
  }

  public byte[] receiveResponse() throws IOException {
    return channel.readBytes();
  }

  private void executeCommand(final Object command) {
    value.setLength(0);

    if (command instanceof List) {
      final List<Object> list = (List<Object>) command;
      if (list.isEmpty())
        return;

      final Object cmd = list.get(0);
      if (!(cmd instanceof String))
        LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid command[0] %s (type=%s)", command, cmd.getClass());

      final String cmdString = ((String) cmd).toUpperCase();

      try {
        switch (cmdString) {
        case "DECR":
          decrBy(list);
          break;

        case "DECRBY":
          decrBy(list);
          break;

        case "GET":
          get(list);
          break;

        case "GETDEL":
          getDel(list);
          break;

        case "EXISTS":
          exists(list);
          break;

        case "HDEL":
          hDel(list);
          break;

        case "HEXISTS":
          hExists(list);
          break;

        case "HGET":
          hGet(list);
          break;

        case "HMGET":
          hMGet(list);
          break;

        case "HSET":
        case "HMSET": // HMSET IS DEPRECATED IN FAVOUR OF HSET
          hSet(list);
          break;

        case "INCR":
          incrBy(list, false);
          break;

        case "INCRBY":
          incrBy(list, false);
          break;

        case "INCRBYFLOAT":
          incrBy(list, true);
          break;

        case "PING":
          ping(list);
          break;

        case "SET":
          set(list);
          break;

        default:
          value.append("-Command not found");
        }

      } catch (Exception e) {
        value.append("-");
        value.append(e.getMessage());
      }

      appendCrLf();

    } else
      LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid command %s", command);
  }

  private void decrBy(final List<Object> list) {
    final String k = (String) list.get(1);
    final int by = list.size() > 2 ? Integer.parseInt((String) list.get(2)) : 1;

    Object number = defaultBucket.get(k);
    if (!(number instanceof Number)) {
      if (NumberUtils.isIntegerNumber(number.toString()))
        number = Long.parseLong(number.toString());
      else
        throw new RedisException("Key '" + k + "' is not a number");
    }

    final Number newValue = Type.decrement((Number) number, by);
    defaultBucket.put(k, newValue);
    value.append(":");
    value.append(newValue);
  }

  private void exists(final List<Object> list) {
    final String k = (String) list.get(1);
    int total = 0;
    for (int i = 1; i < list.size(); i++)
      total += defaultBucket.containsKey(list.get(i)) ? 1 : 0;

    respondValue(total, false);
  }

  private void get(final List<Object> list) {
    final String k = (String) list.get(1);
    final Object v = defaultBucket.get(k);
    respondValue(v, true);
  }

  private void getDel(final List<Object> list) {
    final String k = (String) list.get(1);
    final Object v = defaultBucket.remove(k);
    respondValue(v, true);
  }

  private void hDel(final List<Object> list) {
    final String bucketName = (String) list.get(1);

    final int pos = bucketName.indexOf(".");
    if (pos < 0)
      throw new RedisException("Bucket name must be in the format <database>.<index>");

    final String databaseName = bucketName.substring(0, pos);
    final String keyType = bucketName.substring(pos + 1);

    final Database database = server.getDatabase(databaseName);

    int deleted = 0;

    if (keyType.startsWith("#")) {
      new RID(database, keyType).getRecord().delete();
      ++deleted;
    } else {
      final Index index = database.getSchema().getIndexByName(keyType);

      for (int i = 2; i < list.size(); i++) {
        final String key = (String) list.get(i);

        final Object[] keys;
        if (key.startsWith("[")) {
          keys = new JSONArray(key).toList().toArray();
        } else if (key.startsWith("\"")) {
          keys = new String[] { key.substring(1, key.length() - 1) };
        } else
          keys = new String[] { key };

        final IndexCursor cursor = index.get(keys);
        if (cursor.hasNext()) {
          cursor.next().getRecord().delete();
          ++deleted;
        }
      }
    }
    value.append(":");
    value.append(deleted);
  }

  private void hExists(final List<Object> list) {
    final String bucketName = (String) list.get(1);
    final String key = (String) list.get(2);

    final Record record = getRecord(bucketName, key);
    respondValue(record != null ? 1 : 0, false);
  }

  private void hGet(final List<Object> list) {
    final String bucketName = (String) list.get(1);
    final String key = (String) list.get(2);

    final Record record = getRecord(bucketName, key);
    respondValue(record != null ? record.toJSON() : null, true);
  }

  private void hMGet(final List<Object> list) {
    final String bucketName = (String) list.get(1);
    final List<Object> keys = list.subList(2, list.size());

    final List<Record> records = getRecords(bucketName, keys);

    value.append("*");
    value.append(records.size());

    for (int i = 0; i < records.size(); i++) {
      appendCrLf();
      final Record record = records.get(i);
      respondValue(record != null ? record.toJSON() : null, true);
    }
  }

  private void hSet(final List<Object> list) {
    final String databaseName = (String) list.get(1);
    final String typeName = (String) list.get(2);

    final Database database = server.getDatabase(databaseName);
    database.transaction(() -> {
      for (int i = 3; i < list.size(); i++) {
        final JSONObject v = new JSONObject((String) list.get(i));

        final DocumentType type = database.getSchema().getType(typeName);

        final MutableDocument document;

        if (type instanceof VertexType)
          document = database.newVertex(typeName);
        else if (type instanceof EdgeType)
          document = new MutableEdge(database, type, null);
        else
          document = database.newDocument(typeName);

        document.fromJSON(v);
        document.save();
      }
    });
    value.append(":");
    value.append(list.size() - 3);
  }

  private void incrBy(final List<Object> list, final boolean decimal) {
    final String k = (String) list.get(1);

    final Number by;
    if (list.size() > 2) {
      if (decimal)
        by = Double.valueOf((String) list.get(2));
      else
        by = Integer.valueOf((String) list.get(2));
    } else
      by = 1;

    Object number = defaultBucket.get(k);
    if (!(number instanceof Number)) {
      if (NumberUtils.isIntegerNumber(number.toString()))
        number = Long.parseLong(number.toString());
      else
        throw new RedisException("Key '" + k + "' is not a number");
    }

    final Number newValue = Type.increment((Number) number, by);
    defaultBucket.put(k, newValue);
    value.append(newValue instanceof Long ? ":" : "+");
    value.append(newValue);
  }

  private void set(final List<Object> list) {
    final String k = (String) list.get(1);
    final String v = (String) list.get(2);
    defaultBucket.put(k, v);
    value.append("+");
    value.append("OK");
  }

  private void ping(final List<Object> list) {
    final String response = list.size() > 1 ? (String) list.get(1) : "PONG";
    value.append("+");
    value.append(response);
  }

  private Object parseNext() throws IOException {
    final byte b = readNext();

    if (b == '+')
      // SIMPLE STRING
      return parseValueUntilLF();
    else if (b == ':')
      // INTEGER
      return Integer.parseInt(parseValueUntilLF());
    else if (b == '$') {
      // BATCH STRING
      final String value = parseChars(Integer.parseInt(parseValueUntilLF()));
      skipLF();
      return value;
    } else if (b == '*') {
      // ARRAY
      final List<Object> array = new ArrayList<>();
      final int arraySize = Integer.parseInt(parseValueUntilLF());
      for (int i = 0; i < arraySize; ++i)
        array.add(parseNext());
      return array;
    } else {
      LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s'", (char) b);
      return null;
    }
  }

  private void skipLF() throws IOException {
    final byte b = readNext();
    if (b == '\r') {
      final byte b2 = readNext();
      if (b2 == '\n') {
      } else
        LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\n", (char) b2);
    } else
      LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\r", (char) b);
  }

  private String parseValueUntilLF() throws IOException {
    value.setLength(0);

    boolean slashR = false;

    while (!shutdown) {
      final byte b = readNext();

      if (!slashR) {
        if (b == '\r')
          slashR = true;
        else
          value.append((char) b);
      } else {
        if (b == '\n')
          break;
        else
          LogManager.instance().log(this, Level.SEVERE, "Redis wrapper: Error on parsing value waiting for LF, but found '%s' after /r", (char) b);
      }
    }

    return value.toString();
  }

  private void respondValue(final Object v, final boolean forceString) {
    if (v == null)
      value.append("$-1");
    else if (!forceString && v instanceof Number) {
      value.append(":");
      value.append(v);
    } else {
      value.append("$");
      value.append(v.toString().length());
      appendCrLf();
      value.append(v);
    }
  }

  private void appendCrLf() {
    value.append("\r\n");
  }

  private String parseChars(final int size) throws IOException {
    value.setLength(0);

    for (int i = 0; i < size && !shutdown; ++i) {
      final byte b = readNext();
      value.append((char) b);
    }

    return value.toString();
  }

  private byte readNext() throws IOException {
    if (posInBuffer < bytesRead)
      return buffer[posInBuffer++];

    posInBuffer = 0;

    do {
      bytesRead = channel.inStream.read(buffer);

//      String debug = "";
//      for (int i = 0; i < bytesRead; ++i) {
//        debug += (char) buffer[i];
//      }
//      LogManager.instance().log(this, Level.INFO, "Redis wrapper: Read '%s'...", debug);

    } while (bytesRead == 0);

    if (bytesRead == -1)
      throw new EOFException();

    return buffer[posInBuffer++];
  }

  private Record getRecord(final String bucketName, final String key) {
    Record record;
    final int pos = bucketName.indexOf(".");
    if (pos < 0) {
      // BY RID
      final Database database = server.getDatabase(bucketName);

      if (key.startsWith("#"))
        record = new RID(database, key).asDocument();
      else
        throw new RedisException("Retrieving a record by RID, the key must be as #<bucket-id>:<bucket-position>. Example: #13:432");
    } else {
      // BY INDEX
      final String databaseName = bucketName.substring(0, pos);
      final String keyType = bucketName.substring(pos + 1);

      final Database database = server.getDatabase(databaseName);

      final Index index = database.getSchema().getIndexByName(keyType);

      final Object[] keys;
      if (key.startsWith("[")) {
        keys = new JSONArray(key).toList().toArray();
      } else if (key.startsWith("\"")) {
        keys = new String[] { key.substring(1, key.length() - 1) };
      } else
        keys = new String[] { key };

      final IndexCursor cursor = index.get(keys);
      record = cursor.hasNext() ? cursor.next().asDocument() : null;
    }
    return record;
  }

  private List<Record> getRecords(final String bucketName, final List<Object> keys) {
    final List<Record> records = new ArrayList<>();

    final int pos = bucketName.indexOf(".");
    if (pos < 0) {
      // BY RID
      final Database database = server.getDatabase(bucketName);

      for (Object key : keys) {
        final String k = key.toString();
        if (k.startsWith("#"))
          records.add(new RID(database, k).asDocument());
        else
          throw new RedisException("Retrieving a record by RID, the key must be as #<bucket-id>:<bucket-position>. Example: #13:432");
      }
    } else {
      // BY INDEX
      final String databaseName = bucketName.substring(0, pos);
      final String keyType = bucketName.substring(pos + 1);

      final Database database = server.getDatabase(databaseName);

      final Index index = database.getSchema().getIndexByName(keyType);

      for (Object key : keys) {
        final String k = key.toString();
        final Object[] compositeKey;
        if (k.startsWith("[")) {
          compositeKey = new JSONArray(key).toList().toArray();
        } else if (k.startsWith("\"")) {
          compositeKey = new String[] { k.substring(1, k.length() - 1) };
        } else
          compositeKey = new String[] { k };

        final IndexCursor cursor = index.get(compositeKey);
        records.add(cursor.hasNext() ? cursor.next().asDocument() : null);
      }
    }
    return records;
  }
}
