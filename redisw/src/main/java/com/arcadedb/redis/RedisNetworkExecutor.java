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
 */
package com.arcadedb.redis;

import com.arcadedb.Constants;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.server.ArcadeDBServer;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

public class RedisNetworkExecutor extends Thread {
  private final        ArcadeDBServer      server;
  private final        Database            database;
  private final        ChannelBinaryServer channel;
  private volatile     boolean             shutdown = false;

  private       int           posInBuffer = 0;
  private final StringBuilder value       = new StringBuilder();
  private final byte[]        buffer      = new byte[32 * 1024];
  private       int           bytesRead   = 0;

  public RedisNetworkExecutor(final ArcadeDBServer server, final Socket socket, final Database database) throws IOException {
    setName(Constants.PRODUCT + "-redis/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());
    this.database = database;
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {
        executeCommand(parseNext());

        replyToClient(value);

      } catch (EOFException | SocketException e) {
        server.log(this, Level.FINE, "Redis wrapper: Error on reading request", e);
        close();
      } catch (SocketTimeoutException e) {
        // IGNORE IT
      } catch (IOException e) {
        server.log(this, Level.SEVERE, "Redis wrapper: Error on reading request", e);
      }
    }
  }

  private void executeCommand(final Object command) {
    value.setLength(0);

    if (command instanceof List) {
      final List<Object> list = (List<Object>) command;
      if (list.isEmpty())
        return;

      final Object cmd = list.get(0);
      if (!(cmd instanceof String))
        server.log(this, Level.SEVERE, "Redis wrapper: Invalid command[0] %s (type=%s)", command, cmd.getClass());

      final String cmdString = (String) cmd;

      if (cmdString.equals("GET")) {
        value.append("+bar\r\n");
      } else if (cmdString.equals("SET")) {
        final String k = (String) list.get(1);
        final String v = (String) list.get(2);

        insert("_default", k, v);

        value.append("+OK\r\n");
      } else if (cmdString.equals("HGET")) {
        final String bucket = (String) list.get(1);
        final String k = (String) list.get(2);

        final String v = read(bucket, k);

        value.append("+" + v + "\r\n");
      } else if (cmdString.equals("HSET")) {
        final String bucket = (String) list.get(1);
        final String k = (String) list.get(2);
        final String v = (String) list.get(3);

        insert(bucket, k, v);

        value.append("+1\r\n");
      }

    } else
      server.log(this, Level.SEVERE, "Redis wrapper: Invalid command %s", command);
  }

  private String read(final String bucketName, final String key) {
    return "";
  }

  private void insert(final String bucketName, final String key, final String value) {

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
      server.log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s'", (char) b);
      return null;
    }
  }

  private void skipLF() throws IOException {
    final byte b = readNext();
    if (b == '\r') {
      final byte b2 = readNext();
      if (b2 == '\n') {
      } else
        server.log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\n", (char) b2);
    } else
      server.log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\r", (char) b);
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
          server.log(this, Level.SEVERE, "Redis wrapper: Error on parsing value waiting for LF, but found '%s' after /r", (char) b);
      }
    }

    return value.toString();
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
//      server.log(this, Level.INFO, "Redis wrapper: Read '%s'...", debug);

    } while (bytesRead == 0);

    if (bytesRead == -1)
      throw new EOFException();

    return buffer[posInBuffer++];
  }

  public void replyToClient(final StringBuilder response) throws IOException {
    server.log(this, Level.FINE, "Redis wrapper: Sending response back to the client '%s'...", response);

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
}
