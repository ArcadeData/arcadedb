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
package com.arcadedb.postgres;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.IN_PROPERTY;
import static com.arcadedb.schema.Property.OUT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

/**
 * Postgres Reference for Protocol Messages: https://www.postgresql.org/docs/9.6/protocol-message-formats.html
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresNetworkExecutor extends Thread {
  public enum ERROR_SEVERITY {FATAL, ERROR}

  public static final String PG_SERVER_VERSION = "12.0";

  private static final int                                            BUFFER_LENGTH   = 32 * 1024;
  private static final Map<Long, Pair<Long, PostgresNetworkExecutor>> ACTIVE_SESSIONS = new ConcurrentHashMap<>();

  private final ArcadeDBServer              server;
  private final ChannelBinaryServer         channel;
  private final byte[]                      buffer                = new byte[BUFFER_LENGTH];
  private final Map<String, PostgresPortal> portals               = new HashMap<>();
  private final boolean                     DEBUG                 = GlobalConfiguration.POSTGRES_DEBUG.getValueAsBoolean();
  private final Map<String, Object>         connectionProperties  = new HashMap<>();
  private final Set<String>                 ignoreQueriesAppNames = new HashSet<>(//
      List.of("dbvis", "Database Navigator - Pool"));
  private final Set<String>                 ignoreQueries         = new HashSet<>(//
      List.of(//
          "select distinct PRIVILEGE_TYPE as PRIVILEGE_NAME from INFORMATION_SCHEMA.USAGE_PRIVILEGES order by PRIVILEGE_TYPE asc",//
          "SELECT oid, typname FROM pg_type"));

  private volatile boolean shutdown = false;

  private Database database;
  private int      nextByte                   = 0;
  private boolean  reuseLastByte              = false;
  private String   userName                   = null;
  private String   databaseName               = null;
  private String   userPassword               = null;
  private int      consecutiveErrors          = 0;
  private long     processIdSequence          = 0;
  private boolean  explicitTransactionStarted = false;
  private boolean  errorInTransaction         = false;

  private interface ReadMessageCallback {
    void read(char type, long length) throws IOException;
  }

  private interface WriteMessageCallback {
    void write() throws IOException;
  }

  public PostgresNetworkExecutor(final ArcadeDBServer server, final Socket socket, final Database database) throws IOException {
    setName(Constants.PRODUCT + "-postgres/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());
    this.database = database;
  }

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }

  @Override
  public void run() {
    try {
      if (!readStartupMessage(true))
        return;

      writeMessage("request for password", () -> channel.writeUnsignedInt(3), 'R', 8);

      waitForAMessage();

      if (!readMessage("password", (type, length) -> userPassword = readString(), 'p'))
        return;

      if (!openDatabase())
        return;

      writeMessage("authentication ok", () -> channel.writeUnsignedInt(0), 'R', 8);

      // BackendKeyData
      final long pid = processIdSequence++;
      final long secret = Math.abs(new Random().nextInt(10000000));
      writeMessage("backend key data", () -> {
        channel.writeUnsignedInt((int) pid);
        channel.writeUnsignedInt((int) secret);
      }, 'K', 12);

      ACTIVE_SESSIONS.put(pid, new Pair<>(secret, this));

      sendServerParameter("server_version", PG_SERVER_VERSION);
      sendServerParameter("server_encoding", "UTF8");
      sendServerParameter("client_encoding", "UTF8");

      try {
        writeReadyForQueryMessage();

        while (!shutdown) {
          try {

            readMessage("any", (type, length) -> {
              consecutiveErrors = 0;

              switch (type) {
              case 'P' -> parseCommand();
              case 'B' -> bindCommand();
              case 'E' -> executeCommand();
              case 'Q' -> queryCommand();
              case 'S' -> syncCommand();
              case 'D' -> describeCommand();
              case 'C' -> closeCommand();
              case 'H' -> flushCommand();
              case 'X' -> {
                // TERMINATE
                shutdown = true;
                return;
              }
              default -> throw new PostgresProtocolException("Message '" + type + "' not managed");
              }

            }, 'P', 'B', 'E', 'Q', 'S', 'D', 'C', 'H', 'X');

          } catch (final Exception e) {
            setErrorInTx();

            if (e instanceof PostgresProtocolException) {
              LogManager.instance().log(this, Level.SEVERE, e.getMessage(), e);
              LogManager.instance().log(this, Level.SEVERE, "PSQL: Closing connection with client");
              return;
            } else {
              LogManager.instance().log(this, Level.SEVERE, "PSQL: Error on reading request: %s", e, e.getMessage());
              if (++consecutiveErrors > 3) {
                LogManager.instance().log(this, Level.SEVERE, "PSQL: Closing connection with client");
                return;
              }
            }
          }
        }
      } finally {
        ACTIVE_SESSIONS.remove(pid);
      }

    } finally {
      close();
    }
  }

  private void syncCommand() {
    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: sync (thread=%s)", Thread.currentThread().threadId());

    if (errorInTransaction) {
      // DISCARDED PREVIOUS MESSAGES TILL THIS POINT
      database.rollback();
      errorInTransaction = false;
    } else if (!explicitTransactionStarted) {
      if (database.isTransactionActive())
        database.commit();
    }
    writeReadyForQueryMessage();
  }

  private void flushCommand() {
    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: flush (thread=%s)", Thread.currentThread().threadId());
    writeReadyForQueryMessage();
  }

  private void closeCommand() throws IOException {
    final byte closeType = channel.readByte();
    final String prepStatementOrPortal = readString();

    if (errorInTransaction)
      return;

    if (closeType == 'P')
      getPortal(prepStatementOrPortal, true);

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: close '%s' type=%s (thread=%s)", prepStatementOrPortal, (char) closeType,
          Thread.currentThread().threadId());

    writeMessage("close complete", null, '3', 4);
  }

  private void describeCommand() throws IOException {
    final byte type = channel.readByte();
    final String portalName = readString();

    if (DEBUG)
      LogManager.instance()
          .log(this, Level.INFO, "PSQL: describe '%s' type=%s (errorInTransaction=%s thread=%s)", portalName, (char) type,
              errorInTransaction, Thread.currentThread().threadId());

    if (errorInTransaction)
      return;

    final PostgresPortal portal = getPortal(portalName, false);
    if (portal == null) {
      writeNoData();
      return;
    }

    if (type == 'P') {
      if (portal.sqlStatement != null) {
        final Object[] parameters = portal.parameterValues != null ? portal.parameterValues.toArray() : new Object[0];
        final ResultSet resultSet = portal.sqlStatement.execute(database, parameters, createCommandContext());
        portal.executed = true;
        if (portal.isExpectingResult) {
          portal.cachedResultSet = browseAndCacheResultSet(resultSet, 0);
          portal.columns = getColumns(portal.cachedResultSet);
          writeRowDescription(portal.columns);
        } else
          writeNoData();
      } else {
        if (portal.columns != null)
          writeRowDescription(portal.columns);
      }
    } else if (type == 'S') {
      writeNoData();
    } else
      throw new PostgresProtocolException("Unexpected describe type '" + type + "'");
  }

  private void executeCommand() {
    try {
      final String portalName = readString();
      final int limit = (int) channel.readUnsignedInt();

      if (errorInTransaction)
        return;

      final PostgresPortal portal = getPortal(portalName, true);
      if (portal == null) {
        writeNoData();
        return;
      }

      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: execute (portal=%s) (limit=%d)-> %s (thread=%s)", portalName, limit, portal,
                Thread.currentThread().threadId());

      if (portal.ignoreExecution)
        writeNoData();
      else {
        if (!portal.executed) {
          ResultSet resultSet;
          if (portal.sqlStatement != null) {
            final Object[] parameters = portal.parameterValues != null ? portal.parameterValues.toArray() : new Object[0];
            resultSet = portal.sqlStatement.execute(database, parameters, createCommandContext());
          } else {
            resultSet = database.command(portal.language, portal.query, server.getConfiguration(), getParams(portal));
          }
          portal.executed = true;
          if (portal.isExpectingResult) {
            portal.cachedResultSet = browseAndCacheResultSet(resultSet, limit);
            portal.columns = getColumns(portal.cachedResultSet);
            writeRowDescription(portal.columns);
          }
        }

        if (portal.isExpectingResult) {
          if (portal.columns == null)
            portal.columns = getColumns(portal.cachedResultSet);

          writeDataRows(portal.cachedResultSet, portal.columns);
          writeCommandComplete(portal.query, portal.cachedResultSet == null ? 0 : portal.cachedResultSet.size());
        } else
          writeNoData();
      }
    } catch (final CommandParsingException e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + e.getCause().getMessage(), "42601");
    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on executing query: " + e.getMessage(), "XX000");
    }
  }

  private CommandContext createCommandContext() {
    CommandContext commandContext = new BasicCommandContext();
    commandContext.setConfiguration(server.getConfiguration());
    return commandContext;
  }

  private void queryCommand() {
    try {
      String queryText = readString().trim();
      if (queryText.endsWith(";"))
        queryText = queryText.substring(0, queryText.length() - 1);

      if (errorInTransaction)
        return;

      if (queryText.isEmpty()) {
        emptyQueryResponse();
        return;
      }

      final Query query = getLanguageAndQuery(queryText);
      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: query -> %s ", query);

      final ResultSet resultSet;
      if (query.query.startsWith("SET ")) {
        setConfiguration(query.query);
        resultSet = new IteratorResultSet(createResultSet("STATUS", "Setting ignored").iterator());
      } else if (query.query.equals("SELECT VERSION()"))
        resultSet = new IteratorResultSet(createResultSet("VERSION", "11.0.0").iterator());
      else if (query.query.equals("SELECT CURRENT_SCHEMA()"))
        resultSet = new IteratorResultSet(createResultSet("CURRENT_SCHEMA", database.getName()).iterator());
      else if (query.query.equalsIgnoreCase("BEGIN") ||
          query.query.equalsIgnoreCase("BEGIN TRANSACTION")) {
        explicitTransactionStarted = true;
        database.begin();
        resultSet = new IteratorResultSet(Collections.emptyIterator());
      } else if (ignoreQueries.contains(query.query))
        resultSet = new IteratorResultSet(Collections.emptyIterator());
      else {
        resultSet = database.command(query.language, query.query, server.getConfiguration());
      }
      final List<Result> cachedResultSet = browseAndCacheResultSet(resultSet, 0);

      final Map<String, PostgresType> columns = getColumns(cachedResultSet);
      writeRowDescription(columns);
      writeDataRows(cachedResultSet, columns);
      writeCommandComplete(queryText, cachedResultSet.size());

    } catch (final CommandParsingException e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + e.getCause().getMessage(), "42601");
    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on executing query: " + e.getMessage(), "XX000");
    } finally {
      writeReadyForQueryMessage();
    }
  }

  private void writeReadyForQueryMessage() {
    final byte transactionStatus;
    if (explicitTransactionStarted)
      transactionStatus = 'T';
    else
      transactionStatus = 'I';

    writeMessage("ready for query", () -> channel.writeByte(transactionStatus), 'Z', 5);
  }

  private List<Result> browseAndCacheResultSet(final ResultSet resultSet, final int limit) {
    final List<Result> cachedResultSet = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      if (row == null)
        continue;

      cachedResultSet.add(row);

      if (limit > 0 && cachedResultSet.size() >= limit) {
        portalSuspendedResponse();
        break;
      }
    }
    return cachedResultSet;
  }

  private Object[] getParams(PostgresPortal portal) {
    Object[] parameters = portal.parameterValues != null ? portal.parameterValues.toArray() : new Object[0];

    if (portal.language.equals("cypher")) {
      Object[] parametersCypher = new Object[parameters.length * 2];
      for (int i = 0; i < parameters.length; i++) {
        parametersCypher[i] = "" + (i + 1);
        parametersCypher[i + 1] = portal.parameterValues.get(i);
      }
      return parametersCypher;
    }

    return parameters;
  }

  private Map<String, PostgresType> getColumns(final List<Result> resultSet) {
    final Map<String, PostgresType> columns = new LinkedHashMap<>();

    boolean atLeastOneElement = false;
    for (final Result row : resultSet) {
      if (row.isElement())
        atLeastOneElement = true;

      final Set<String> propertyNames = row.getPropertyNames();
      for (final String p : propertyNames) {
        // Add all property names with default VARCHAR type
        if (!columns.containsKey(p)) {
          columns.put(p, PostgresType.VARCHAR);
        }

        final Object value = row.getProperty(p);

        // Only update type if value is not null
        if (value != null) {
          PostgresType currentType = columns.get(p);

          final Class<?> valueClass = value.getClass();
          if (currentType == null || currentType.cls != valueClass) {

            PostgresType newType = PostgresType.getTypeForValue(value);
            // assign new type if it id better than the current one: better means more specific than VARCHAR
            if (newType != null &&
                newType != currentType
            ) {
              currentType = newType;
            }
            // in the end, if currentType is null, assign VARCHAR
            if (currentType == null)
              currentType = PostgresType.VARCHAR;

            columns.put(p, currentType);

          }
        }
      }
    }

    if (atLeastOneElement) {
      columns.put(RID_PROPERTY, PostgresType.VARCHAR);
      columns.put(TYPE_PROPERTY, PostgresType.VARCHAR);
      columns.put(CAT_PROPERTY, PostgresType.CHAR);
    }

    return columns;
  }

  private void writeRowDescription(final Map<String, PostgresType> columns) {
    if (columns == null)
      return;

//    final ByteBuffer bufferDescription = ByteBuffer.allocate(64 * 1024).order(ByteOrder.BIG_ENDIAN);
    final Binary bufferDescription = new Binary();

    for (final Map.Entry<String, PostgresType> col : columns.entrySet()) {
      final String columnName = col.getKey();
      final PostgresType columnType = col.getValue();

      bufferDescription.putByteArray(columnName.getBytes(DatabaseFactory.getDefaultCharset()));//The field name.
      bufferDescription.putByte((byte) 0);

      //If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
      bufferDescription.putInt(0);
      //If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
      bufferDescription.putShort((short) 0);
      // The object ID of the field's data type.
      bufferDescription.putInt(columnType.code);
      // The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
      bufferDescription.putShort((short) columnType.size);
      // The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
      bufferDescription.putInt(-1);
      // The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
      bufferDescription.putShort((short) 0);
    }

    bufferDescription.flip();
    writeMessage("row description", () -> {
      channel.writeUnsignedShort((short) columns.size());
      channel.writeBuffer(bufferDescription.getByteBuffer());
    }, 'T', 4 + 2 + bufferDescription.limit());
  }

  private void writeDataRows(final List<Result> resultSet, final Map<String, PostgresType> columns) throws IOException {
    if (resultSet.isEmpty())
      return;

    final Binary bufferData = new Binary();
    final Binary bufferValues = new Binary();

    for (final Result row : resultSet) {
      bufferValues.putShort((short) columns.size()); // Int16 The number of column values that follow (possibly zero).

      for (final Map.Entry<String, PostgresType> postgresTypeEntry : columns.entrySet()) {
        final String propertyName = postgresTypeEntry.getKey();

        Object value = switch (propertyName) {
          case RID_PROPERTY -> row.isElement() ? row.getElement().get().getIdentity() : row.getProperty(propertyName);
          case TYPE_PROPERTY -> row.isElement() ? row.getElement().get().getTypeName() : row.getProperty(propertyName);
          case OUT_PROPERTY -> {
            if (row.isElement()) {
              final Document record = row.getElement().get();
              if (record instanceof Vertex vertex)
                yield vertex.countEdges(Vertex.DIRECTION.OUT, null);
              else if (record instanceof Edge edge)
                yield edge.getOut();
            }
            yield row.getProperty(propertyName);
          }
          case IN_PROPERTY -> {
            if (row.isElement()) {
              final Document record = row.getElement().get();
              if (record instanceof Vertex vertex)
                yield vertex.countEdges(Vertex.DIRECTION.IN, null);
              else if (record instanceof Edge edge)
                yield edge.getIn();
            }
            yield row.getProperty(propertyName);
          }
          case CAT_PROPERTY -> {
            if (row.isElement()) {
              final Document record = row.getElement().get();
              if (record instanceof Vertex)
                yield "v";
              else if (record instanceof Edge)
                yield "e";
              else
                yield "d";
            }
            yield row.getProperty(propertyName);
          }
          default -> row.getProperty(propertyName);
        };

        postgresTypeEntry.getValue().serializeAsText(postgresTypeEntry.getValue(), bufferValues, value);
      }

      bufferValues.flip();
      bufferData.putByte((byte) 'D');
      bufferData.putInt(4 + bufferValues.getByteBuffer().limit());
      bufferData.putBuffer(bufferValues.getByteBuffer());

      bufferData.flip();
      channel.writeBuffer(bufferData.getByteBuffer());

      bufferData.clear();
      bufferValues.clear();
    }

    channel.flush();

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL:-> %d row data (%s) (thread=%s)", resultSet.size(),
          FileUtils.getSizeAsString(bufferData.limit()), Thread.currentThread().threadId());
  }

  private void bindCommand() {
    try {
      // BIND
      final String portalName = readString();
      final String sourcePreparedStatement = readString();

      final PostgresPortal portal = getPortal(portalName, false);
      if (portal == null) {
        writeMessage("bind complete", null, '2', 4);
        return;
      }

      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: bind (portal=%s) -> %s (thread=%s)", portalName, sourcePreparedStatement,
                Thread.currentThread().threadId());

      final int paramFormatCount = channel.readShort();
      if (paramFormatCount > 0) {
        portal.parameterFormats = new ArrayList<>(paramFormatCount);
        for (int i = 0; i < paramFormatCount; i++) {
          final int formatCode = channel.readUnsignedShort();
          portal.parameterFormats.add(formatCode);
        }
      }

      final int paramValuesCount = channel.readShort();
      if (paramValuesCount > 0) {
        portal.parameterValues = new ArrayList<>(paramValuesCount);
        for (int i = 0; i < paramValuesCount; i++) {
          final long paramSize = channel.readUnsignedInt();
          final byte[] paramValue = new byte[(int) paramSize];
          channel.readBytes(paramValue);

          portal.parameterValues.add(//
              PostgresType.deserialize(portal.parameterTypes.get(i), portal.parameterFormats.get(i), paramValue)//
          );
        }
      }

      final int resultFormatCount = channel.readShort();
      if (resultFormatCount > 0) {
        portal.resultFormats = new ArrayList<>(resultFormatCount);
        for (int i = 0; i < resultFormatCount; i++) {
          final int resultFormat = channel.readUnsignedShort();
          portal.resultFormats.add(resultFormat);
        }
      }

      if (errorInTransaction)
        return;

      writeMessage("bind complete", null, '2', 4);

    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on parsing bind message: " + e.getMessage(), "XX000");
    }
  }

  private void parseCommand() {
    try {
      // PARSE
      final String portalName = readString();

      final Query query = getLanguageAndQuery(readString());

      final PostgresPortal portal = new PostgresPortal(query.query, query.language);
      final int paramCount = channel.readShort();

      if (paramCount > 0) {
        portal.parameterTypes = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
          final long param = channel.readUnsignedInt();
          portal.parameterTypes.add(param);
        }
      }

      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: parse (portal=%s) -> %s (params=%d) (errorInTransaction=%s thread=%s)", portalName,
                portal.query, paramCount, errorInTransaction, Thread.currentThread().threadId());

      if (errorInTransaction)
        return;

      if (portal.query.isEmpty()) {
        emptyQueryResponse();
        return;
      }

      final String upperCaseText = portal.query.toUpperCase(Locale.ENGLISH);

      if (portal.query.isEmpty() ||//
          (ignoreQueriesAppNames.contains(connectionProperties.get("application_name")) &&//
              ignoreQueries.contains(portal.query))) {
        // RETURN EMPTY RESULT
        portal.executed = true;
        portal.cachedResultSet = new ArrayList<>();
      } else if (upperCaseText.startsWith("SAVEPOINT ")) {
        portal.ignoreExecution = true;
      } else if (upperCaseText.startsWith("SET ")) {
        setConfiguration(portal.query);
        portal.ignoreExecution = true;
      } else if (upperCaseText.equals("SELECT VERSION()")) {
        createResultSet(portal, "VERSION", "11.0.0");

      } else if (upperCaseText.equals("SELECT CURRENT_SCHEMA()")) {
        createResultSet(portal, "CURRENT_SCHEMA", database.getName());

      } else if (upperCaseText.equals("SHOW TRANSACTION ISOLATION LEVEL")) {
        final Database.TRANSACTION_ISOLATION_LEVEL dbIsolationLevel = database.getTransactionIsolationLevel();
        final String level = dbIsolationLevel.name().replace('_', ' ');
        createResultSet(portal, "LEVEL", level);

      } else if (upperCaseText.startsWith("SHOW ")) {
        createResultSet(portal, "CURRENT_SCHEMA", database.getName());

      } else if ("dbvis".equals(connectionProperties.get("application_name"))) {
        // SPECIAL CASES
        if (portal.query.equals(
            "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace  WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))  ORDER BY TABLE_SCHEM")
            || portal.query.equals("SELECT     COLLATION_SCHEMA,     COLLATION_NAME FROM     INFORMATION_SCHEMA.COLLATIONS")) {
          // SPECIAL CASE DB VISUALIZER

          portal.executed = true;
          portal.cachedResultSet = new ArrayList<>();

          final Map<String, Object> map = new HashMap<>();
          map.put("TABLE_CATALOG", null);
          map.put("TABLE_SCHEM", "");

          final Result result = new ResultInternal(map);
          portal.cachedResultSet.add(result);

          portal.columns = new HashMap<>();
          portal.columns.put("TABLE_CATALOG", PostgresType.VARCHAR);
          portal.columns.put("TABLE_SCHEM", PostgresType.VARCHAR);

        } else if (portal.query.contains("ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ")) {
          portal.executed = true;
          portal.cachedResultSet = new ArrayList<>();
          for (final DocumentType t : database.getSchema().getTypes()) {
            final Map<String, Object> map = new HashMap<>();
            map.put("TABLE_CAT", "");
            map.put("TABLE_SCHEM", "");
            map.put("TABLE_TYPE", "TABLE");
            map.put("TABLE_NAME", t.getName());
            map.put("REMARKS", "");
            map.put("TYPE_CAT", "");
            map.put("TYPE_SCHEM", "");
            map.put("TYPE_NAME", "");
            map.put("SELF_REFERENCING_COL_NAME", "");
            map.put("REF_GENERATION", "");

            final Result result = new ResultInternal(map);
            portal.cachedResultSet.add(result);

            portal.columns = new HashMap<>();
            portal.columns.put("TABLE_CAT", PostgresType.VARCHAR);
            portal.columns.put("TABLE_SCHEM", PostgresType.VARCHAR);
            portal.columns.put("TABLE_TYPE", PostgresType.VARCHAR);
            portal.columns.put("TABLE_NAME", PostgresType.VARCHAR);
            portal.columns.put("REMARKS", PostgresType.VARCHAR);
            portal.columns.put("TYPE_CAT", PostgresType.VARCHAR);
            portal.columns.put("TYPE_SCHEM", PostgresType.VARCHAR);
            portal.columns.put("TYPE_NAME", PostgresType.VARCHAR);
            portal.columns.put("SELF_REFERENCING_COL_NAME", PostgresType.VARCHAR);
            portal.columns.put("REF_GENERATION", PostgresType.VARCHAR);
          }
        }
      } else if (portal.query.equals(
          "select distinct GRANTEE as USER_NAME, 'N' as IS_EXPIRED, 'N' as IS_LOCKED from INFORMATION_SCHEMA.USAGE_PRIVILEGES order by GRANTEE asc")) {
        portal.executed = true;
        portal.cachedResultSet = new ArrayList<>();
        final Map<String, Object> map = new HashMap<>();
        map.put("USER_NAME", "root");
        map.put("IS_EXPIRED", 'N');
        map.put("IS_LOCKED", 'N');
        final Result result = new ResultInternal(map);
        portal.cachedResultSet.add(result);

        portal.columns = new HashMap<>();
        portal.columns.put("USER_NAME", PostgresType.VARCHAR);
        portal.columns.put("IS_EXPIRED", PostgresType.CHAR);
        portal.columns.put("IS_LOCKED", PostgresType.CHAR);

      } else if (portal.query.equals(
          "select CHARACTER_SET_NAME as CHARSET_NAME, -1 as MAX_LENGTH from INFORMATION_SCHEMA.CHARACTER_SETS order by CHARACTER_SET_NAME asc")) {
        portal.executed = true;
        portal.cachedResultSet = new ArrayList<>();
        final Map<String, Object> map = new HashMap<>();
        map.put("CHARSET_NAME", "UTF-8");
        map.put("MAX_LENGTH", -1);
        final Result result = new ResultInternal(map);
        portal.cachedResultSet.add(result);

        portal.columns = new HashMap<>();
        portal.columns.put("CHARSET_NAME", PostgresType.VARCHAR);
        portal.columns.put("MAX_LENGTH", PostgresType.INTEGER);
      } else if (//
          portal.query.equals(
              "select NSPNAME as SCHEMA_NAME, case when lower(NSPNAME)='pg_catalog' then 'Y' else 'N' end as IS_PUBLIC, case when lower(NSPNAME)='information_schema' then 'Y' else 'N' end as IS_SYSTEM, 'N' as IS_EMPTY from PG_CATALOG.PG_NAMESPACE order by NSPNAME asc")
              || portal.query.equals(
              "select SCHEMA_NAME, case when lower(SCHEMA_NAME)='pg_catalog' then 'Y' else 'N' end as IS_PUBLIC, case when lower(SCHEMA_NAME)='information_schema' then 'Y' else 'N' end as IS_SYSTEM, 'N' as IS_EMPTY from INFORMATION_SCHEMA.SCHEMATA order by SCHEMA_NAME asc")) {

        portal.executed = true;
        portal.cachedResultSet = new ArrayList<>();

        for (final String dbName : server.getDatabaseNames()) {
          final Map<String, Object> map = new HashMap<>();
          map.put("SCHEMA_NAME", dbName);
          map.put("IS_PUBLIC", "Y");
          map.put("IS_SYSTEM", "N");
          map.put("IS_EMPTY", "N");
          final Result result = new ResultInternal(map);
          portal.cachedResultSet.add(result);
        }

        portal.columns = new HashMap<>();
        portal.columns.put("SCHEMA_NAME", PostgresType.VARCHAR);
        portal.columns.put("IS_PUBLIC", PostgresType.CHAR);
        portal.columns.put("IS_SYSTEM", PostgresType.CHAR);
        portal.columns.put("IS_EMPTY", PostgresType.CHAR);
      } else if (portal.query.equals(
          "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace  WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))  AND nspname LIKE E'%' ORDER BY TABLE_SCHEM")) {

        portal.executed = true;
        portal.cachedResultSet = new ArrayList<>();

        for (final DocumentType t : database.getSchema().getTypes()) {
          final Map<String, Object> map = new HashMap<>();
          map.put("TABLE_SCHEM", t.getName());
          map.put("TABLE_CATALOG", database.getName());
          final Result result = new ResultInternal(map);
          portal.cachedResultSet.add(result);
        }

        portal.columns = new HashMap<>();
        portal.columns.put("TABLE_SCHEM", PostgresType.VARCHAR);
        portal.columns.put("TABLE_CATALOG", PostgresType.VARCHAR);
      } else {
        switch (portal.language) {
        case "sql":
          final SQLQueryEngine sqlEngine = (SQLQueryEngine) database.getQueryEngine("sql");
          portal.sqlStatement = sqlEngine.parse(query.query, (DatabaseInternal) database);
          if (portal.query.equalsIgnoreCase("BEGIN") || portal.query.equalsIgnoreCase("BEGIN TRANSACTION")) {
            explicitTransactionStarted = true;
            setEmptyResultSet(portal);
          } else if (portal.query.equalsIgnoreCase("COMMIT")) {
            explicitTransactionStarted = false;
            setEmptyResultSet(portal);
          }
          break;

        default:
          //nooop
        }
      }

      portals.put(portalName, portal);

      // ParseComplete
      writeMessage("parse complete", null, '1', 4);

    } catch (final CommandParsingException e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on parsing query: " + e.getCause().getMessage(), "42601");
    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on parsing query: " + e.getMessage(), "XX000");
    }
  }

  private void setConfiguration(final String query) {
    final String q = query.substring("SET ".length());
    String[] parts = q.split("=");
    if (parts.length < 2)
      parts = q.split(" TO ");

    parts[0] = parts[0].trim();
    parts[1] = parts[1].trim();

    if (parts[1].startsWith("'") || parts[1].startsWith("\""))
      parts[1] = parts[1].substring(1, parts[1].length() - 1);

    if (parts[0].equals("datestyle")) {
      if (parts[1].equals("ISO"))
        database.getSchema().setDateTimeFormat(DateUtils.DATE_TIME_ISO_8601_FORMAT);
      else
        LogManager.instance().log(this, Level.INFO, "datestyle '%s' not supported", parts[1]);
    }

    connectionProperties.put(parts[0], parts[1]);
  }

  private void setEmptyResultSet(final PostgresPortal portal) {
    portal.executed = true;
    portal.isExpectingResult = true;
    portal.cachedResultSet = Collections.emptyList();
    portal.columns = getColumns(portal.cachedResultSet);
  }

  private void sendServerParameter(final String name, final String value) {
    final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

    final int length = 4 + nameBytes.length + 1 + valueBytes.length + 1;

    writeMessage("parameter status", () -> {
      writeString(name);
      writeString(value);
    }, 'S', length);
  }

  private boolean openDatabase() {
    if (databaseName == null) {
      writeError(ERROR_SEVERITY.FATAL, "Database not selected", "HV00Q");
      return false;
    }

    try {
      final ServerSecurityUser dbUser = server.getSecurity().authenticate(userName, userPassword, databaseName);

      database = server.getDatabase(databaseName);

      DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(dbUser.getDatabaseUser(database));

      database.setAutoTransaction(true);

    } catch (final ServerSecurityException e) {
      writeError(ERROR_SEVERITY.FATAL, "Credentials not valid", "28P01");
      return false;
    } catch (final DatabaseOperationException e) {
      writeError(ERROR_SEVERITY.FATAL, "Database does not exist", "HV00Q");
      return false;
    }

    return true;
  }

  private boolean readStartupMessage(final boolean no2ssl) {
    try {
      final long len = channel.readUnsignedInt();
      final long protocolVersion = channel.readUnsignedInt();
      if (protocolVersion == 80877103) {
        // REQUEST FOR SSL, NOT SUPPORTED
        if (no2ssl) {
          channel.writeByte((byte) 'N');
          channel.flush();

          LogManager.instance().log(this, Level.INFO,
              "PSQL: received not supported SSL connection request. Sending back error message to the client");

          // REPEAT
          return readStartupMessage(false);
        }

        throw new PostgresProtocolException("SSL authentication is not supported");
      } else if (protocolVersion == 80877102) {
        // CANCEL REQUEST, IGNORE IT
        final long pid = channel.readUnsignedInt();
        final long secret = channel.readUnsignedInt();

        LogManager.instance().log(this, Level.INFO, "PSQL: Received cancel request pid %d", pid);

        final Pair<Long, PostgresNetworkExecutor> session = ACTIVE_SESSIONS.get(pid);
        if (session != null) {
          if (session.getFirst() == secret) {
            LogManager.instance().log(this, Level.INFO, "PSQL: Canceling session " + pid);
            session.getSecond().close();
          } else
            LogManager.instance().log(this, Level.INFO, "PSQL: Blocked unauthorized canceling session " + pid);
        } else
          LogManager.instance().log(this, Level.INFO, "PSQL: Session " + pid + " not found");

        close();
        return false;
      }

      if (len > 8) {
        while (readNextByte() != 0) {
          reuseLastByte();

          final String paramName = readString();
          final String paramValue = readString();

          switch (paramName) {
          case "user":
            userName = paramValue;
            break;
          case "database":
            databaseName = paramValue;
            break;
          case "options":
            // DEPRECATED, IGNORE IT
            break;
          case "replication":
            // NOT SUPPORTED, IGNORE IT
            break;
          }

          connectionProperties.put(paramName, paramValue);
        }
      }
    } catch (final IOException e) {
      setErrorInTx();
      throw new PostgresProtocolException("Error on parsing startup message", e);
    }
    return true;
  }

  private void writeError(final ERROR_SEVERITY severity, final String errorMessage, final String errorCode) {
    try {
      final String sev = severity.toString();

      final int length = 4 + //
          1 + errorMessage.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1 + sev.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1 + errorCode.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1;

      channel.writeByte((byte) 'E');
      channel.writeUnsignedInt(length);

      channel.writeByte((byte) 'M');
      writeString(errorMessage);

      channel.writeByte((byte) 'S');
      writeString(sev);

      channel.writeByte((byte) 'C');
      writeString(errorCode);

      channel.writeByte((byte) 0);
      channel.flush();
    } catch (final IOException e) {
      setErrorInTx();
      throw new PostgresProtocolException("Error on sending error '" + errorMessage + "' to the client", e);
    }
  }

  private void writeMessage(final String messageName, final WriteMessageCallback callback, final char messageCode,
      final long length) {
    try {
      channel.writeByte((byte) messageCode);
      channel.writeUnsignedInt((int) length);
      if (callback != null)
        callback.write();
      channel.flush();

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL:-> %s (%s - %s) (thread=%s)", null, messageName, messageCode,
            FileUtils.getSizeAsString(length), Thread.currentThread().threadId());

    } catch (final IOException e) {
      setErrorInTx();
      throw new PostgresProtocolException("Error on sending '" + messageName + "' message", e);
    }
  }

  private boolean readMessage(final String messageName, final ReadMessageCallback callback, final char... expectedMessageCodes) {
    try {
      if (!channel.inputHasData()) {
        CodeUtils.sleep(100);
        return false;
      }

      final char type = (char) readNextByte();
      final long length = channel.readUnsignedInt();

      if (expectedMessageCodes != null && expectedMessageCodes.length > 0) {
        // VALIDATE MESSAGES
        boolean valid = false;
        for (int i = 0; i < expectedMessageCodes.length; i++) {
          if (type == expectedMessageCodes[i]) {
            valid = true;
            break;
          }
        }

        if (!valid) {
          // READ TILL THE END OF THE MESSAGE
          if (length > 4)
            readBytes((int) (length - 4));
          throw new PostgresProtocolException("Unexpected message type '" + type + "' for message " + messageName);
        }
      }

      //if (length > 4)
      callback.read(type, length - 4);

      return true;

    } catch (final EOFException e) {
      // CLIENT CLOSES THE CONNECTION
      setErrorInTx();
      return false;
    } catch (final IOException e) {
      setErrorInTx();
      throw new PostgresProtocolException("Error on reading " + messageName + " message: " + e.getMessage(), e);
    }
  }

  private int readNextByte() throws IOException {
    if (reuseLastByte) {
      // USE THE BYTE ALREADY READ
      reuseLastByte = false;
      return nextByte;
    }

    return nextByte = channel.readUnsignedByte();
  }

  private void waitForAMessage() {
    while (!channel.inputHasData()) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException interruptedException) {
        throw new PostgresProtocolException("Error on reading from the channel");
      }
    }
  }

  private void reuseLastByte() {
    reuseLastByte = true;
  }

  private String readString() throws IOException {
    int len = 0;
    for (; len < buffer.length; len++) {
      final int b = readNextByte();
      if (b == 0)
        return new String(buffer, 0, len, DatabaseFactory.getDefaultCharset());

      buffer[len] = (byte) b;
    }

    len = readUntilTerminator(len);

    throw new PostgresProtocolException("String content (" + len + ") too long (>" + BUFFER_LENGTH + ")");
  }

  private void writeString(final String text) throws IOException {
    channel.writeBytes(text.getBytes(StandardCharsets.UTF_8));
    channel.writeByte((byte) 0);
  }

  private int readUntilTerminator(int len) throws IOException {
    // OUT OF BUFFER SIZE, CONTINUE READING AND DISCARD THE CONTENT
    for (; readNextByte() != 0; len++) {
    }
    return len;
  }

  private void readBytes(final int len) throws IOException {
    for (int i = 0; i < len; i++)
      readNextByte();
  }

  private void writeCommandComplete(final String queryText, final int resultSetCount) {
    final String upperCaseText = queryText.toUpperCase(Locale.ENGLISH);
    final String tag = getTag(upperCaseText, resultSetCount);
    writeMessage("command complete",
        () -> writeString(tag), 'C', 4 + tag.length() + 1);
  }

  private String getTag(String upperCaseText, int resultSetCount) {
    if (upperCaseText.startsWith("CREATE VERTEX") || upperCaseText.startsWith("INSERT INTO")) {
      return "INSERT 0 " + resultSetCount;
    } else if (upperCaseText.startsWith("SELECT") || upperCaseText.startsWith("MATCH")) {
      return "SELECT " + resultSetCount;
    } else if (upperCaseText.startsWith("UPDATE")) {
      return "UPDATE " + resultSetCount;
    } else if (upperCaseText.startsWith("DELETE")) {
      return "DELETE " + resultSetCount;
    } else if (upperCaseText.equals("BEGIN") || upperCaseText.equals("BEGIN TRANSACTION")) {
      return "BEGIN";
    } else {
      return "";
    }
  }

  private void writeNoData() {
    writeMessage("no data", null, 'n', 4);
  }

  private PostgresPortal getPortal(final String name, final boolean remove) {
    if (remove)
      return portals.remove(name);
    else
      return portals.get(name);
  }

  private void createResultSet(final PostgresPortal portal, final Object... elements) {
    portal.executed = true;
    portal.cachedResultSet = createResultSet(elements);
    portal.columns = getColumns(portal.cachedResultSet);
  }

  private List<Result> createResultSet(final Object... elements) {
    if (elements.length % 2 != 0)
      throw new IllegalArgumentException("Result set elements must be in pairs");

    final List<Result> resultSet = new ArrayList<>();
    for (int i = 0; i < elements.length; i += 2) {
      final Map<String, Object> map = new HashMap<>(2);
      map.put((String) elements[i], elements[i + 1]);
      resultSet.add(new ResultInternal(map));
    }
    return resultSet;
  }

  private Query getLanguageAndQuery(final String query) {
    String language = "sql";
    String queryText = query;

    // Regular expression to match language prefixes
    Pattern pattern = Pattern.compile("\\{(\\w+)\\}");
    Matcher matcher = pattern.matcher(query);

    if (matcher.find()) {
      language = matcher.group(1);
      queryText = query.substring(matcher.end());
    }

    return new Query(language, queryText);
  }

  private void emptyQueryResponse() {
    writeMessage("empty query response", null, 'I', 4);
  }

  private void portalSuspendedResponse() {
    writeMessage("portal suspended response", null, 's', 4);
  }

  private void setErrorInTx() {
    if (explicitTransactionStarted)
      errorInTransaction = true;
  }

  private record Query(String language, String query) {
  }

}
