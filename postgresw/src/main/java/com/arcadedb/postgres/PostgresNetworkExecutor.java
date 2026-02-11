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
import com.arcadedb.schema.Property;
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
import java.util.concurrent.ThreadLocalRandom;
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
      final long secret = Math.abs(ThreadLocalRandom.current().nextInt(10000000));
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

  private void flushCommand() throws IOException {
    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: flush (thread=%s)", Thread.currentThread().threadId());
    // Flush message does NOT generate any response according to PostgreSQL protocol.
    // It just forces the backend to deliver any data pending in its output buffers.
    // See: https://www.postgresql.org/docs/current/protocol-message-formats.html
    channel.flush();
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
          portal.rowDescriptionSent = true;
        } else
          writeNoData();
      } else {
        if (portal.columns != null) {
          writeRowDescription(portal.columns);
          portal.rowDescriptionSent = true;
        }
      }
    } else if (type == 'S') {
      // Describe Statement: send ParameterDescription followed by RowDescription/NoData
      // This tells the client how many parameters the prepared statement expects
      writeParameterDescription(portal);

      // Now send RowDescription or NoData
      // For SELECT queries, we need to determine the columns from the type schema
      if (portal.isExpectingResult && portal.columns == null) {
        portal.columns = getColumnsFromQuerySchema(portal.query);
      }

      if (portal.columns != null && !portal.columns.isEmpty()) {
        writeRowDescription(portal.columns);
        portal.rowDescriptionSent = true;
      } else {
        // We can't determine columns at DESCRIBE time (e.g., INSERT without schema info)
        // Send NoData, but keep isExpectingResult = true so EXECUTE can handle it properly
        // The actual query execution will determine if there are results
        writeNoData();
      }
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
            // Only send RowDescription if not already sent during DESCRIBE
            // But always use columns from actual result for DataRows consistency
            if (!portal.rowDescriptionSent) {
              portal.columns = getColumns(portal.cachedResultSet);
              writeRowDescription(portal.columns);
              portal.rowDescriptionSent = true;
            }
          }
        }

        if (portal.isExpectingResult && portal.cachedResultSet != null && !portal.cachedResultSet.isEmpty()) {
          // Query returned results - send them
          final Map<String, PostgresType> dataRowColumns = getColumns(portal.cachedResultSet);

          if (DEBUG)
            LogManager.instance().log(this, Level.INFO,
                "PSQL: executeCommand columns - portal.columns=%s, dataRowColumns=%s, resultSize=%d (thread=%s)",
                portal.columns != null ? portal.columns.keySet() : "null",
                dataRowColumns.keySet(),
                portal.cachedResultSet.size(),
                Thread.currentThread().threadId());

          // If RowDescription wasn't sent during DESCRIBE (e.g., INSERT with RETURN),
          // we need to send it now before the data rows
          if (!portal.rowDescriptionSent) {
            portal.columns = dataRowColumns;
            writeRowDescription(portal.columns);
            portal.rowDescriptionSent = true;
          }

          // Verify column count matches what was sent in RowDescription
          if (portal.columns != null && portal.columns.size() != dataRowColumns.size()) {
            // Column count mismatch - use the original columns from DESCRIBE
            // This can happen if sample query returned different properties than actual query
            if (DEBUG)
              LogManager.instance().log(this, Level.WARNING,
                  "PSQL: Column count mismatch - RowDesc=%d, DataRow=%d (thread=%s)",
                  portal.columns.size(), dataRowColumns.size(), Thread.currentThread().threadId());
          }

          // Use the columns that were sent in RowDescription for consistency
          final Map<String, PostgresType> columnsToUse = portal.columns != null ? portal.columns : dataRowColumns;
          writeDataRows(portal.cachedResultSet, columnsToUse);
          writeCommandComplete(portal.query, portal.cachedResultSet.size());
        } else {
          // Query doesn't return data (INSERT/UPDATE/DELETE without RETURNING) or empty result
          final int affectedRows = portal.cachedResultSet != null ? portal.cachedResultSet.size() : 0;
          writeCommandComplete(portal.query, affectedRows);
        }
      }
    } catch (final CommandParsingException e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), "42601");
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
      if (query.query.toUpperCase(Locale.ENGLISH).startsWith("SET ")) {
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
        // Check for pg_type/pg_catalog queries from JDBC drivers
        final List<Result> pgTypeResult = handlePgTypeQuery(query.query);
        if (pgTypeResult != null) {
          resultSet = new IteratorResultSet(pgTypeResult.iterator());
        } else {
          resultSet = database.command(query.language, query.query, server.getConfiguration());
        }
      }
      final List<Result> cachedResultSet = browseAndCacheResultSet(resultSet, 0);

      final Map<String, PostgresType> columns = getColumns(cachedResultSet);
      writeRowDescription(columns);
      writeDataRows(cachedResultSet, columns);
      writeCommandComplete(queryText, cachedResultSet.size());

    } catch (final CommandParsingException e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), "42601");
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
    return browseAndCacheResultSet(resultSet, limit, true);
  }

  /**
   * Browse a result set and cache results up to the limit.
   *
   * @param resultSet           The result set to browse
   * @param limit               Maximum number of results to cache (0 = unlimited)
   * @param sendSuspendedOnLimit If true and limit is reached, sends PortalSuspended message.
   *                            Set to false for internal queries (like schema discovery) that
   *                            should not send protocol messages.
   */
  private List<Result> browseAndCacheResultSet(final ResultSet resultSet, final int limit, final boolean sendSuspendedOnLimit) {
    final List<Result> cachedResultSet = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      if (row == null)
        continue;

      cachedResultSet.add(row);

      if (limit > 0 && cachedResultSet.size() >= limit) {
        if (sendSuspendedOnLimit)
          portalSuspendedResponse();
        break;
      }
    }
    return cachedResultSet;
  }

  private Object[] getParams(PostgresPortal portal) {
    Object[] parameters = portal.parameterValues != null ? portal.parameterValues.toArray() : new Object[0];

    if (portal.language.equals("cypher") || portal.language.equals("opencypher")) {
      Object[] parametersCypher = new Object[parameters.length * 2];
      for (int i = 0; i < parameters.length; i++) {
        parametersCypher[i * 2] = "" + (i + 1);
        parametersCypher[i * 2 + 1] = portal.parameterValues.get(i);
      }
      return parametersCypher;
    }

    return parameters;
  }

  /**
   * Handles PostgreSQL system catalog queries (pg_type, pg_catalog) from JDBC drivers.
   * These queries are used by JDBC drivers to introspect types, especially for arrays.
   *
   * @param query The SQL query to check
   * @return A list of results if this is a pg_type query we handle, null otherwise
   */
  private List<Result> handlePgTypeQuery(final String query) {
    final String upperQuery = query.toUpperCase();

    // Check if this is a pg_type/pg_catalog query
    if (!upperQuery.contains("PG_TYPE") && !upperQuery.contains("PG_CATALOG")) {
      return null;
    }

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: handling pg_type query: %s (thread=%s)",
          query, Thread.currentThread().threadId());

    // Handle common JDBC driver queries for array type information
    // Query pattern: SELECT e.typdelim, e.typname FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = <oid> AND t.typelem = e.oid
    // or: SELECT typelem FROM pg_type WHERE oid = <oid>
    // or: SELECT ... FROM pg_type WHERE typname = '<name>'

    // Extract OID from the query if present
    Pattern oidPattern = Pattern.compile("(?:t\\.oid|oid)\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    Matcher oidMatcher = oidPattern.matcher(query);

    if (oidMatcher.find()) {
      int oid = Integer.parseInt(oidMatcher.group(1));

      // Map array type OIDs to their element type information
      // PostgreSQL array types and their element types:
      // 1007 = int4[] -> 23 = int4, 1009 = text[] -> 25 = text, etc.
      String elemTypeName = null;
      String typDelim = ",";
      int elemOid = 0;

      switch (oid) {
        case 1007 -> { elemTypeName = "int4"; elemOid = 23; }      // int4[]
        case 1009 -> { elemTypeName = "text"; elemOid = 25; }      // text[]
        case 1016 -> { elemTypeName = "int8"; elemOid = 20; }      // int8[]
        case 1021 -> { elemTypeName = "float4"; elemOid = 700; }   // float4[]
        case 1022 -> { elemTypeName = "float8"; elemOid = 701; }   // float8[]
        case 1000 -> { elemTypeName = "bool"; elemOid = 16; }      // bool[]
        case 1003 -> { elemTypeName = "char"; elemOid = 18; }      // char[]
        case 199 -> { elemTypeName = "json"; elemOid = 114; }      // json[]
        case 1043 -> { elemTypeName = "varchar"; elemOid = 0; }    // varchar (not an array)
        default -> { elemTypeName = "text"; elemOid = 25; }        // Default to text for unknown arrays
      }

      // Determine what columns the query is requesting
      final Map<String, Object> map = new HashMap<>();

      if (upperQuery.contains("TYPELEM")) {
        map.put("typelem", elemOid);
      }
      if (upperQuery.contains("TYPDELIM")) {
        map.put("typdelim", typDelim);
      }
      if (upperQuery.contains("TYPNAME") || upperQuery.contains("E.TYPNAME")) {
        map.put("typname", elemTypeName);
      }
      if (upperQuery.contains("TYPARRAY")) {
        // typarray is the OID of the array type for this scalar type
        // For scalar types, return the corresponding array OID
        map.put("typarray", oid);
      }
      if (upperQuery.contains("TYPTYPE")) {
        map.put("typtype", "b"); // 'b' = base type
      }
      if (upperQuery.contains("TYPINPUT")) {
        map.put("typinput", "array_in");
      }

      // Only return a result if we matched an array type and have data to return
      if (!map.isEmpty() && elemOid > 0) {
        final Result result = new ResultInternal(map);
        return Collections.singletonList(result);
      }
    }

    // Handle query by type name pattern
    Pattern namePattern = Pattern.compile("typname\\s*=\\s*'([^']+)'", Pattern.CASE_INSENSITIVE);
    Matcher nameMatcher = namePattern.matcher(query);
    if (nameMatcher.find()) {
      String typeName = nameMatcher.group(1);

      // Map common type names to their OIDs
      final Map<String, Object> map = new HashMap<>();
      switch (typeName.toLowerCase()) {
        case "_int4" -> { map.put("oid", 1007); map.put("typelem", 23); }
        case "_int8" -> { map.put("oid", 1016); map.put("typelem", 20); }
        case "_text" -> { map.put("oid", 1009); map.put("typelem", 25); }
        case "_float4" -> { map.put("oid", 1021); map.put("typelem", 700); }
        case "_float8" -> { map.put("oid", 1022); map.put("typelem", 701); }
        case "_bool" -> { map.put("oid", 1000); map.put("typelem", 16); }
        case "int4" -> { map.put("oid", 23); map.put("typelem", 0); }
        case "int8" -> { map.put("oid", 20); map.put("typelem", 0); }
        case "text" -> { map.put("oid", 25); map.put("typelem", 0); }
        case "varchar" -> { map.put("oid", 1043); map.put("typelem", 0); }
        default -> {
          // Return empty result for unknown types
          return Collections.emptyList();
        }
      }

      if (!map.isEmpty()) {
        map.put("typname", typeName);
        map.put("typdelim", ",");
        final Result result = new ResultInternal(map);
        return Collections.singletonList(result);
      }
    }

    // For other pg_type queries we don't specifically handle, return empty result
    // This prevents errors from trying to query non-existent tables
    return Collections.emptyList();
  }

  private Map<String, PostgresType> getColumns(final List<Result> resultSet) {
    final Map<String, PostgresType> columns = new LinkedHashMap<>();

    boolean atLeastOneElement = false;
    for (final Result row : resultSet) {
      if (row.isElement())
        atLeastOneElement = true;

      final Set<String> propertyNames = row.getPropertyNames();
      for (final String p : propertyNames) {
        if (!columns.containsKey(p)) {
          // Determine the PostgreSQL type based on the actual value
          // For arrays/collections, use proper array type codes so JDBC drivers can parse them
          // For scalar values, use VARCHAR since we serialize everything as text
          final Object value = row.getProperty(p);
          final PostgresType pgType = PostgresType.getTypeForValue(value);

          // For array types, use the detected array type so clients can properly parse arrays
          // For non-array types, use VARCHAR to ensure consistent text serialization
          if (pgType.isArrayType()) {
            columns.put(p, pgType);
          } else {
            columns.put(p, PostgresType.VARCHAR);
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

  /**
   * Extract column schema from a SELECT query by parsing the type name and querying for a sample row.
   * This is used during DESCRIBE Statement to return RowDescription before the query is executed.
   * ArcadeDB is schema-less so we need to query actual data to discover dynamically-added properties.
   */
  private Map<String, PostgresType> getColumnsFromQuerySchema(final String query) {
    if (query == null || query.isEmpty()) {
      return null;
    }

    // Try to extract the type name from the query
    // Patterns: "SELECT FROM TypeName", "SELECT * FROM TypeName", "SELECT ... FROM TypeName"
    final String upperQuery = query.toUpperCase();
    final int fromIndex = upperQuery.indexOf(" FROM ");
    if (fromIndex < 0) {
      return null;
    }

    String afterFrom = query.substring(fromIndex + 6).trim();

    // Extract type name (ends at WHERE, LIMIT, ORDER, or end of string)
    String typeName = afterFrom;
    for (String terminator : new String[]{" WHERE ", " LIMIT ", " ORDER ", " GROUP ", ";"}) {
      final int idx = typeName.toUpperCase().indexOf(terminator);
      if (idx > 0) {
        typeName = typeName.substring(0, idx);
      }
    }
    typeName = typeName.trim();

    // Skip schema: prefix if present
    if (typeName.toLowerCase().startsWith("schema:")) {
      return null; // Schema queries have different structure
    }

    try {
      // First verify the type exists
      final DocumentType docType = database.getSchema().getType(typeName);
      if (docType == null) {
        return null;
      }

      // Query for a sample row to discover all properties (including dynamically-added ones)
      // Use LIMIT 1 to minimize overhead
      // Use sendSuspendedOnLimit=false because this is an internal query for schema discovery,
      // not a client-initiated query that should send protocol messages
      final String sampleQuery = "SELECT FROM " + typeName + " LIMIT 1";
      final ResultSet resultSet = database.query("sql", sampleQuery, server.getConfiguration());
      final List<Result> sampleRows = browseAndCacheResultSet(resultSet, 1, false);

      if (!sampleRows.isEmpty()) {
        // Use the sample row to discover columns
        final Map<String, PostgresType> cols = getColumns(sampleRows);
        if (DEBUG)
          LogManager.instance().log(this, Level.INFO,
              "PSQL: getColumnsFromQuerySchema('%s') -> type=%s, sampleQuery='%s', found %d rows, columns=%s (thread=%s)",
              query, typeName, sampleQuery, sampleRows.size(), cols.keySet(), Thread.currentThread().threadId());
        return cols;
      }

      // If no rows exist, fall back to schema-defined properties
      final Map<String, PostgresType> columns = new LinkedHashMap<>();

      // Add system properties first (these are returned for document/vertex types)
      columns.put(RID_PROPERTY, PostgresType.VARCHAR);
      columns.put(TYPE_PROPERTY, PostgresType.VARCHAR);
      columns.put(CAT_PROPERTY, PostgresType.CHAR);

      // Add all defined properties from the type
      for (final String propName : docType.getPropertyNames()) {
        final Property prop = docType.getProperty(propName);
        if (prop != null && prop.getType() != null) {
          columns.put(propName, PostgresType.getTypeFromArcade(prop.getType()));
        } else {
          columns.put(propName, PostgresType.VARCHAR);
        }
      }

      return columns;

    } catch (Exception e) {
      if (DEBUG)
        LogManager.instance().log(this, Level.WARNING, "PSQL: failed to get columns from schema for query '%s': %s",
            query, e.getMessage());
      return null;
    }
  }

  private void writeRowDescription(final Map<String, PostgresType> columns) {
    if (columns == null)
      return;

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL:-> RowDescription: %d columns: %s (thread=%s)",
          columns.size(), columns.keySet(), Thread.currentThread().threadId());

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
      final int dataRowLength = 4 + bufferValues.getByteBuffer().limit();
      bufferData.putByte((byte) 'D');
      bufferData.putInt(dataRowLength);
      bufferData.putBuffer(bufferValues.getByteBuffer());

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO,
            "PSQL:-> DataRow: cols=%d, bufferValues=%d, dataRowLength=%d, bufferData=%d (thread=%s)",
            columns.size(), bufferValues.getByteBuffer().limit(), dataRowLength,
            bufferData.position(), Thread.currentThread().threadId());

      bufferData.flip();
      channel.writeBuffer(bufferData.getByteBuffer());

      bufferData.clear();
      bufferValues.clear();
    }

    channel.flush();

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL:-> %d row(s) data written (thread=%s)", resultSet.size(),
          Thread.currentThread().threadId());
  }

  private void bindCommand() {
    try {
      // BIND
      final String portalName = readString();
      final String sourcePreparedStatement = readString();

      // Look up the prepared statement (stored during PARSE)
      // The portal name may be different (often empty for unnamed portal)
      PostgresPortal portal = getPortal(sourcePreparedStatement, false);
      if (portal == null) {
        // Try with portal name as fallback for backwards compatibility
        portal = getPortal(portalName, false);
      }
      if (portal == null) {
        writeMessage("bind complete", null, '2', 4);
        return;
      }

      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: bind (portal=%s) -> %s (thread=%s)", portalName, sourcePreparedStatement,
                Thread.currentThread().threadId());

      final int paramFormatCount = channel.readShort();
      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: bind paramFormatCount=%d (thread=%s)",
            paramFormatCount, Thread.currentThread().threadId());
      if (paramFormatCount > 0) {
        portal.parameterFormats = new ArrayList<>(paramFormatCount);
        for (int i = 0; i < paramFormatCount; i++) {
          final int formatCode = channel.readUnsignedShort();
          portal.parameterFormats.add(formatCode);
        }
      }

      final int paramValuesCount = channel.readShort();
      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: bind paramValuesCount=%d (thread=%s)",
            paramValuesCount, Thread.currentThread().threadId());
      if (paramValuesCount > 0) {
        portal.parameterValues = new ArrayList<>(paramValuesCount);
        for (int i = 0; i < paramValuesCount; i++) {
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind reading param %d size (thread=%s)", i, Thread.currentThread().threadId());
          final long paramSize = channel.readUnsignedInt();
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind param %d size=%d (thread=%s)", i, paramSize, Thread.currentThread().threadId());
          final byte[] paramValue = new byte[(int) paramSize];
          channel.readBytes(paramValue);
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind param %d value read (thread=%s)", i, Thread.currentThread().threadId());

          // Determine format code according to PostgreSQL protocol:
          // - If paramFormatCount == 0: all parameters use text format (0)
          // - If paramFormatCount == 1: all parameters use that single format code
          // - Otherwise: each parameter uses its corresponding format code
          final int formatCode;
          if (portal.parameterFormats == null || portal.parameterFormats.isEmpty()) {
            formatCode = 0; // Default to text format
          } else if (portal.parameterFormats.size() == 1) {
            formatCode = portal.parameterFormats.get(0); // Single format for all
          } else {
            formatCode = portal.parameterFormats.get(i); // Per-parameter format
          }

          // Determine type code - use UNSPECIFIED (0) if not declared in PARSE
          final long typeCode = (portal.parameterTypes != null && i < portal.parameterTypes.size())
              ? portal.parameterTypes.get(i)
              : 0L; // UNSPECIFIED type

          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind deserializing param %d typeCode=%d formatCode=%d (thread=%s)",
                i, typeCode, formatCode, Thread.currentThread().threadId());
          portal.parameterValues.add(PostgresType.deserialize(typeCode, formatCode, paramValue));
          if (DEBUG)
            LogManager.instance().log(this, Level.INFO, "PSQL: bind param %d deserialized (thread=%s)", i, Thread.currentThread().threadId());
        }
      }

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: bind reading resultFormatCount (thread=%s)", Thread.currentThread().threadId());
      final int resultFormatCount = channel.readShort();
      if (resultFormatCount > 0) {
        portal.resultFormats = new ArrayList<>(resultFormatCount);
        for (int i = 0; i < resultFormatCount; i++) {
          final int resultFormat = channel.readUnsignedShort();
          portal.resultFormats.add(resultFormat);
        }
        if (DEBUG)
          LogManager.instance().log(this, Level.INFO, "PSQL: bind resultFormats=%s (0=text, 1=binary) (thread=%s)",
              portal.resultFormats, Thread.currentThread().threadId());
      }

      if (errorInTransaction)
        return;

      // Store the portal under the portal name (which may be empty for unnamed portal)
      // This is necessary because EXECUTE looks up portals by portal name, not prepared statement name
      // PostgreSQL protocol: PARSE creates "prepared statement", BIND creates "portal" from it
      if (!portalName.equals(sourcePreparedStatement)) {
        portals.put(portalName, portal);
        if (DEBUG)
          LogManager.instance().log(this, Level.INFO, "PSQL: bind stored portal under name '%s' (thread=%s)",
              portalName, Thread.currentThread().threadId());
      }

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
      } else {
        // Client sent paramCount=0 (e.g., asyncpg, node-postgres)
        // Detect $N placeholders in the query to determine actual parameter count
        final int detectedParams = detectParameterPlaceholders(query.query);
        if (detectedParams > 0) {
          portal.parameterTypes = new ArrayList<>(detectedParams);
          for (int i = 0; i < detectedParams; i++) {
            // Use VARCHAR (OID 1043) as default type instead of 0 (unspecified)
            // This prevents asyncpg from trying to introspect unknown types via pg_type
            portal.parameterTypes.add((long) PostgresType.VARCHAR.code);
          }
        }
      }

      final int actualParamCount = portal.parameterTypes != null ? portal.parameterTypes.size() : 0;
      if (DEBUG)
        LogManager.instance()
            .log(this, Level.INFO, "PSQL: parse (portal=%s) -> %s (params=%d, detected=%d) (errorInTransaction=%s thread=%s)",
                portalName, portal.query, paramCount, actualParamCount, errorInTransaction, Thread.currentThread().threadId());

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
        // Check for pg_type/pg_catalog queries from JDBC drivers (extended query protocol)
        final List<Result> pgTypeResult = handlePgTypeQuery(portal.query);
        if (pgTypeResult != null) {
          // pg_type query handled - set up the portal with results
          portal.executed = true;
          portal.cachedResultSet = pgTypeResult;
          portal.columns = getColumns(pgTypeResult);
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
      }

      portals.put(portalName, portal);

      // ParseComplete
      writeMessage("parse complete", null, '1', 4);

    } catch (final CommandParsingException e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on parsing query: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), "42601");
    } catch (final Exception e) {
      setErrorInTx();
      writeError(ERROR_SEVERITY.ERROR, "Error on parsing query: " + e.getMessage(), "XX000");
    }
  }

  private void setConfiguration(final String query) {
    final int setLength = "SET ".length();
    // Use original query to preserve case of values
    final String q = query.substring(setLength);

    // Try to split by either '=' or ' TO ' (case-insensitive)
    String[] parts = q.split("=");
    if (parts.length < 2) {
      // Try case-insensitive split for " TO "
      parts = q.split("(?i)\\s+TO\\s+");
    }

    if (parts.length < 2) {
      LogManager.instance().log(this, Level.WARNING, "Invalid SET command format: %s", query);
      return;
    }

    parts[0] = parts[0].trim();
    parts[1] = parts[1].trim();

    if (parts[1].startsWith("'") || parts[1].startsWith("\""))
      parts[1] = parts[1].substring(1, parts[1].length() - 1);

    // Use case-insensitive comparison for parameter names
    final String paramName = parts[0].toLowerCase(Locale.ENGLISH);
    if (paramName.equals("datestyle")) {
      if (parts[1].equalsIgnoreCase("ISO"))
        database.getSchema().setDateTimeFormat(DateUtils.DATE_TIME_ISO_8601_FORMAT);
      else
        LogManager.instance().log(this, Level.INFO, "datestyle '%s' not supported", parts[1]);
    }

    connectionProperties.put(paramName, parts[1]);
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

  /**
   * Writes ParameterDescription message ('t') describing the parameters of a prepared statement.
   * This is required by the PostgreSQL extended query protocol for DESCRIBE 'S' (Statement).
   */
  private void writeParameterDescription(final PostgresPortal portal) {
    final int paramCount = portal.parameterTypes != null ? portal.parameterTypes.size() : 0;
    // Message format: 't' + int32 length + int16 param count + int32[] type OIDs
    final int messageLength = 4 + 2 + (paramCount * 4);

    writeMessage("parameter description", () -> {
      channel.writeShort((short) paramCount);
      if (portal.parameterTypes != null) {
        for (final Long typeOid : portal.parameterTypes) {
          channel.writeUnsignedInt(typeOid != null ? typeOid.intValue() : 0); // 0 = unspecified type
        }
      }
    }, 't', messageLength);
  }

  /**
   * Detects $N style parameter placeholders in a query and returns the count.
   * PostgreSQL uses $1, $2, etc. for positional parameters.
   * Returns the highest parameter number found (e.g., "$3" returns 3).
   */
  private int detectParameterPlaceholders(final String query) {
    int maxParam = 0;
    final Pattern pattern = Pattern.compile("\\$(\\d+)");
    final Matcher matcher = pattern.matcher(query);
    while (matcher.find()) {
      final int paramNum = Integer.parseInt(matcher.group(1));
      if (paramNum > maxParam) {
        maxParam = paramNum;
      }
    }
    return maxParam;
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
