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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.postgresql.util.PGobject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * openCypher over the PostgreSQL wire protocol: JDBC, each statement prefixed with {@value #MARKER} so the server hands it to
 * its openCypher engine. The PostgresProtocolPlugin must be enabled.
 * <p>
 * JDBC parameters are positional, and the server names the n-th one {@code $n} for openCypher. So every {@code $name} of the
 * statement (outside a string literal, a comment or a backticked name) becomes a {@code ?}, and the values are bound in the
 * order the names occur, a name used twice bound twice. Scalars bind as their SQL type; a list (or an array) binds as a json
 * parameter, which the server reads back into a list of maps, lists and numbers, so a list of maps and a list of float lists
 * both cross. A map parameter does not: the server keeps a json object as a JSON document, not as a map, and this client
 * refuses it with {@link UnsupportedOperationException} rather than send something the query would read differently.
 */
public final class PostgresProtocolClient implements ProtocolClient {
  static final String MARKER = "{cypher}";

  private final Connection connection;

  /** A statement as JDBC takes it: the text with {@code ?} placeholders and the parameter name each placeholder stands for. */
  record Translated(String sql, List<String> names) {
  }

  public PostgresProtocolClient(final ServerUnderTest server, final String database) {
    final Properties props = new Properties();
    props.setProperty("user", USER);
    props.setProperty("password", ServerUnderTest.PASSWORD);
    props.setProperty("ssl", "false");
    try {
      this.connection = DriverManager.getConnection(
          "jdbc:postgresql://" + server.wrapper().host() + ":" + server.postgresPort() + "/" + database, props);
    } catch (final SQLException e) {
      throw new IllegalStateException("Cannot connect over PostgreSQL to database '" + database + "'", e);
    }
  }

  @Override
  public List<Map<String, Object>> query(final String cypher, final Map<String, Object> params) {
    final Translated t = translate(cypher);
    try (final PreparedStatement st = connection.prepareStatement(t.sql())) {
      bind(st, t.names(), params);
      try (final ResultSet rs = st.executeQuery()) {
        final ResultSetMetaData meta = rs.getMetaData();
        final int columns = meta.getColumnCount();
        final String[] names = new String[columns];
        for (int c = 0; c < columns; c++)
          names[c] = meta.getColumnLabel(c + 1);
        final List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
          final Map<String, Object> row = new LinkedHashMap<>(columns * 2);
          for (int c = 0; c < columns; c++)
            row.put(names[c], fromJdbc(rs.getObject(c + 1)));
          rows.add(row);
        }
        return rows;
      }
    } catch (final SQLException e) {
      throw new IllegalStateException("PostgreSQL query failed: " + cypher, e);
    }
  }

  @Override
  public void transaction(final List<String> cyphers, final List<Map<String, Object>> params) {
    // every statement is translated and its parameters checked before anything is sent
    final List<Translated> statements = new ArrayList<>(cyphers.size());
    for (int i = 0; i < cyphers.size(); i++) {
      final Translated t = translate(cyphers.get(i));
      final Map<String, Object> p = ProtocolClient.paramsOf(params, i);
      for (final String name : t.names())
        checkBindable(name, p.get(name));
      statements.add(t);
    }
    try {
      connection.setAutoCommit(false);
      Exception failure = null;
      try {
        for (int i = 0; i < statements.size(); i++)
          try (final PreparedStatement st = connection.prepareStatement(statements.get(i).sql())) {
            bind(st, statements.get(i).names(), ProtocolClient.paramsOf(params, i));
            st.execute();
          }
        connection.commit();
      } catch (final SQLException | RuntimeException e) {
        failure = e;
        try {
          connection.rollback();
        } catch (final SQLException rollback) {
          e.addSuppressed(rollback);
        }
        throw e;
      } finally {
        try {
          connection.setAutoCommit(true);
        } catch (final SQLException restore) {
          // never replace the failure of the transaction with the one of restoring the mode
          if (failure == null)
            throw restore;
          failure.addSuppressed(restore);
        }
      }
    } catch (final SQLException e) {
      throw new IllegalStateException("PostgreSQL transaction failed", e);
    }
  }

  /**
   * The statement with the {@value #MARKER} prefix and its {@code $name} parameters as {@code ?} placeholders, in order. String
   * literals ('...' and "..."), backticked names and comments are copied as they are.
   */
  static Translated translate(final String cypher) {
    final StringBuilder sql = new StringBuilder(MARKER.length() + cypher.length()).append(MARKER);
    final List<String> names = new ArrayList<>();
    final int length = cypher.length();
    int i = 0;
    while (i < length) {
      final char c = cypher.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        final int end = endOfQuoted(cypher, i, c);
        sql.append(cypher, i, end);
        i = end;
      } else if (c == '/' && i + 1 < length && cypher.charAt(i + 1) == '/') {
        final int nl = cypher.indexOf('\n', i);
        final int end = nl < 0 ? length : nl;
        sql.append(cypher, i, end);
        i = end;
      } else if (c == '/' && i + 1 < length && cypher.charAt(i + 1) == '*') {
        final int close = cypher.indexOf("*/", i + 2);
        final int end = close < 0 ? length : close + 2;
        sql.append(cypher, i, end);
        i = end;
      } else if (c == '$' && i + 1 < length && Character.isJavaIdentifierPart(cypher.charAt(i + 1))) {
        int end = i + 1;
        while (end < length && Character.isJavaIdentifierPart(cypher.charAt(end)))
          ++end;
        names.add(cypher.substring(i + 1, end));
        sql.append('?');
        i = end;
      } else {
        sql.append(c);
        ++i;
      }
    }
    return new Translated(sql.toString(), names);
  }

  /** The index just past the closing quote of the literal opened at {@code start}; a backslash escapes the next char. */
  private static int endOfQuoted(final String text, final int start, final char quote) {
    int i = start + 1;
    while (i < text.length()) {
      final char c = text.charAt(i);
      if (c == '\\' && quote != '`')
        i += 2;
      else if (c == quote)
        return i + 1;
      else
        ++i;
    }
    return text.length();
  }

  private static void bind(final PreparedStatement st, final List<String> names, final Map<String, Object> params)
      throws SQLException {
    for (int i = 0; i < names.size(); i++) {
      final String name = names.get(i);
      if (params == null || !params.containsKey(name))
        throw new IllegalArgumentException("Parameter '" + name + "' has no value");
      final Object value = params.get(name);
      checkBindable(name, value);
      final int index = i + 1;
      switch (value) {
        case null -> st.setNull(index, Types.NULL);
        case String s -> st.setString(index, s);
        case Boolean b -> st.setBoolean(index, b);
        case Double d -> st.setDouble(index, d);
        case Float f -> st.setDouble(index, f);
        case Number n -> st.setLong(index, n.longValue());
        default -> {
          // a Collection or an array, checked above: json, which the server reads back into lists, maps and numbers
          final PGobject json = new PGobject();
          json.setType("json");
          json.setValue(new JSONArray((Collection<?>) Parity.normalise(value)).toString());
          st.setObject(index, json);
        }
      }
    }
  }

  /** Refuses a value this client cannot send so that the server reads it as the same value. */
  static void checkBindable(final String name, final Object value) {
    if (value == null || value instanceof String || value instanceof Boolean || value instanceof Collection<?> || value.getClass()
        .isArray())
      return;
    if (value instanceof Double || value instanceof Float || value instanceof Long || value instanceof Integer
        || value instanceof Short || value instanceof Byte)
      return;
    if (value instanceof Map<?, ?>)
      throw new UnsupportedOperationException("Parameter '" + name + "' is a map, which cannot be bound over PostgreSQL: the "
          + "server reads a json object parameter as a JSON document, not as a map");
    throw new UnsupportedOperationException(
        "Parameter '" + name + "' of type " + value.getClass().getSimpleName() + " cannot be bound over PostgreSQL");
  }

  /** A JDBC value as the plain Java value the other clients produce: an SQL array as a list, a json document parsed. */
  private static Object fromJdbc(final Object value) throws SQLException {
    if (value instanceof Array array) {
      try {
        final Object[] elements = (Object[]) array.getArray();
        final List<Object> list = new ArrayList<>(elements.length);
        for (final Object e : elements)
          list.add(fromJdbc(e));
        return list;
      } finally {
        array.free();
      }
    }
    if (value instanceof PGobject object && object.getValue() != null && ("json".equals(object.getType()) || "jsonb".equals(
        object.getType()))) {
      final String text = object.getValue().trim();
      if (text.startsWith("["))
        return new JSONArray(text).toList();
      if (text.startsWith("{"))
        return new JSONObject(text).toMap();
      return new JSONArray("[" + text + "]").toList().getFirst();
    }
    return value;
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (final SQLException e) {
      throw new IllegalStateException("Cannot close the PostgreSQL connection", e);
    }
  }
}
