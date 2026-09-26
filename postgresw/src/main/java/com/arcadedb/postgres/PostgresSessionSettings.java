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

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * The run-time parameters of ONE Postgres wire connection: what its {@code SET}/{@code RESET} commands and its startup
 * packet established, and what {@code SHOW} answers (issue #8217). A PostgreSQL {@code SET} is session-scoped, so
 * nothing here reaches the database, the schema or any other connection.
 * <p>
 * {@code SHOW} answers the value this connection set, else the value its startup packet named, else the server
 * default. A few parameters describe what this server actually does rather than a preference the client can change,
 * and {@code SHOW} always answers the truth for those:
 * <ul>
 *   <li>{@code server_version}, {@code server_encoding}, {@code integer_datetimes}, {@code is_superuser} are read-only
 *   in PostgreSQL too, and a {@code SET} of them is refused with {@code 55P02} as PostgreSQL refuses it;</li>
 *   <li>{@code client_encoding} is accepted - drivers send it routinely - but every text value leaves this server in
 *   UTF-8, so {@code SHOW} answers {@code UTF8};</li>
 *   <li>{@code standard_conforming_strings} is accepted but {@code SHOW} answers {@code on}: a plain {@code '...'}
 *   literal is never backslash-interpreted here, and a driver that reads {@code off} back would escape for a parser
 *   this server does not have;</li>
 *   <li>{@code DateStyle} keeps the field order the client asks for, but its output style is always {@code ISO}: that
 *   is the only format {@code PostgresType} writes a date or timestamp in;</li>
 *   <li>{@code role} and {@code session_authorization} (also {@code SET ROLE}, {@code SET SESSION AUTHORIZATION}) are
 *   accepted only to reset them: a connection always runs with the privileges of the user it authenticated as, and a
 *   client told its role changed would believe it had dropped privileges it still holds (issue #8392);</li>
 *   <li>{@code transaction_isolation} and {@code default_transaction_isolation} (also {@code SET TRANSACTION} and
 *   {@code SET SESSION CHARACTERISTICS AS TRANSACTION}) are accepted for the level the transactions really run at and
 *   refused with {@code 0A000} for any other; {@code transaction_read_only} and {@code default_transaction_read_only}
 *   are accepted only as {@code off}, since this server has no read-only transactions (issue #8392).</li>
 * </ul>
 * <b>Transaction scope</b> (issue #8242), as in PostgreSQL: a {@code SET} made inside a transaction that is then rolled
 * back is undone by the rollback, and a {@code SET LOCAL} lasts only until the end of the transaction it was made in,
 * committed or not. The executor reports the end of every transaction through {@link #commit()} and
 * {@link #rollback()}; the values as they stood before the first {@code SET} of the transaction are copied then, and
 * only then, so a transaction that sets nothing pays nothing. {@code SET x TO DEFAULT} and {@code RESET x} restore
 * what PostgreSQL calls the reset value: the one the startup packet named, else the server default.
 * <p>
 * <b>Reporting</b> (issue #8241): PostgreSQL announces every {@code GUC_REPORT} parameter in a {@code ParameterStatus}
 * message at startup and again whenever its value changes, and drivers cache those values instead of asking
 * ({@code DateStyle}, {@code TimeZone}, {@code client_encoding}, ...). {@link #reportChanges} hands the executor what
 * changed since the last report - the value {@code SHOW} answers, never the raw {@code SET} text, which is what keeps
 * pgjdbc's checks on a reported {@code DateStyle} and {@code client_encoding} satisfied.
 * <p>
 * Not thread-safe: a connection is served by one thread.
 */
final class PostgresSessionSettings {
  static final String DATESTYLE = "datestyle";

  /**
   * The parameters PostgreSQL marks {@code GUC_REPORT} and this server answers, spelled as PostgreSQL spells them in
   * a {@code ParameterStatus} message.
   */
  static final String[] REPORTED_PARAMETERS = { "application_name", "client_encoding", "DateStyle", "integer_datetimes",
      "IntervalStyle", "is_superuser", "server_encoding", "server_version", "standard_conforming_strings", "TimeZone" };

  private static final String  DEFAULT_DATE_ORDER  = "MDY";
  private static final Pattern DATESTYLE_SEPARATOR = Pattern.compile("[,\\s]+");

  /**
   * A parsed {@code SET [SESSION | LOCAL] <name> = <value>}, {@code RESET <name>} or {@code RESET ALL}. A null
   * {@code value} restores the reset value ({@code DEFAULT}, {@code RESET}); a null {@code name} is {@code RESET ALL}.
   */
  record Assignment(String name, String value, boolean local) {
    static final Assignment RESET_ALL = new Assignment(null, null, false);
    /**
     * A {@code SET} PostgreSQL accepts and that changes nothing here, as it changes nothing in PostgreSQL either (e.g.
     * {@code SET CONSTRAINTS ALL DEFERRED} with no deferrable constraint). Compared by identity.
     */
    static final Assignment NO_OP     = new Assignment("", null, false);
  }

  static final String SQLSTATE_FEATURE_NOT_SUPPORTED = "0A000";
  static final String SQLSTATE_INVALID_VALUE         = "22023";
  static final String READ_ONLY_NOT_SUPPORTED        =
      "read-only transactions are not supported by this server: a transaction declared READ ONLY would still accept writes";

  /**
   * Thrown when a {@code SET} is refused, carrying the SQLSTATE PostgreSQL answers the same refusal with.
   */
  static final class SettingException extends RuntimeException {
    final String sqlState;

    SettingException(final String message, final String sqlState) {
      super(message);
      this.sqlState = sqlState;
    }
  }

  // What the startup packet named: the value SET ... TO DEFAULT and RESET restore, as in PostgreSQL.
  private final Map<String, String> resetValues    = new HashMap<>();
  // What the session-scoped SETs established, including those of the transaction still open.
  private       Map<String, String> sessionValues  = new HashMap<>();
  // sessionValues as they stood before the first session-scoped SET of the open transaction; null when there was none.
  private       Map<String, String> rollbackValues = null;
  // SET LOCAL values of the open transaction; a null value is a SET LOCAL ... TO DEFAULT.
  private final Map<String, String> localValues    = new HashMap<>();
  private       boolean             superuser      = false;
  private       String              sessionUser    = "";
  // The isolation level the open transaction runs at (the default one when none is open), and the default level of
  // every new transaction. Read at SET time, because a database's default level can change while connected.
  private       Supplier<Database.TRANSACTION_ISOLATION_LEVEL> currentIsolation = () -> Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  private       Supplier<Database.TRANSACTION_ISOLATION_LEVEL> defaultIsolation = () -> Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  // Bumped on every change, so reportChanges() costs one int comparison on the statements that change nothing.
  private       int                 version        = 0;
  private       int                 reportedVersion = -1;
  private final String[]            reportedValues = new String[REPORTED_PARAMETERS.length];

  /**
   * Applies a session-scoped {@code SET <name> = <value>}. A null value - the unquoted {@code DEFAULT} keyword, as
   * {@code PostgresNetworkExecutor.parseSetCommand()} returns it - restores the reset value.
   *
   * @throws SettingException if PostgreSQL would refuse the same {@code SET}; nothing changes then
   */
  void set(final String name, final String value) {
    set(name, value, false);
  }

  /**
   * Applies a parsed {@code SET}/{@code RESET}.
   *
   * @throws SettingException if PostgreSQL would refuse the same command; nothing changes then
   */
  void apply(final Assignment assignment) {
    if (assignment == Assignment.NO_OP)
      return;
    if (assignment.name() == null)
      resetAll();
    else
      set(assignment.name(), assignment.value(), assignment.local());
  }

  /**
   * Applies a {@code SET}, session-scoped or, with {@code local}, for the rest of the open transaction only.
   *
   * @throws SettingException if PostgreSQL would refuse the same {@code SET}; nothing changes then
   */
  void set(final String name, final String value, final boolean local) {
    final String key = name.toLowerCase(Locale.ENGLISH);
    if (!isStored(key, value))
      return;
    final String normalized = normalize(key, value);
    if (local)
      localValues.put(key, normalized);
    else {
      saveForRollback();
      if (normalized == null)
        sessionValues.remove(key);
      else
        sessionValues.put(key, normalized);
      // A session-scoped SET after a SET LOCAL of the same parameter supersedes it, as in PostgreSQL
      localValues.remove(key);
    }
    ++version;
  }

  /**
   * {@code RESET ALL}: every parameter goes back to its reset value, the {@code SET LOCAL} ones included.
   */
  void resetAll() {
    saveForRollback();
    sessionValues.clear();
    localValues.clear();
    ++version;
  }

  /**
   * Records a parameter the client sent in its startup packet, which is also what {@code RESET} restores it to. Unlike
   * {@link #set}, a value that would be refused is logged and ignored: failing the whole connection for a startup
   * preference would be harsher than PostgreSQL, which reports a bad one as a warning for most parameters.
   */
  void setFromStartup(final String name, final String value) {
    final String key = name.toLowerCase(Locale.ENGLISH);
    try {
      if (!isStored(key, value))
        return;
      final String normalized = normalize(key, value);
      if (normalized != null)
        resetValues.put(key, normalized);
      ++version;
    } catch (final SettingException e) {
      LogManager.instance().log(this, Level.INFO, "Ignoring startup parameter %s='%s': %s", name, value, e.getMessage());
    }
  }

  /**
   * Whether the authenticated user is this server's superuser, which {@code is_superuser} answers.
   */
  void setSuperuser(final boolean superuser) {
    if (this.superuser != superuser) {
      this.superuser = superuser;
      ++version;
    }
  }

  /**
   * The user the connection authenticated as, which {@code session_authorization} answers.
   */
  void setSessionUser(final String sessionUser) {
    this.sessionUser = sessionUser != null ? sessionUser : "";
  }

  /**
   * Where the isolation levels a {@code SET TRANSACTION} / {@code SET SESSION CHARACTERISTICS} is checked against come
   * from: the level of the open transaction (the default one when none is open) and the default level.
   */
  void setIsolationLevels(final Supplier<Database.TRANSACTION_ISOLATION_LEVEL> currentIsolation,
      final Supplier<Database.TRANSACTION_ISOLATION_LEVEL> defaultIsolation) {
    this.currentIsolation = currentIsolation;
    this.defaultIsolation = defaultIsolation;
  }

  /**
   * The transaction the settings changes were made in committed: its session-scoped {@code SET}s stay, its
   * {@code SET LOCAL}s end. Called at the end of every transaction, so it returns at once when nothing was set.
   */
  void commit() {
    rollbackValues = null;
    if (!localValues.isEmpty()) {
      localValues.clear();
      ++version;
    }
  }

  /**
   * The transaction the settings changes were made in rolled back: its session-scoped {@code SET}s are undone and its
   * {@code SET LOCAL}s end.
   */
  void rollback() {
    if (rollbackValues != null) {
      sessionValues = rollbackValues;
      rollbackValues = null;
      ++version;
    }
    if (!localValues.isEmpty()) {
      localValues.clear();
      ++version;
    }
  }

  /**
   * The value {@code SHOW <name>} answers: what this connection set, else what its startup packet named, else the
   * server default, else an empty string.
   */
  String show(final String name) {
    final String key = name.toLowerCase(Locale.ENGLISH);
    return switch (key) {
      // Server facts: nothing a client sets changes them.
      case "server_version" -> PostgresNetworkExecutor.PG_SERVER_VERSION;
      case "server_encoding", "client_encoding" -> "UTF8";
      case "integer_datetimes", "standard_conforming_strings" -> "on";
      case "is_superuser" -> superuser ? "on" : "off";
      case DATESTYLE -> "ISO, " + currentDateOrder();
      case "role" -> "none";
      case "session_authorization" -> sessionUser;
      case "transaction_isolation" -> isolationName(currentIsolation.get());
      case "default_transaction_isolation" -> isolationName(defaultIsolation.get());
      case "transaction_read_only", "default_transaction_read_only" -> "off";
      default -> {
        final String value = current(key);
        if (value != null)
          yield value;
        yield switch (key) {
          case "timezone" -> "UTC";
          case "intervalstyle" -> "postgres";
          default -> "";
        };
      }
    };
  }

  /**
   * Hands {@code sink} every {@link #REPORTED_PARAMETERS reported parameter} whose value changed since the last call -
   * all of them on the first - with the value {@link #show} answers for it. PostgreSQL reports a change once, when the
   * statement that made it is over, however many times the value moved in between; a value that moved and came back
   * is not reported at all.
   */
  void reportChanges(final BiConsumer<String, String> sink) {
    if (version == reportedVersion)
      return;
    reportedVersion = version;
    for (int i = 0; i < REPORTED_PARAMETERS.length; i++) {
      final String value = show(REPORTED_PARAMETERS[i]);
      if (!value.equals(reportedValues[i])) {
        reportedValues[i] = value;
        sink.accept(REPORTED_PARAMETERS[i], value);
      }
    }
  }

  /**
   * False for the parameters a {@code SET} is accepted for but that are never stored, since {@code SHOW} answers what
   * the server really does; throws for the read-only ones, and for a value the server cannot honour: a {@code SET}
   * answered with success must have done what it asked (issue #8392). A null {@code value} is the reset value, which
   * is what these parameters already hold.
   */
  private boolean isStored(final String key, final String value) {
    return switch (key) {
      case "server_version", "server_encoding", "integer_datetimes", "is_superuser" ->
          throw new SettingException("parameter \"" + key + "\" cannot be changed", "55P02"); // cant_change_runtime_param
      // Accepted, as drivers send them routinely, but not stored: show() answers what the server really does.
      case "client_encoding", "standard_conforming_strings" -> false;
      case "role" -> {
        if (value != null && !"none".equalsIgnoreCase(value.trim()))
          throw new SettingException("SET ROLE is not supported by this server: the session keeps the privileges of the user it "
              + "authenticated as (\"" + sessionUser + "\")", SQLSTATE_FEATURE_NOT_SUPPORTED);
        yield false;
      }
      case "session_authorization" -> {
        if (value != null)
          throw new SettingException("SET SESSION AUTHORIZATION is not supported by this server: the session keeps the privileges "
              + "of the user it authenticated as (\"" + sessionUser + "\")", SQLSTATE_FEATURE_NOT_SUPPORTED);
        yield false;
      }
      case "transaction_isolation" -> {
        checkIsolation(key, value, currentIsolation.get());
        yield false;
      }
      case "default_transaction_isolation" -> {
        checkIsolation(key, value, defaultIsolation.get());
        yield false;
      }
      case "transaction_read_only", "default_transaction_read_only" -> {
        if (value != null && parseBoolean(key, value))
          throw new SettingException(READ_ONLY_NOT_SUPPORTED, SQLSTATE_FEATURE_NOT_SUPPORTED);
        yield false;
      }
      default -> true;
    };
  }

  /**
   * Accepts an isolation level only when it is the one the transactions really run at. {@code read uncommitted} is
   * {@code read committed}, as in PostgreSQL, whose READ UNCOMMITTED behaves as READ COMMITTED.
   */
  private static void checkIsolation(final String key, final String value, final Database.TRANSACTION_ISOLATION_LEVEL actual) {
    if (value == null)
      return;
    final String level = value.trim().replaceAll("\\s+", " ").toLowerCase(Locale.ENGLISH);
    final String effective = switch (level) {
      case "read uncommitted", "read committed" -> "read committed";
      case "repeatable read", "serializable" -> level;
      default -> throw new SettingException("invalid value for parameter \"" + key + "\": \"" + value + "\"", SQLSTATE_INVALID_VALUE);
    };
    final String actualName = isolationName(actual);
    if (!effective.equals(actualName))
      throw new SettingException(
          "transaction isolation level \"" + level + "\" is not supported here: transactions run at \"" + actualName + "\"",
          SQLSTATE_FEATURE_NOT_SUPPORTED);
  }

  /**
   * An isolation level spelled as PostgreSQL reports it, e.g. {@code read committed}.
   */
  static String isolationName(final Database.TRANSACTION_ISOLATION_LEVEL level) {
    return level.name().replace('_', ' ').toLowerCase(Locale.ENGLISH);
  }

  /**
   * A PostgreSQL boolean parameter value: {@code on}/{@code off}, {@code true}/{@code false}, {@code yes}/{@code no},
   * {@code 1}/{@code 0}, or an unambiguous prefix of one of the words.
   */
  private static boolean parseBoolean(final String key, final String value) {
    final String v = value.trim().toLowerCase(Locale.ENGLISH);
    if (v.equals("1") || v.equals("on") || (!v.isEmpty() && ("true".startsWith(v) || "yes".startsWith(v))))
      return true;
    if (v.equals("0") || v.equals("of") || v.equals("off") || (!v.isEmpty() && ("false".startsWith(v) || "no".startsWith(v))))
      return false;
    throw new SettingException("parameter \"" + key + "\" requires a Boolean value", SQLSTATE_INVALID_VALUE);
  }

  /**
   * The form a value is stored in, null for the reset value. {@code DateStyle} is stored as its field order alone.
   */
  private String normalize(final String key, final String value) {
    if (value == null)
      return null;
    return switch (key) {
      case DATESTYLE -> parseDateOrder(value);
      case "intervalstyle" -> parseIntervalStyle(value);
      default -> value;
    };
  }

  private String current(final String key) {
    if (!localValues.isEmpty() && localValues.containsKey(key)) {
      final String local = localValues.get(key);
      return local != null ? local : resetValues.get(key);
    }
    final String value = sessionValues.get(key);
    return value != null ? value : resetValues.get(key);
  }

  private String currentDateOrder() {
    final String order = current(DATESTYLE);
    return order != null ? order : DEFAULT_DATE_ORDER;
  }

  private void saveForRollback() {
    if (rollbackValues == null)
      rollbackValues = new HashMap<>(sessionValues);
  }

  /**
   * The field order a {@code DateStyle} value selects - the current one when the value names only an output style, as
   * in PostgreSQL. An output style other than {@code ISO} is accepted but not applied, since dates only ever travel as
   * ISO on this server; {@code German} still implies the {@code DMY} order unless the value names one, as in PostgreSQL.
   */
  private String parseDateOrder(final String value) {
    String order = null;
    boolean german = false;
    for (final String token : DATESTYLE_SEPARATOR.split(value.trim())) {
      final String style = token.toUpperCase(Locale.ENGLISH);
      switch (style) {
      case "ISO" -> {
      }
      case "SQL", "POSTGRES", "GERMAN" -> {
        german |= "GERMAN".equals(style);
        LogManager.instance().log(this, Level.INFO, "DateStyle output '%s' not supported, dates are sent as ISO", token);
      }
      case "MDY", "US", "NONEURO", "NONEUROPEAN" -> order = "MDY";
      case "DMY", "EURO", "EUROPEAN" -> order = "DMY";
      case "YMD" -> order = "YMD";
      default -> throw new SettingException("invalid value for parameter \"DateStyle\": \"" + value + "\"", "22023"); // invalid_parameter_value
      }
    }
    if (order != null)
      return order;
    return german ? "DMY" : currentDateOrder();
  }

  /**
   * An {@code IntervalStyle} value, lower-cased as PostgreSQL reports it; refused with {@code 22023} unless it is one of
   * the four PostgreSQL knows.
   */
  private static String parseIntervalStyle(final String value) {
    final String style = value.trim().toLowerCase(Locale.ENGLISH);
    return switch (style) {
      case "postgres", "postgres_verbose", "sql_standard", "iso_8601" -> style;
      default -> throw new SettingException("invalid value for parameter \"IntervalStyle\": \"" + value + "\"", "22023");
    };
  }
}
