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
package com.arcadedb.bolt;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.opencypher.procedures.db.DbLabels;
import com.arcadedb.query.opencypher.procedures.db.DbPropertyKeys;
import com.arcadedb.query.opencypher.procedures.db.DbRelationshipTypes;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Serves the three schema-introspection procedures Neo4j clients call over Bolt -
 * {@code db.labels()}, {@code db.relationshipTypes()} and {@code db.propertyKeys()} - out of
 * {@link CypherProcedureRegistry}, the same registry entries the native Cypher {@code CALL} path executes.
 * <p>
 * The Bolt executor intercepts these calls instead of running them through the query engine, because the
 * Neo4j tooling that sends them also sends a combined {@code UNION} form the engine does not parse. The
 * interception used to carry its own copy of what each procedure returns, which drifted from the registry
 * versions (issue #6151): relationship types were not filtered for Cypher's composite {@code A~B} label
 * types, and property keys came back sorted rather than in schema order. Everything here now asks the
 * registry, so there is one implementation of each procedure and one place to fix it.
 * </p>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
final class BoltSystemProcedures {
  private static final Object[] NO_ARGS       = new Object[0];
  private static final String   CALL_PREFIX   = "call ";
  /** Real probes are tiny; a longer statement is never matched, which also bounds the regex work on client input. */
  private static final int      MAX_PROBE_LENGTH = 4096;
  // One alternation, so each region is taken by whichever opener comes first: an apostrophe inside a block comment
  // cannot pair with a quote outside it, nor a slash-star inside a string open a comment
  private static final Pattern  QUOTED_OR_COMMENT = Pattern.compile(
      "'[^'\\\\]*+(?:\\\\.[^'\\\\]*+)*+'|\"[^\"\\\\]*+(?:\\\\.[^\"\\\\]*+)*+\"|`[^`]*+`|/\\*.*?\\*/");
  private static final Pattern  UNION         = Pattern.compile(" union (?:all )?");
  private static final Pattern  WHITESPACE    = Pattern.compile("\\s+");
  private static final Pattern  FOREIGN_CLAUSE = Pattern.compile(
      "(?<![\\w$.])(?:create|merge|set|delete|detach|remove|foreach|call|load|match|optional|union|use|finish|insert|drop|alter|grant|deny|revoke|start|stop|terminate|enable|rename)\\b");
  private static final String   LABELS        = DbLabels.NAME.toLowerCase(Locale.ROOT);
  private static final String   RELATIONSHIPS = DbRelationshipTypes.NAME.toLowerCase(Locale.ROOT);
  private static final String   PROPERTY_KEYS = DbPropertyKeys.NAME.toLowerCase(Locale.ROOT);
  // The field each schema procedure yields, lower-case as the normalized query is. A single call is served only
  // when the answer cannot differ from the engine's: no alias, and no projection of anything but that field.
  private static final Map<String, Pattern> SINGLE_TAIL   = Map.of(
      LABELS, singleTail("label"), RELATIONSHIPS, singleTail("relationshiptype"), PROPERTY_KEYS, singleTail("propertykey"));
  // The Desktop form: each UNION segment collects its procedure into the one `result` column, optionally sliced
  private static final Map<String, Pattern> COMBINED_TAIL = Map.of(
      LABELS, combinedTail("label"), RELATIONSHIPS, combinedTail("relationshiptype"), PROPERTY_KEYS, combinedTail("propertykey"));

  /**
   * The field names and the rows a served query answers with, in the shape the Bolt executor streams them.
   *
   * @param fields the record field names, i.e. the procedure's yield fields
   * @param rows   one entry per record, each holding one value per field
   */
  record Served(List<String> fields, List<List<Object>> rows) {
  }

  private BoltSystemProcedures() {
    // Utility class - prevent instantiation
  }

  /**
   * Normalizes a query for the Bolt executor's system-query matching: trimmed, lowercased and with every
   * whitespace run collapsed to a single space.
   * <p>
   * The lowercasing is {@link Locale#ROOT}-bound on purpose: under a Turkish default locale {@code toLowerCase()}
   * maps {@code I} to {@code ı}, so an upper-case {@code CALL DB.RELATIONSHIPTYPES()} would stop matching.
   * </p>
   *
   * @param query the raw query text
   *
   * @return the normalized form used by every anchored match
   */
  static String normalize(final String query) {
    return WHITESPACE.matcher(stripLeadingComments(query.trim()).toLowerCase(Locale.ROOT)).replaceAll(" ");
  }

  /**
   * Drops the {@code //} and block comments a driver or tool may put in front of a statement, so that the anchored
   * matching sees the statement itself. Done on the raw text, because normalizing collapses the newline that ends a
   * line comment.
   */
  private static String stripLeadingComments(final String trimmed) {
    String text = trimmed;
    while (text.startsWith("//") || text.startsWith("/*")) {
      final int end;
      if (text.startsWith("//")) {
        final int newline = text.indexOf('\n');
        end = newline < 0 ? text.length() : newline + 1;
      } else {
        final int close = text.indexOf("*/");
        end = close < 0 ? text.length() : close + 2;
      }
      text = text.substring(end).trim();
    }
    return text;
  }

  /**
   * Answers whether the normalized query mentions any of the three schema procedures.
   *
   * @param normalized a query normalized by {@link #normalize(String)}
   *
   * @return true if the Bolt executor should try to serve it here
   */
  static boolean isSchemaProcedureQuery(final String normalized) {
    // Only a statement that IS the introspection query - a single call, or the combined UNION form Neo4j Desktop
    // sends - is answered here. A name found deeper in the text (a CALL subquery branch, a string literal) or a call
    // followed by further clauses belongs to a larger statement the engine has to run (issue #8908).
    return schemaCallsOf(normalized) != null;
  }

  /**
   * Answers whether the normalized query is a call to the named system procedure and nothing else: the name must end
   * at a token boundary, and what follows may only be an empty argument list, an optional {@code YIELD} of plain
   * identifiers, an optional {@code RETURN} of plain identifiers or {@code collect(identifier)}, and an optional
   * semicolon. An allow-list on purpose: any other clause after the call (a {@code CREATE}, a {@code MATCH}, a
   * {@code SET}...) makes it a larger statement, and answering it here would silently drop the rest, writes included.
   *
   * @param normalized    a query normalized by {@link #normalize(String)}
   * @param procedureName the lower-case procedure name
   *
   * @return true if the statement is just {@code CALL <procedureName>} with an optional plain YIELD/RETURN
   */
  static boolean isStandaloneCall(final String normalized, final String procedureName) {
    final Pattern tail = SINGLE_TAIL.get(procedureName);
    return tail != null && matchesSegment(normalized, procedureName, tail) != null;
  }

  private static Pattern singleTail(final String field) {
    return Pattern.compile(" ?(?:\\( ?\\))?(?: yield (?:\\*|" + field + "))?(?: return " + field + ")? ?;?");
  }

  private static Pattern combinedTail(final String field) {
    return Pattern.compile(" ?(?:\\( ?\\))?(?: yield " + field + ")? return collect\\(" + field
        + "\\)(?:\\[\\.\\.(\\d{1,9})\\])? as result ?;?");
  }

  /** @return the matcher when the segment is exactly {@code CALL <name>} followed by the tail, else null */
  private static Matcher matchesSegment(final String segment, final String procedureName, final Pattern tail) {
    final int end = endOfCallName(segment, procedureName);
    if (end < 0)
      return null;
    final Matcher matcher = tail.matcher(segment).region(end, segment.length());
    return matcher.matches() ? matcher : null;
  }

  /**
   * @return the offset just past {@code CALL <procedureName>} when the statement opens with it, else -1
   */
  private static int endOfCallName(final String normalized, final String procedureName) {
    if (normalized.length() > MAX_PROBE_LENGTH || !normalized.startsWith(CALL_PREFIX)
        || !normalized.regionMatches(CALL_PREFIX.length(), procedureName, 0, procedureName.length()))
      return -1;
    return CALL_PREFIX.length() + procedureName.length();
  }

  /**
   * Answers whether the normalized query opens with a call to the named Bolt-only system procedure ({@code dbms.*},
   * {@code db.ping}). Unlike the schema procedures these exist nowhere but in the Bolt interception, so a statement
   * declined here would fail in the engine as an unknown procedure; what follows the call is therefore left to the
   * tail handling (YIELD / WHERE / UNWIND, as Neo4j Browser sends it). Only the anchoring and the token boundary are
   * required (plus no write or further-reading clause in the tail), which is what keeps a mere mention of the name
   * elsewhere in a larger statement out (issue #8908). The tail check is a deny-list on a family with no engine
   * fallback: quoted literals are blanked and a keyword after {@code $} or {@code .} is not a clause, so parameters
   * and property names do not trip it. An {@code EXPLAIN} or
   * {@code PROFILE} prefix, or a comment between {@code CALL} and the name, reaches the engine.
   */
  static boolean isSystemCall(final String normalized, final String procedureName) {
    final int end = endOfCallName(normalized, procedureName);
    if (end < 0)
      return false;
    if (end != normalized.length() && normalized.charAt(end) != '(' && normalized.charAt(end) != ' '
        && normalized.charAt(end) != ';')
      return false;
    // The tail stays open (YIELD / WHERE / UNWIND / RETURN) but never a clause that writes or calls on: serving
    // those here would drop them silently, whereas the engine refuses them loudly.
    // Quoted literals and block comments are blanked first: a keyword inside either is not a clause
    final String tail = QUOTED_OR_COMMENT.matcher(normalized.substring(end)).replaceAll(" ");
    // A line comment survives the blanking (normalize has already folded its newline away, so what it hides cannot be
    // told apart from the statement): such a tail is the engine's to refuse.
    return !tail.contains("//") && !FOREIGN_CLAUSE.matcher(tail).find();
  }

  /**
   * Splits the statement into its UNION segments and returns the schema procedure each one calls, or null when the
   * statement is not exactly one schema call or the three of them combined.
   */
  private record SchemaCall(String name, int limit) {
  }

  private static SchemaCall[] schemaCallsOf(final String normalized) {
    // Every Bolt statement passes through here: bail out before any allocation unless it opens with a call.
    if (normalized.length() > MAX_PROBE_LENGTH || !normalized.startsWith(CALL_PREFIX))
      return null;
    final String[] segments = normalized.indexOf(" union ") < 0 ? new String[] { normalized } : UNION.split(normalized, -1);
    if (segments.length != 1 && segments.length != 3)
      return null;

    final Map<String, Pattern> tails = segments.length == 1 ? SINGLE_TAIL : COMBINED_TAIL;
    final SchemaCall[] calls = new SchemaCall[segments.length];
    for (int i = 0; i < segments.length; ++i) {
      // The segment is anchored on its own: the prefix is re-checked after each UNION.
      calls[i] = schemaCallOf(segments[i], tails, LABELS);
      if (calls[i] == null)
        calls[i] = schemaCallOf(segments[i], tails, RELATIONSHIPS);
      if (calls[i] == null)
        calls[i] = schemaCallOf(segments[i], tails, PROPERTY_KEYS);
      if (calls[i] == null)
        return null;
    }
    if (calls.length == 3 && (calls[0].name().equals(calls[1].name()) || calls[0].name().equals(calls[2].name())
        || calls[1].name().equals(calls[2].name())))
      return null;
    return calls;
  }

  private static SchemaCall schemaCallOf(final String segment, final Map<String, Pattern> tails, final String name) {
    final Matcher matcher = matchesSegment(segment, name, tails.get(name));
    if (matcher == null)
      return null;
    // The slice group only exists in the combined tail
    final String slice = matcher.groupCount() > 0 ? matcher.group(1) : null;
    return new SchemaCall(name, slice == null ? Integer.MAX_VALUE : Integer.parseInt(slice));
  }

  /**
   * Serves a schema-procedure query out of the registry.
   * <p>
   * Recognizes the single-procedure calls and the combined form Neo4j Desktop sends, which unions the three
   * procedures and collects each into a list under a single {@code result} field.
   * </p>
   *
   * @param database   the database the connection is bound to, may be null when none is selected yet
   * @param normalized a query normalized by {@link #normalize(String)}
   *
   * @return the records to stream, or null when the query must be left to the Cypher engine - which happens
   * when the call carries arguments (the registry rejects those with the same error the native {@code CALL}
   * path reports), when the procedure is not registered, and when running it raises anything at all
   */
  static Served serveSchemaProcedure(final Database database, final String normalized) {
    final SchemaCall[] calls = schemaCallsOf(normalized);
    if (calls == null)
      return null;

    try {
      if (calls.length == 3)
        return serveCombined(database, normalized, calls);
      return serveOne(database, normalized, calls[0].name());
    } catch (final Exception e) {
      // The Bolt executor calls its system-query interception before the try/catch that classifies query
      // errors (CommandParsingException vs. retryable conflict vs. plain failure), so an exception escaping
      // here would reach the connection loop's catch-all unclassified. Running a registry procedure is a
      // wider surface than the direct schema iteration this branch used to do, so it declines instead: all
      // three procedures are read-only, which makes re-running the query through the engine free of side
      // effects and gets the client the engine's own, properly classified error.
      LogManager.instance().log(BoltSystemProcedures.class, Level.FINE,
          "Error serving schema procedure from the registry, leaving the query to the engine", e);
      return null;
    }
  }

  /**
   * Serves the combined query Neo4j Desktop sends, one row per procedure, each row holding the list of that
   * procedure's values.
   */
  private static Served serveCombined(final Database database, final String normalized, final SchemaCall[] calls) {
    final CypherProcedure[] procedures = new CypherProcedure[calls.length];
    for (int i = 0; i < calls.length; ++i) {
      procedures[i] = procedureFor(normalized, calls[i].name());
      if (procedures[i] == null)
        return null;
    }

    // One row per segment, in the order the client sent them, each list cut at its own slice
    final List<List<Object>> rows = new ArrayList<>(3);
    if (database != null)
      for (int i = 0; i < calls.length; ++i) {
        final List<Object> values = column(database, procedures[i]);
        rows.add(List.of(values.size() > calls[i].limit() ? new ArrayList<>(values.subList(0, calls[i].limit())) : values));
      }
    return new Served(List.of("result"), rows);
  }

  /**
   * Serves a single procedure call, one record per row the procedure yields.
   */
  private static Served serveOne(final Database database, final String normalized, final String procedureName) {
    final CypherProcedure procedure = procedureFor(normalized, procedureName);
    if (procedure == null)
      return null;

    final List<String> fields = procedure.getYieldFields();
    final List<List<Object>> rows = new ArrayList<>();
    if (database != null) {
      try (final Stream<Result> results = execute(database, procedure)) {
        results.forEach(result -> {
          final List<Object> row = new ArrayList<>(fields.size());
          for (final String field : fields)
            row.add(result.getProperty(field));
          rows.add(row);
        });
      }
    }
    return new Served(fields, rows);
  }

  /**
   * Looks the procedure up, refusing any call that carries arguments so that the query engine gets it.
   * <p>
   * The refusal is not a statement about these procedures' arity - it is that this path only has the query
   * TEXT and cannot evaluate an argument, so whatever a call passes has to be interpreted where arguments
   * are evaluated. Today all three declare zero arguments and the engine answers with the registry's arity
   * error; a procedure that later grew an optional argument would be executed there with it, which is the
   * same outcome, reached the same way.
   * </p>
   */
  private static CypherProcedure procedureFor(final String normalized, final String procedureName) {
    if (callHasArguments(normalized, procedureName))
      return null;
    return CypherProcedureRegistry.get(procedureName);
  }

  /**
   * Reads every value of a single-field procedure into one list, for the combined query's collected form.
   */
  private static List<Object> column(final Database database, final CypherProcedure procedure) {
    final String field = procedure.getYieldFields().get(0);
    final List<Object> values = new ArrayList<>();
    try (final Stream<Result> results = execute(database, procedure)) {
      results.forEach(result -> values.add(result.getProperty(field)));
    }
    return values;
  }

  private static Stream<Result> execute(final Database database, final CypherProcedure procedure) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return procedure.execute(NO_ARGS, null, context);
  }

  /**
   * Detects a call to the named procedure that passes at least one argument, e.g. {@code CALL db.labels('x')}.
   * <p>
   * The Bolt interception only has the query TEXT and cannot evaluate arguments, so a call that carries any is
   * handed to the Cypher engine rather than answered here.
   * </p>
   * <p>
   * This is not a Cypher tokenizer and must not be mistaken for one: it walks to the first {@code )} after the
   * name, so a nested paren or a stray {@code )} later in the query reads as an argument list. Every error it
   * can make is in the same direction - it declines a call it could have served, and the engine answers it -
   * which is why the crude scan is enough for the fixed procedure names this interception covers.
   * </p>
   *
   * @param normalized    a query normalized by {@link #normalize(String)}
   * @param procedureName the lower-case procedure name to look for
   *
   * @return true if the name is followed by a non-empty argument list
   */
  static boolean callHasArguments(final String normalized, final String procedureName) {
    final int length = normalized.length();
    for (int found = normalized.indexOf(procedureName); found >= 0;
        found = normalized.indexOf(procedureName, found + 1)) {
      int pos = found + procedureName.length();
      while (pos < length && normalized.charAt(pos) == ' ')
        ++pos;
      if (pos >= length || normalized.charAt(pos) != '(')
        continue;
      final int close = normalized.indexOf(')', pos);
      final String args = close < 0 ? normalized.substring(pos + 1) : normalized.substring(pos + 1, close);
      if (!args.isBlank())
        return true;
    }
    return false;
  }
}
