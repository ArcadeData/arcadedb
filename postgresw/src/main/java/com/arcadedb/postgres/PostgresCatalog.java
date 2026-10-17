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
import com.arcadedb.postgres.PostgresCatalogSyntax.FromEntry;
import com.arcadedb.postgres.PostgresCatalogSyntax.Join;
import com.arcadedb.postgres.PostgresCatalogSyntax.SetOperation;
import com.arcadedb.postgres.PostgresCatalogSyntax.Statement;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.arcadedb.postgres.PostgresCatalogRowSets.aggregateOf;
import static com.arcadedb.postgres.PostgresCatalogRowSets.combine;
import static com.arcadedb.postgres.PostgresCatalogRowSets.compareValues;
import static com.arcadedb.postgres.PostgresCatalogRowSets.distinctKey;
import static com.arcadedb.postgres.PostgresCatalogSyntax.MAX_NESTING;
import static com.arcadedb.postgres.PostgresCatalogSyntax.containsSetOperation;
import static com.arcadedb.postgres.PostgresCatalogSyntax.isName;
import static com.arcadedb.postgres.PostgresCatalogSyntax.parseFrom;
import static com.arcadedb.postgres.PostgresCatalogSyntax.plainSpellings;
import static com.arcadedb.postgres.PostgresCatalogSyntax.splitConjuncts;
import static com.arcadedb.postgres.PostgresCatalogSyntax.splitTopLevel;
import static com.arcadedb.postgres.PostgresCatalogSyntax.unwrapParentheses;

/**
 * Answers the {@code pg_catalog} and {@code information_schema} queries a PostgreSQL client sends to find out
 * what is in the database it just connected to (issue #6412).
 * <p>
 * These used to be answered by string equality against the exact spelling one tool sends, several of them
 * gated on {@code application_name} being literally {@code "dbvis"} - so the same question from DBeaver,
 * pgAdmin, DataGrip or psql fell through and got nothing, and the tool showed an empty database. The gating
 * was never about the tool: the queries are written by the JDBC driver those tools share, and what differs
 * between them is spelling, not meaning.
 * <p>
 * So recognition here is by <b>shape</b>, and the answer is computed rather than canned:
 * <ol>
 * <li>the FROM clause says which emulated relations the query is about, and therefore which family of rows
 * it is asking for - the schema list, the table list, the columns of a table;</li>
 * <li>the rows of that family are built from ArcadeDB's own schema;</li>
 * <li>the client's own projection is evaluated against each row, so the columns come back under the names
 * the client asked for, in the order it asked for them, with its own {@code CASE} expressions applied - which
 * is how one implementation answers {@code TABLE_TYPE} correctly for every driver that spells that CASE
 * differently.</li>
 * </ol>
 * A shape outside this - an unmodelled catalog relation, a projection with a construct the expression
 * evaluator does not implement - is <b>declined</b>, and the caller answers the empty result set that the
 * rest of {@code pg_catalog} has always answered. Declining is deliberate: a made-up row in a system catalog
 * is worse than no row, because the client believes it.
 * <p>
 * <b>The WHERE clause is the one place that is permissive</b>: a predicate the evaluator cannot read does not
 * get to remove rows. Everything in this catalog is the user's own types in the one database this connection
 * is bound to; there are no system schemas, no toast tables and no other databases' rows in it for an
 * unreadable predicate to be excluding. So the predicates that survive unread ({@code nspname !~ '^pg_temp_'}
 * and its kin) are ones whose whole purpose is to remove rows this catalog never produced, and reading them
 * as "keeps everything" is not a guess - it is what they mean here. The predicates that do the work a client
 * depends on, the name filters of {@code getTables} and {@code getColumns}, are read and applied.
 * <p>
 * <b>Joins.</b> An emulated row carries the columns of every relation it can be joined to, so the join conditions
 * do not pair rows up - but they are not skipped either (issue #10228): an inner join's condition filters like
 * the WHERE clause, which is where psqlodbc puts its table-name filter, and a LEFT JOIN whose condition does not
 * hold reads NULL from the joined relation. Set operations, aggregates and correlated sub-selects are evaluated
 * over the same rows, which is what psql's {@code \dt} and {@code \d} need.
 * <p>
 * <b>The schema model.</b> A PostgreSQL connection is bound to one database and sees the schemas inside it;
 * an ArcadeDB connection is bound to one database whose types are its tables. So the emulated schema list is
 * exactly one schema, named after the connected database - which is what {@code current_schema()} already
 * answered - and every type is a table in it. The previous answers disagreed with each other and with that
 * function: one arm reported every <i>type</i> as a schema, another reported every <i>database on the server</i>
 * as one, which also told any authenticated user the names of databases they have no access to.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresCatalog {
  /** The OID PostgreSQL gives the {@code public} schema, reused for the one schema this catalog emulates. */
  private static final int SCHEMA_OID = 2200;
  /** PostgreSQL's own first user OID: everything below it is a system object. */
  private static final int FIRST_USER_OID = 16384;
  private static final int OID_SPACE      = 1_000_000;
  /** The OID PostgreSQL gives the {@code heap} table access method, which every table here is. */
  private static final int HEAP_ACCESS_METHOD_OID = 2;
  /**
   * How much work one statement may do, in rows read plus sub-select tokens re-read. Nesting depth alone does not
   * bound it: a correlated sub-select runs once per row of the query around it, so {@code EXISTS} nested 32 deep
   * over three rows is 3^32 evaluations. psql's {@code \d} on a table of a hundred columns spends a few thousand
   * units; past the budget the statement is refused with {@link WorkLimitExceeded}.
   */
  private static final long WORK_BUDGET = 5_000_000L;
  /**
   * The OID of the bootstrap superuser in a stock PostgreSQL, used for every "owner" column - here and in
   * {@link PostgresTypeCatalog}'s {@code typowner}, so the two surfaces cannot drift apart on who owns what.
   */
  static final int OWNER_OID = 10;
  /**
   * The schema PostgreSQL's own built-in objects live in, which is what {@code pg_type.typnamespace} answers here
   * (issue #7224). It is deliberately NOT the one schema this catalog emulates for the user's own types: a
   * type row joined to pg_namespace must report the namespace its own typnamespace column names.
   * <p>
   * Every place this catalog names that schema uses this constant - the type rows, a column's {@code udt_schema},
   * and the collation rows - so the answer cannot come out one way on one surface and another way on the next,
   * which is the failure mode #7224 itself was.
   */
  private static final String PG_CATALOG_SCHEMA = "pg_catalog";

  /**
   * A catalog query that would spend more work than this server gives one statement - a client regular expression
   * that backtracks, sub-selects correlated deep enough to multiply. The client is told so, with PostgreSQL's
   * {@code program_limit_exceeded}, rather than handed an empty answer that would look like an empty catalog.
   */
  public static final class WorkLimitExceeded extends RuntimeException {
    public static final String SQLSTATE = "54000";

    WorkLimitExceeded() {
      super("The catalog query needs more work than one statement may spend: simplify it");
    }
  }

  /** A catalog query whose shape this class will not answer. The caller sends an empty result set. */
  public static final Answer DECLINED = new Answer(new LinkedHashMap<>(), null, false);

  /**
   * The emulated relations and the columns each one has. A column outside its relation's set makes the query
   * unanswerable; a column inside it that this catalog does not model answers NULL, which is what PostgreSQL
   * answers for most of them anyway.
   */
  private static final Map<String, Set<String>> RELATIONS = new HashMap<>();
  /** Which family a relation puts the query in. Relations that only decorate a row are absent. */
  private static final Map<String, Family>      FAMILIES  = new HashMap<>();

  /**
   * The answer to a catalog query: the columns to announce, in the order the client projected them, and the
   * rows to send. {@link #DECLINED} is the third possible outcome, distinct from both "not a catalog query"
   * (null) and "a catalog query with no matching rows" (an answer with an empty row list).
   */
  public static class Answer {
    public final LinkedHashMap<String, PostgresType> columns;
    public final List<Map<String, Object>>           rows;
    /**
     * Whether these rows are a closed set a filter selects a real subset of, rather than everything the
     * connection can see anyway. It travels with the answer so that a query reading it through a derived
     * table applies the same strictness to its outer WHERE as the inner one did - without it, wrapping
     * pg_type in a sub-select would reach the permissive default and reopen the "answers every type" hole
     * the strictness exists to close.
     */
    final boolean filtersExactly;

    private Answer(final LinkedHashMap<String, PostgresType> columns, final List<Map<String, Object>> rows,
        final boolean filtersExactly) {
      this.columns = columns;
      this.rows = rows;
      this.filtersExactly = filtersExactly;
    }
  }

  /** The families of rows a catalog query can be about. */
  private enum Family {
    SCHEMAS, TABLES, COLUMNS, DATABASES, ROLES, PRIVILEGES, CHARACTER_SETS, COLLATIONS, VIEWS, TYPES, ATTRDEFS
  }

  private static void relation(final String name, final Family family, final String... columns) {
    RELATIONS.put(name, new LinkedHashSet<>(List.of(columns)));
    if (family != null)
      FAMILIES.put(name, family);
  }

  static {
    relation("pg_namespace", Family.SCHEMAS, "oid", "nspname", "nspowner", "nspacl");
    relation("information_schema.schemata", Family.SCHEMAS, "catalog_name", "schema_name", "schema_owner",
        "default_character_set_catalog", "default_character_set_schema", "default_character_set_name", "sql_path");

    relation("pg_class", Family.TABLES, "oid", "relname", "relnamespace", "reltype", "reloftype", "relowner", "relam",
        "relfilenode", "reltablespace", "relpages", "reltuples", "relallvisible", "reltoastrelid", "relhasindex",
        "relisshared", "relpersistence", "relkind", "relnatts", "relchecks", "relhasrules", "relhastriggers",
        "relhassubclass", "relrowsecurity", "relforcerowsecurity", "relispopulated", "relreplident", "relispartition",
        "relacl", "reloptions", "relhasoids", "relrewrite", "relfrozenxid", "relminmxid", "relpartbound");
    relation("pg_tables", Family.TABLES, "schemaname", "tablename", "tableowner", "tablespace", "hasindexes",
        "hasrules", "hastriggers", "rowsecurity");
    relation("information_schema.tables", Family.TABLES, "table_catalog", "table_schema", "table_name", "table_type",
        "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema",
        "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action");

    relation("pg_attribute", Family.COLUMNS, "attrelid", "attname", "atttypid", "attstattarget", "attlen", "attnum",
        "attndims", "attcacheoff", "atttypmod", "attbyval", "attstorage", "attalign", "attnotnull", "atthasdef",
        "atthasmissing", "attidentity", "attgenerated", "attisdropped", "attislocal", "attinhcount", "attcollation",
        "attacl", "attoptions", "attfdwoptions", "attcompression");
    relation("information_schema.columns", Family.COLUMNS, "table_catalog", "table_schema", "table_name", "column_name",
        "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length",
        "character_octet_length", "numeric_precision", "numeric_precision_radix", "numeric_scale", "datetime_precision",
        "udt_catalog", "udt_schema", "udt_name", "is_identity", "identity_generation", "is_generated",
        "generation_expression", "is_updatable");

    relation("pg_database", Family.DATABASES, "oid", "datname", "datdba", "encoding", "datcollate", "datctype",
        "datistemplate", "datallowconn", "datconnlimit", "dattablespace", "datacl");

    relation("pg_roles", Family.ROLES, "oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb",
        "rolcanlogin", "rolreplication", "rolconnlimit", "rolpassword", "rolvaliduntil", "rolbypassrls", "rolconfig");
    relation("pg_user", Family.ROLES, "usename", "usesysid", "usecreatedb", "usesuper", "userepl", "usebypassrls",
        "passwd", "valuntil", "useconfig");

    relation("information_schema.usage_privileges", Family.PRIVILEGES, "grantor", "grantee", "object_catalog",
        "object_schema", "object_name", "object_type", "privilege_type", "is_grantable");
    relation("information_schema.character_sets", Family.CHARACTER_SETS, "character_set_catalog",
        "character_set_schema", "character_set_name", "character_repertoire", "form_of_use",
        "default_collate_catalog", "default_collate_schema", "default_collate_name");
    relation("information_schema.collations", Family.COLLATIONS, "collation_catalog", "collation_schema",
        "collation_name", "pad_attribute");
    relation("pg_views", Family.VIEWS, "schemaname", "viewname", "viewowner", "definition");
    relation("information_schema.views", Family.VIEWS, "table_catalog", "table_schema", "table_name", "view_definition",
        "check_option", "is_updatable", "is_insertable_into");

    // pg_type is both: on its own it is a query about the types this protocol can produce (issue #7178), and
    // joined to pg_attribute it decorates a column row with that column's type. Which of the two a query is
    // about is decided by rank(), and by whether the other relation CONSTRAINS the row set or only QUALIFIES it
    // (issue #7224): TYPES loses to pg_class and pg_attribute, which select the rows, and wins over pg_namespace,
    // which only names a schema for each type.
    relation("pg_type", Family.TYPES, "oid", "typname", "typnamespace", "typowner", "typlen", "typbyval", "typtype",
        "typcategory", "typispreferred", "typisdefined", "typdelim", "typrelid", "typelem", "typarray", "typinput",
        "typoutput", "typreceive", "typsend", "typmodin", "typmodout", "typanalyze", "typsubscript", "typalign",
        "typstorage", "typbasetype", "typtypmod", "typnotnull", "typndims", "typcollation", "typdefaultbin",
        "typdefault", "typacl");

    // Decorating relations: they never decide the family, they only contribute columns to a row. Every one of
    // them is something ArcadeDB has no equivalent of, so their columns are all NULL - which is exactly what a
    // LEFT JOIN against them yields in PostgreSQL when there is no comment, no default and no inheritance.
    relation("pg_description", null, "objoid", "classoid", "objsubid", "description");
    // A column default, which ArcadeDB does have: one row per column that declares one (issue #10228, psql's \d
    // reads it through a correlated sub-select). adbin holds the default's text and pg_get_expr() prints it.
    relation("pg_attrdef", Family.ATTRDEFS, "oid", "adrelid", "adnum", "adbin", "adsrc");
    // The table access method. Every type is a heap table here, which is what psql's \dt and \d join it for.
    relation("pg_am", null, "oid", "amname", "amhandler", "amtype");
    relation("pg_collation", null, "oid", "collname", "collnamespace", "collowner", "collprovider", "collcollate",
        "collctype");
  }

  private PostgresCatalog() {
  }

  /**
   * A cheap pre-filter, so that an ordinary query pays a substring scan rather than a tokenizer pass. Every
   * emulated relation is either under {@code information_schema} or named {@code pg_something}.
   */
  public static boolean mightBeCatalogQuery(final String query) {
    return containsIgnoreCase(query, "pg_") || containsIgnoreCase(query, "information_schema");
  }

  /**
   * Whether {@link #resolve} may answer the query: a catalog query, or a set operation, which ArcadeDB's SQL has
   * no syntax for (issue #10228) - psqlodbc's {@code SQLTables(SQL_ALL_TABLE_TYPES)} is a UNION over literals and
   * names no catalog relation at all. A set operation whose arms this catalog cannot evaluate is still handed on.
   */
  public static boolean mightBeAnswered(final String query) {
    return mightBeCatalogQuery(query) || mightBeSetOperation(query);
  }

  /**
   * A single pass for the three set-operation keywords as whole words, so an ordinary query pays one scan rather
   * than three, and a name that merely contains one - {@code exceptions}, {@code reunion} - is not tokenized for it.
   */
  private static boolean mightBeSetOperation(final String query) {
    final int length = query.length();
    for (int i = 0; i < length; i++) {
      final int word = switch (query.charAt(i)) {
        case 'u', 'U' -> query.regionMatches(true, i, "union", 0, 5) ? 5 : 0;
        case 'i', 'I' -> query.regionMatches(true, i, "intersect", 0, 9) ? 9 : 0;
        case 'e', 'E' -> query.regionMatches(true, i, "except", 0, 6) ? 6 : 0;
        default -> 0;
      };
      if (word > 0 && (i == 0 || !isIdentifierPart(query.charAt(i - 1))) && (i + word == length
          || !isIdentifierPart(query.charAt(i + word))))
        return true;
    }
    return false;
  }

  private static boolean startsAsQuery(final String query) {
    int i = 0;
    while (i < query.length() && Character.isWhitespace(query.charAt(i)))
      ++i;
    return i < query.length() && (query.charAt(i) == '(' || query.regionMatches(true, i, "select", 0, 6));
  }

  private static boolean isIdentifierPart(final char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }

  /**
   * The name a column is announced under. The rows of an answer are maps keyed by column, and a projection may
   * name two columns alike - {@code SELECT NULL, NULL, relkind} is two {@code ?column?}s, and psqlodbc reads the
   * third column of exactly that query (issue #10228). So the second one is keyed {@code "?column?\0" + n}: NUL
   * can be neither in an identifier nor in a RowDescription field name, which is a C string, so what precedes it
   * is always the name, and no client-written name can ever collide with such a key.
   */
  public static String announcedName(final String column) {
    final int marker = column.indexOf('\0');
    return marker < 0 ? column : column.substring(0, marker);
  }

  private static String uniqueColumnKey(final String name, final Set<String> used) {
    if (used.add(name))
      return name;
    for (int n = 2; ; n++) {
      final String key = name + '\0' + n;
      if (used.add(key))
        return key;
    }
  }

  static boolean containsIgnoreCase(final String text, final String search) {
    final int limit = text.length() - search.length();
    for (int i = 0; i <= limit; i++)
      if (text.regionMatches(true, i, search, 0, search.length()))
        return true;
    return false;
  }

  /**
   * Answers a catalog query.
   *
   * @param parameters the values bound to the query's {@code $n} placeholders, which the JDBC driver uses for
   *                   every name pattern it filters by, so the filter cannot be applied without them
   *
   * @return null when the query is not about an emulated catalog relation and the caller must go on with its
   * normal dispatch; {@link #DECLINED} when it is one but its shape cannot be answered honestly; otherwise
   * the columns and rows to send.
   */
  public static Answer resolve(final String query, final Database database, final String userName,
      final Object... parameters) {
    if (query == null || database == null)
      return null;

    final boolean catalogNamed = mightBeCatalogQuery(query);
    if (!catalogNamed && !mightBeSetOperation(query))
      return null;
    // A set operation naming no catalog relation is a SELECT, possibly parenthesised: an INSERT or a batch whose
    // data merely contains the word is not tokenized for it.
    if (!catalogNamed && !startsAsQuery(query))
      return null;

    List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize(query);
    if (tokens == null || tokens.isEmpty())
      return null;

    // A trailing statement terminator is not part of the statement.
    while (!tokens.isEmpty() && tokens.get(tokens.size() - 1).isSymbol(";"))
      tokens.remove(tokens.size() - 1);

    substituteParameters(tokens, parameters);
    tokens = plainSpellings(tokens);

    if (!catalogNamed) {
      // Reached only for being a set operation: anything this catalog cannot evaluate is the SQL engine's to
      // answer - with its own error, rather than with the empty result set a declined catalog query gets.
      if (!containsSetOperation(tokens))
        return null;
      final Answer answer = resolveBounded(tokens, new Context(database, userName), true);
      return answer == DECLINED ? null : answer;
    }

    return resolveBounded(tokens, new Context(database, userName), false);
  }

  /**
   * Resolves a whole statement. One that spends more work than it may (see {@link #WORK_BUDGET}) is refused with
   * {@link WorkLimitExceeded} rather than declined: an empty answer would read as "no such relation", which is not
   * what happened. A set operation naming no catalog relation is handed back to the SQL engine instead.
   */
  private static Answer resolveBounded(final List<PostgresCatalogToken> tokens, final Context context,
      final boolean nested) {
    try {
      return resolveStatement(tokens, context, null, nested);
    } catch (final PostgresCatalogExpression.BudgetExceeded e) {
      if (nested)
        return null;
      throw new WorkLimitExceeded();
    }
  }

  /**
   * The highest {@code $n} placeholder the query names, which is how many values must be bound to it.
   */
  static int countPlaceholders(final String query) {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize(query);
    if (tokens == null)
      return 0;

    int highest = 0;
    for (final PostgresCatalogToken token : tokens) {
      if (token.type != PostgresCatalogToken.Type.SYMBOL || token.text.length() < 2 || token.text.charAt(0) != '$')
        continue;
      try {
        highest = Math.max(highest, Integer.parseInt(token.text.substring(1)));
      } catch (final NumberFormatException e) {
        // not a placeholder
      }
    }
    return highest;
  }

  /**
   * Replaces every {@code $n} placeholder with the literal the client bound to it. The catalog is answered
   * with the parameter values in hand rather than at Parse time, because the driver's table and column lists
   * put their name filters in parameters: without this, {@code getColumns("Article")} would be answered with
   * every column of every type.
   */
  private static void substituteParameters(final List<PostgresCatalogToken> tokens, final Object[] parameters) {
    if (parameters == null || parameters.length == 0)
      return;

    for (int i = 0; i < tokens.size(); i++) {
      final PostgresCatalogToken token = tokens.get(i);
      if (token.type != PostgresCatalogToken.Type.SYMBOL || token.text.length() < 2 || token.text.charAt(0) != '$')
        continue;

      final int index;
      try {
        index = Integer.parseInt(token.text.substring(1));
      } catch (final NumberFormatException e) {
        continue;
      }

      if (index < 1 || index > parameters.length)
        continue;

      final PostgresCatalogToken literal = PostgresCatalogToken.literal(parameters[index - 1]);
      if (literal != null)
        tokens.set(i, literal);
    }
  }

  /**
   * @param outer  the resolver of the row a sub-select is correlated to, null for a statement that is not one
   * @param nested whether the statement is part of another one - a set-operation arm, a derived table, a
   *               sub-select - where a SELECT with no FROM is a row of literals this catalog evaluates. At the
   *               top level such a SELECT is not a catalog query, and is left to the SQL engine as it always was.
   */
  private static Answer resolveStatement(final List<PostgresCatalogToken> tokens, final Context context,
      final RowResolver outer, final boolean nested) {
    if (!context.enter())
      return DECLINED;
    try {
      return resolveNested(unwrapParentheses(tokens), context, outer, nested);
    } finally {
      context.exit();
    }
  }

  private static Answer resolveNested(final List<PostgresCatalogToken> tokens, final Context context,
      final RowResolver outer, final boolean nested) {
    final SetOperation setOperation = SetOperation.split(tokens);
    if (setOperation != null)
      return resolveSetOperation(setOperation, context, outer);

    if (tokens.isEmpty() || !tokens.get(0).isKeyword("SELECT"))
      // Not a SELECT at all: a WITH, an INSERT, something else entirely. If it names an emulated relation it
      // is still a catalog query, and answering it is not this class's job either way.
      return namesAnEmulatedRelation(tokens) ? DECLINED : null;

    final Statement statement = Statement.split(tokens);
    if (statement == null)
      return namesAnEmulatedRelation(tokens) ? DECLINED : null;

    if (statement.from == null) {
      if (!nested)
        return namesAnEmulatedRelation(tokens) ? DECLINED : null;
      // A row of literals - an arm of psqlodbc's UNION of relkinds, a sub-select's SELECT 1: one row, no relation.
      return project(statement, List.of(), List.of(new Row()), context, Map.of(), true, null, outer);
    }

    final List<FromEntry> from = parseFrom(statement.from);
    if (from == null || from.isEmpty())
      return namesAnEmulatedRelation(tokens) ? DECLINED : null;

    final Map<String, Collection<String>> columnsByRelation = new LinkedHashMap<>();
    final List<Row> rows;
    boolean toleratesUnreadableFilter = true;

    if (from.size() == 1 && from.get(0).derived != null) {
      // A derived table: the inner SELECT is the catalog query and this one is a filter over its output. The
      // JDBC driver's column list is written this way, so answering it means answering both levels.
      final Answer inner = resolveStatement(from.get(0).derived, context, outer, true);
      if (inner == null || inner.rows == null)
        return inner == null ? null : DECLINED;

      // The inner query decides how its own rows may be filtered, and the outer WHERE reads those same
      // rows: a sub-select over pg_type must not become the way to get the permissive filter back.
      toleratesUnreadableFilter = !inner.filtersExactly;

      final String name = from.get(0).alias == null ? "" : from.get(0).alias;
      rows = new ArrayList<>(inner.rows.size());
      for (final Map<String, Object> innerRow : inner.rows)
        rows.add(new Row().withAll(name, innerRow));
      columnsByRelation.put(name, inner.columns.keySet());
      from.set(0, new FromEntry(name, null, from.get(0).alias, null, Join.FIRST));
    } else {
      Family family = null;
      for (final FromEntry entry : from) {
        if (entry.derived != null)
          // A derived table joined to something else: more than one level of query planning, which is past
          // what a catalog emulation should be pretending to do.
          return DECLINED;

        if (!RELATIONS.containsKey(entry.relation)) {
          // A relation this catalog does not model. A type the user created is not one - even when its name
          // starts with pg_ - so the query belongs to the SQL engine; a genuine PostgreSQL catalog relation
          // is a catalog query that cannot be answered.
          if (context.database().getSchema().existsType(entry.relation)
              || (entry.writtenAs != null && context.database().getSchema().existsType(entry.writtenAs)))
            return null;
          return isCatalogRelationName(entry.relation) ? DECLINED : null;
        }

        // COLUMNS wins over TABLES wins over SCHEMAS: a query joining pg_class to pg_attribute is asking about
        // the columns, and the rows it wants are one per column rather than one per table.
        final Family entryFamily = FAMILIES.get(entry.relation);
        if (entryFamily != null)
          family = mostSpecific(family, entryFamily);

        columnsByRelation.put(entry.relation, RELATIONS.get(entry.relation));
      }

      if (family == null)
        return DECLINED;

      // The permissive-WHERE rule rests on every row being one of the user's own types in the one database
      // this connection is bound to, so a predicate that cannot be read can only be excluding rows this
      // catalog never produced. pg_type breaks that premise: its rows are a fixed set of built-in types, and
      // a filter over them selects a real subset. Answering "every type" to "the type named hstore" is the
      // fabricated system-catalog row this class exists to avoid - psycopg asks exactly that and fails with
      // "found 23 different types named hstore" - so here an unreadable predicate declines instead.
      toleratesUnreadableFilter = family != Family.TYPES;

      rows = context.rows(family);
    }

    context.charge(rows.size());

    final boolean[] secondAliases = secondAliasesRead(statement, from);
    if (secondAliases == null)
      return DECLINED;

    return project(statement, from, rows, context, columnsByRelation, toleratesUnreadableFilter, secondAliases, outer);
  }

  /**
   * Which FROM entries are a second alias the query reads a relation through, a shape this row model can only
   * partly represent: a {@link Row} carries one map per relation, so both aliases read the same map. The second
   * alias would answer with the first one's values, and a projection naming a column through both would read
   * one row where the client asked about two - which is precisely the kind of wrong answer that makes a strict
   * client (the Arrow ADBC driver among them) abort.
   * <p>
   * The one form of it that IS representable is a second alias on the right of a LEFT JOIN whose condition
   * does not hold for the row: it then reads NULL, whatever map it shares. psql's {@code \d} is that form -
   * {@code LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid)}, and a type has no TOAST table (issue #10228).
   * Such an entry is returned flagged, and {@link #project} declines the query if a row matches it after all.
   * Any other second alias declines the query here, and the caller sends the empty result set that any
   * unreadable catalog shape gets.
   * <p>
   * The self-join that matters in practice - {@code FROM pg_type t, pg_type e WHERE t.oid = N AND t.typelem =
   * e.oid}, where the projected columns describe a different row from the one the filter selects - is
   * answered by {@link PostgresTypeCatalog}, which runs first and recognises it as its own shape.
   * <p>
   * Only aliases the query actually <b>reads through</b> count, and an inner join's condition is a read: it
   * filters like the WHERE clause. An alias that appears solely in an outer join's condition does not: the JDBC
   * driver's {@code getColumns()} joins {@code pg_class} and {@code pg_namespace} a
   * second time purely to look up a comment it then never projects, and declining that would take out the
   * column list of every JDBC client.
   *
   * @return one flag per FROM entry, or null when the query reads a relation through a second alias that is
   * not the right side of a LEFT JOIN
   */
  private static boolean[] secondAliasesRead(final Statement statement, final List<FromEntry> from) {
    final boolean[] read = new boolean[from.size()];

    final List<List<PostgresCatalogToken>> clauses = new ArrayList<>(Arrays.asList(statement.projection,
        statement.where, statement.groupBy, statement.orderBy));
    // An inner join's condition is a filter (see project()), so a name read in it is read as in WHERE.
    for (final FromEntry entry : from)
      if (entry.on != null && entry.join != Join.LEFT && entry.join != Join.RIGHT && entry.join != Join.FULL)
        clauses.add(entry.on);

    for (final List<PostgresCatalogToken> clause : clauses) {
      if (clause == null)
        continue;

      for (int i = 0; i + 1 < clause.size(); i++) {
        if (!isName(clause.get(i)) || !clause.get(i + 1).isSymbol("."))
          continue;

        final int index = entryIndex(from, clause.get(i).text.toLowerCase(Locale.ENGLISH));
        if (index >= 0 && from.get(index).derived == null)
          read[index] = true;
      }
    }

    final boolean[] second = new boolean[from.size()];
    final Map<String, Integer> subjects = new HashMap<>();
    for (int i = 0; i < from.size(); i++) {
      if (!read[i] || subjects.putIfAbsent(from.get(i).relation, i) == null)
        continue;
      if (from.get(i).join != Join.LEFT || from.get(i).on == null)
        return null;
      second[i] = true;
    }
    return second;
  }

  private static Answer resolveSetOperation(final SetOperation operation, final Context context,
      final RowResolver outer) {
    final List<Answer> answers = new ArrayList<>(operation.arms.size());
    boolean declined = false;
    for (final List<PostgresCatalogToken> arm : operation.arms) {
      final Answer answer = resolveStatement(arm, context, outer, true);
      if (answer == null)
        // An arm reading the user's own types: the whole statement is the SQL engine's, not this catalog's.
        return null;
      declined |= answer.rows == null;
      answers.add(answer);
    }
    if (declined)
      return DECLINED;

    // The first arm names the columns, as in PostgreSQL; every arm must have as many.
    final List<String> keys = new ArrayList<>(answers.get(0).columns.keySet());
    final List<List<List<Object>>> arms = new ArrayList<>(answers.size());
    boolean filtersExactly = false;
    for (final Answer answer : answers) {
      if (answer.columns.size() != keys.size())
        return DECLINED;
      filtersExactly |= answer.filtersExactly;
      final List<List<Object>> values = new ArrayList<>(answer.rows.size());
      for (final Map<String, Object> row : answer.rows)
        values.add(new ArrayList<>(row.values()));
      arms.add(values);
    }

    final List<List<Object>> combined = combine(operation.operators, operation.all, arms);

    List<Map<String, Object>> rows = new ArrayList<>(combined.size());
    for (final List<Object> values : combined) {
      final Map<String, Object> row = new LinkedHashMap<>(keys.size());
      for (int k = 0; k < keys.size(); k++)
        row.put(keys.get(k), values.get(k));
      rows.add(row);
    }

    if (operation.orderBy != null)
      rows = sort(operation.orderBy, rows);
    if (rows == null)
      return DECLINED;
    if (operation.limit != null && rows.size() > operation.limit)
      rows = new ArrayList<>(rows.subList(0, operation.limit));

    return new Answer(columnsOf(keys, rows), rows, filtersExactly);
  }

  private static Family mostSpecific(final Family current, final Family candidate) {
    if (current == null)
      return candidate;
    if (current == candidate)
      return current;
    // The families that can legitimately appear together in one query are the three that describe the same
    // hierarchy; the most detailed one is what the query is about.
    if (rank(candidate) > rank(current))
      return candidate;
    return current;
  }

  /**
   * How specific a family is, when a query names relations belonging to more than one. The ladder ranks by what
   * the other relation DOES to the row set, which is the only thing that decides what the query is about:
   * <ul>
   * <li>a relation that CONSTRAINS it outranks the one it is joined to - pg_class or pg_attribute joined to
   * pg_type is a question about tables or columns that happens to name the type of each, and answering it with
   * one row per type instead of one row per column would change what every such query means;</li>
   * <li>a relation that only QUALIFIES it does not - pg_namespace joined to pg_type attaches a schema to each
   * type and selects nothing, so the query is still about types.</li>
   * </ul>
   * Issue #7224: SCHEMAS used to outrank TYPES, so {@code DatabaseMetaData.getTypeInfo()} - pg_type joined to
   * pg_namespace, which is pure qualification - was answered with the one schema row, whose pg_type columns
   * {@link Row#complete()} then filled with NULL. The client got one nameless type instead of the type list,
   * and no error. SCHEMAS therefore sits BELOW TYPES: pg_namespace is the one relation here that can never be
   * the subject of a query naming anything else, because this catalog models exactly one schema.
   * <p>
   * Issue #7868 finished that reading rather than stopping at TYPES. "pg_namespace can never be the subject of
   * a query naming anything else" is not a statement about TYPES, it is a statement about pg_namespace, so
   * SCHEMAS is now the BOTTOM of the ladder and every family that can be the subject of a query - ROLES,
   * DATABASES, PRIVILEGES, CHARACTER_SETS, COLLATIONS - sits above it instead of in an unranked bucket below.
   * {@code SELECT r.rolname FROM pg_roles r JOIN pg_namespace n ON n.nspowner = r.oid} was answered as a schema
   * query, one nameless role, which is the #7224 outcome with pg_roles where pg_type was.
   * <p>
   * The families above SCHEMAS tie with each other, and the tie is immaterial rather than merely tolerated:
   * {@link #buildRows} answers all six with the SAME row, {@link #singletonRow}'s, which carries every one of
   * their relations. A tie broken by FROM order used to change the answer - a ROLES row had no pg_database
   * columns - and now cannot.
   * <p>
   * TYPES still loses to TABLES, VIEWS and COLUMNS, which is the half of the old ranking that was right, and
   * beats the single-row families: pg_type joined to pg_roles reads an owner for each type, so it qualifies.
   * <p>
   * ATTRDEFS (issue #10228) is one row per column default, so it constrains a table query - one row per default
   * rather than one per table - and is constrained by a column query, whose LEFT JOIN to pg_attrdef reads NULL for
   * a column without a default.
   */
  private static int rank(final Family family) {
    return switch (family) {
      case SCHEMAS -> 0;
      case ROLES, DATABASES, PRIVILEGES, CHARACTER_SETS, COLLATIONS -> 1;
      case TYPES -> 2;
      case TABLES, VIEWS -> 3;
      case ATTRDEFS -> 4;
      case COLUMNS -> 5;
    };
  }

  private static boolean isCatalogRelationName(final String relation) {
    return relation.startsWith("pg_") || relation.startsWith("information_schema.");
  }

  /**
   * Whether the statement names one of the relations this catalog models, which is what makes a statement it
   * cannot otherwise read a catalog query rather than an ordinary one. The test is against the modelled names
   * exactly: a type the user called {@code pg_notes} is their table, and swallowing statements about it -
   * which a "starts with pg_" test would do, to the DDL that creates it as much as to the queries that read
   * it - would make part of the user's own schema unusable.
   */
  private static boolean namesAnEmulatedRelation(final List<PostgresCatalogToken> tokens) {
    for (int i = 0; i < tokens.size(); i++) {
      final PostgresCatalogToken token = tokens.get(i);
      if (token.type != PostgresCatalogToken.Type.IDENTIFIER)
        continue;

      String name = token.text.toLowerCase(Locale.ENGLISH);
      if (i + 2 < tokens.size() && tokens.get(i + 1).isSymbol(".")
          && tokens.get(i + 2).type == PostgresCatalogToken.Type.IDENTIFIER) {
        final String qualified = name + "." + tokens.get(i + 2).text.toLowerCase(Locale.ENGLISH);
        if (qualified.startsWith("pg_catalog."))
          name = qualified.substring("pg_catalog.".length());
        else
          name = qualified;
      }

      if (RELATIONS.containsKey(name))
        return true;
    }
    return false;
  }

  // ---------------------------------------------------------------- rows

  /**
   * What the emulated catalog is a view of: one database, seen by one authenticated user.
   * <p>
   * Also where the name/OID lookups a {@code regclass} cast needs are memoised. Both directions are
   * evaluated once per row - the WHERE clause runs against every row of the family - so a linear scan of
   * the schema inside them would make a table-schema query quadratic in the number of types. The maps are
   * built at most once per statement, and only when a query actually casts to an OID-alias type.
   */
  private static final class Context {
    private final Database database;
    private final String   userName;

    private Map<Integer, String>      namesByOid;
    private Map<String, DocumentType> typesByFoldedName;
    /**
     * The rows of each family, built at most once per statement. A correlated sub-select is resolved once per
     * row of the query around it, and building its rows means walking the schema: psql's {@code \d} asks for each
     * column's default this way, which would otherwise rebuild every column row once per column.
     */
    private final Map<Family, List<Row>> rowsByFamily = new EnumMap<>(Family.class);
    private int                          nesting;
    private long                         work;

    Context(final Database database, final String userName) {
      this.database = database;
      this.userName = userName;
    }

    List<Row> rows(final Family family) {
      List<Row> rows = rowsByFamily.get(family);
      if (rows == null) {
        rows = buildRows(family, this);
        rowsByFamily.put(family, rows);
      }
      return rows;
    }

    /** @return false when one more level of nesting would pass {@link #MAX_NESTING} */
    boolean enter() {
      if (nesting >= MAX_NESTING)
        return false;
      ++nesting;
      return true;
    }

    void exit() {
      --nesting;
    }

    /** @throws PostgresCatalogExpression.BudgetExceeded once this statement has spent its {@link #WORK_BUDGET} */
    void charge(final int units) {
      work += units;
      if (work > WORK_BUDGET)
        throw new PostgresCatalogExpression.BudgetExceeded();
    }

    Database database() {
      return database;
    }

    String userName() {
      return userName;
    }

    String schema() {
      return database.getName();
    }

    /**
     * The relation an OID names, for the {@code attrelid::regclass} direction of the cast. Built over the
     * types in name order rather than in schema iteration order: {@link #oidOf} derives an OID from a hash,
     * so two names can in principle land on the same one, and this is the first place such a collision
     * would be externally visible - one type's name printed for another type's row. Which of the two wins
     * has to at least not depend on the order a hash set happened to iterate in.
     */
    String relationNamed(final int oid) {
      if (namesByOid == null) {
        namesByOid = new HashMap<>();
        for (final DocumentType type : sortedTypes(this))
          namesByOid.putIfAbsent(oidOf(type.getName()), type.getName());
      }
      return namesByOid.get(oid);
    }

    /**
     * The type a relation name names, given the name with its quoting already taken off by
     * {@link RowResolver#unqualify} - so whether the client quoted it is not something this method can
     * still tell, and the order the two lookups run in is what stands in for it.
     * <p>
     * The exact match runs first. For a quoted name that is what PostgreSQL means, since a quoted
     * identifier is itself; for an unquoted one PostgreSQL would have folded it to lower case and found
     * nothing, and taking the exact match anyway is the deliberate divergence that keeps a case-sensitive
     * schema addressable - {@code SELECT * FROM Article} has to reach the type spelled that way.
     * <p>
     * Then the case-insensitive one, because a client that wrote an unquoted {@code article} still means
     * the type spelled {@code Article}. Two or more types differing only in case answer nothing: none of
     * them is the one meant, and picking one would be a guess.
     */
    DocumentType typeNamed(final String name) {
      if (name.isEmpty())
        return null;
      if (database.getSchema().existsType(name))
        return database.getSchema().getType(name);

      if (typesByFoldedName == null) {
        typesByFoldedName = new HashMap<>();
        for (final DocumentType type : sortedTypes(this)) {
          final String folded = type.getName().toLowerCase(Locale.ENGLISH);
          // A folded name stays in the map once it is struck out, mapped to null. containsKey is what tells
          // "not seen yet" from "seen and struck out", and removing the entry instead would not: a third
          // name folding the same way would find the key absent and install itself as the answer.
          if (typesByFoldedName.containsKey(folded))
            typesByFoldedName.put(folded, null);
          else
            typesByFoldedName.put(folded, type);
        }
      }

      // Absent and struck out both answer null here, and both mean the same thing: no type this name names.
      return typesByFoldedName.get(name.toLowerCase(Locale.ENGLISH));
    }
  }

  /** One emulated catalog row, holding the columns of every relation the query could have joined. */
  private static class Row {
    final Map<String, Map<String, Object>> relations = new HashMap<>();
    /** Whether a derived table's row carries two columns announced alike, see {@link #announcedName}. */
    boolean duplicateNames;

    Map<String, Object> of(final String relation) {
      return relations.computeIfAbsent(relation, name -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
    }

    Row with(final String relation, final Object... namesAndValues) {
      final Map<String, Object> columns = of(relation);
      for (int i = 0; i < namesAndValues.length; i += 2)
        columns.put((String) namesAndValues[i], namesAndValues[i + 1]);
      return this;
    }

    /** Adopts an already-projected row of a derived table as this row's only relation. */
    Row withAll(final String relation, final Map<String, Object> values) {
      of(relation).putAll(values);
      for (final String column : values.keySet())
        if (column.indexOf('\0') >= 0) {
          duplicateNames = true;
          break;
        }
      return this;
    }

    /**
     * Whether a name read from these columns is ambiguous: a derived table announced two columns under it, and
     * PostgreSQL refuses to guess which one a reference means.
     */
    boolean isAmbiguous(final Map<String, Object> columns, final String name) {
      return duplicateNames && columns.containsKey(name + "\0" + 2);
    }

    /**
     * Every column of every emulated relation that this row does not explicitly set answers NULL rather than
     * "no such column", so a query joining a relation whose content ArcadeDB has no equivalent of - comments,
     * defaults, collations - gets the empty answer PostgreSQL's own LEFT JOIN would give it.
     */
    Row complete() {
      for (final Map.Entry<String, Set<String>> relation : RELATIONS.entrySet()) {
        final Map<String, Object> columns = of(relation.getKey());
        for (final String column : relation.getValue())
          columns.putIfAbsent(column, null);
      }
      return this;
    }
  }

  private static List<Row> buildRows(final Family family, final Context context) {
    return switch (family) {
      // Six families, one row, the SAME row: this catalog models exactly one of each of these things, and
      // singletonRow() carries all six on it (issue #7868), so which of them a query naming two is decided to be
      // about cannot change the answer. Splitting them into six builders is what left them FROM-order dependent:
      // a ROLES row carried no pg_database, so `SELECT r.rolname, d.datname FROM pg_roles r, pg_database d` read
      // datname out of the null fill while the same query with the FROM order swapped answered it.
      case SCHEMAS, DATABASES, ROLES, PRIVILEGES, CHARACTER_SETS, COLLATIONS -> List.of(singletonRow(context).complete());
      case TABLES -> tableRows(context);
      case COLUMNS -> columnRows(context);
      case TYPES -> typeRows(context);
      case ATTRDEFS -> defaultRows(context);
      // ArcadeDB has no relation that a PostgreSQL client would render as a view.
      case VIEWS -> List.of();
    };
  }

  /**
   * One row per type this protocol can produce, which is the only set of types it can honestly describe. The
   * values come from {@link PostgresTypeCatalog}, so a client reading pg_type through the generic catalog is
   * told exactly what {@code SELECT ... FROM pg_type} tells it through the recogniser that runs first.
   * <p>
   * This is what makes the Apache Arrow ADBC driver able to connect (issue #7178). Its type-resolver
   * bootstrap - {@code SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM pg_catalog.pg_type
   * WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r' AND typreceive::TEXT != 'array_recv'} - is a
   * shape no regular expression should be asked to read, and the driver rejects the connection outright when
   * the answer has no columns rather than falling back to anything.
   * <p>
   * Issue #7224: each row carries the {@code pg_namespace} row of the schema the type lives in, because a type
   * query that names pg_namespace does so to qualify each type - {@code DatabaseMetaData.getTypeInfo()} joins
   * it and then filters on {@code n.nspname != 'pg_toast'}. Left to {@link Row#complete()}'s null fill that
   * predicate would read as UNKNOWN and, TYPES being the family that does not tolerate an unreadable filter,
   * the whole query would be declined. The schema is pg_catalog rather than the database's own, which is what
   * {@code pg_type.typnamespace} already says these types are in, so the join column and the joined row agree.
   */
  private static List<Row> typeRows(final Context context) {
    final List<PostgresType> types = PostgresTypeCatalog.types();
    final List<Row> rows = new ArrayList<>(types.size());

    for (final PostgresType type : types) {
      // Built on singletonRow() like every other row, so the owner a type query joins on typowner is on the row
      // (issue #7868), re-pointed at pg_catalog afterwards: that is the schema typnamespace says these types are
      // in, so the join column and the joined row agree.
      final Row row = describeType(singletonRow(context), type);
      rows.add(inSchema(row, row.of("pg_type").get("typnamespace"), PG_CATALOG_SCHEMA).complete());
    }

    return rows;
  }

  /**
   * Re-points the columns that say WHICH SCHEMA a row is about, on every relation that has any - not only
   * {@code pg_namespace}. {@link #singletonRow} sets them all to the database's own schema, which is right for
   * every row except a type row: the types this protocol produces live in {@code pg_catalog}, which is what
   * {@code pg_type.typnamespace} already says, so a type row has to move them all together or the relations it
   * carries contradict each other - pg_namespace answering {@code pg_catalog} while
   * {@code information_schema.schemata} and {@code usage_privileges} still named the user's schema (found in
   * review of issue #7868).
   * <p>
   * The CATALOG columns are deliberately not moved: in PostgreSQL a catalog is a database, not a schema, and
   * these rows are all in the one database this connection is on whichever schema they describe.
   */
  private static Row inSchema(final Row row, final Object oid, final String name) {
    return row//
        .with("pg_namespace", "oid", oid, "nspname", name, "nspowner", OWNER_OID)//
        .with("information_schema.schemata", "schema_name", name)//
        .with("information_schema.usage_privileges", "object_schema", name, "object_name", name);
  }

  /**
   * Fills a row's {@code pg_type} columns with the given type's, which is the one place that decides what
   * this catalog says a type is. Both callers need it to be the same answer: a client that enumerates
   * pg_type and one that reads a column's type off a {@code pg_attribute} join must not be told two
   * different things about the same OID.
   */
  private static Row describeType(final Row row, final PostgresType type) {
    final Map<String, Object> columns = row.of("pg_type");
    for (final String column : PostgresTypeCatalog.COLUMNS)
      columns.put(column, PostgresTypeCatalog.columnValue(type, column));
    return row;
  }

  /**
   * The row every other row in this catalog is built on: everything this catalog models exactly ONE of - the
   * schema, the role that owns it, the database, the one USAGE privilege on that schema, the one character set
   * and the one collation. They are carried TOGETHER, on every row, rather than one per family.
   * <p>
   * Issue #7868. A relation a query names purely to QUALIFY its rows has to be carried on each emitted row, or
   * the join reads {@link Row#complete()}'s null fill: {@code JOIN pg_roles r ON r.oid = c.relowner} - the shape
   * a GUI browser uses to list tables with their owner - answered NULL for every table, even though the catalog
   * returns the name from {@code pg_get_userbyid(c.relowner)} and stamps {@link #OWNER_OID} on the very column
   * being joined. The same held for {@code nspowner}, {@code typowner} and {@code datdba}.
   * <p>
   * Carrying ALL of them here rather than each family building its own row is what makes {@link #rank()}'s tie
   * among these six immaterial instead of merely undocumented. A row per family left them FROM-order dependent
   * pairwise - a ROLES row had no {@code pg_database} columns, so {@code SELECT r.rolname, d.datname FROM
   * pg_roles r, pg_database d} answered a NULL {@code datname} while the same query with the FROM order swapped
   * answered it - which is the #7224 defect one level up, inside the tied bucket (found in review).
   * <p>
   * It costs nothing: {@link Row#complete()} already materialises EVERY column of EVERY modelled relation on
   * every row it emits. The only thing this decides is whether those columns hold the catalog's answer or a NULL.
   * <p>
   * Only the connected user is described as a role: enumerating the server's accounts is not something an
   * emulated catalog should hand out, and no client needs it to browse the database it is connected to.
   */
  private static Row singletonRow(final Context context) {
    final String schema = context.schema();
    return new Row()//
        .with("pg_namespace", "oid", SCHEMA_OID, "nspname", schema, "nspowner", OWNER_OID)//
        .with("information_schema.schemata", "catalog_name", schema, "schema_name", schema, "schema_owner",
            context.userName())//
        .with("pg_roles", "oid", OWNER_OID, "rolname", context.userName(), "rolsuper", Boolean.FALSE, "rolinherit",
            Boolean.TRUE, "rolcreaterole", Boolean.FALSE, "rolcreatedb", Boolean.FALSE, "rolcanlogin", Boolean.TRUE,//
            "rolreplication", Boolean.FALSE, "rolconnlimit", -1, "rolbypassrls", Boolean.FALSE)//
        .with("pg_user", "usename", context.userName(), "usesysid", OWNER_OID, "usecreatedb", Boolean.FALSE,//
            "usesuper", Boolean.FALSE, "userepl", Boolean.FALSE, "usebypassrls", Boolean.FALSE)//
        .with("pg_database", "oid", FIRST_USER_OID, "datname", schema, "datdba", OWNER_OID,//
            // 6 is UTF8 in PostgreSQL's pg_encoding table, which is the only encoding this protocol speaks.
            "encoding", 6, "datcollate", "C", "datctype", "C", "datistemplate", Boolean.FALSE, "datallowconn",
            Boolean.TRUE, "datconnlimit", -1, "dattablespace", 0)//
        .with("information_schema.usage_privileges", "grantor", context.userName(), "grantee", context.userName(),//
            "object_catalog", schema, "object_schema", schema, "object_name", schema, "object_type", "SCHEMA",//
            "privilege_type", "USAGE", "is_grantable", "NO")//
        .with("information_schema.character_sets", "character_set_catalog", null, "character_set_schema", null,//
            "character_set_name", "UTF8", "character_repertoire", "UCS", "form_of_use", "UTF8",//
            "default_collate_catalog", schema, "default_collate_schema", PG_CATALOG_SCHEMA, "default_collate_name",
            "default")//
        .with("information_schema.collations", "collation_catalog", schema, "collation_schema", PG_CATALOG_SCHEMA,//
            "collation_name", "default", "pad_attribute", "NO PAD");
  }

  private static List<Row> tableRows(final Context context) {
    final List<DocumentType> types = sortedTypes(context);
    final List<Row> rows = new ArrayList<>(types.size());

    for (final DocumentType type : types)
      rows.add(tableRow(context, type).complete());

    return rows;
  }

  private static Row tableRow(final Context context, final DocumentType type) {
    final String schema = context.schema();
    final int oid = oidOf(type.getName());

    return singletonRow(context)//
        .with("pg_class", "oid", oid, "relname", type.getName(), "relnamespace", SCHEMA_OID, "relowner", OWNER_OID,//
            "relkind", "r", "relpersistence", "p", "relnatts", type.getPropertyNames().size(),//
            "relhasindex", !type.getAllIndexes(false).isEmpty(), "relhasrules", Boolean.FALSE,//
            "relhastriggers", Boolean.FALSE, "relhassubclass", !type.getSubTypes().isEmpty(),//
            "relispartition", Boolean.FALSE, "relisshared", Boolean.FALSE, "relispopulated", Boolean.TRUE,//
            "relrowsecurity", Boolean.FALSE, "relforcerowsecurity", Boolean.FALSE, "relchecks", 0,//
            // -1 is PostgreSQL's own "never analysed, count unknown", which is honest here: counting the
            // records of every type to answer a table list would turn a metadata query into a full scan.
            "reltuples", -1, "relpages", 0, "relam", HEAP_ACCESS_METHOD_OID, "reltype", 0, "relfilenode", oid,
            "reltablespace", 0, "reloftype", 0, "reltoastrelid", 0, "relreplident", "d", "relrewrite", 0)//
        .with("pg_am", "oid", HEAP_ACCESS_METHOD_OID, "amname", "heap", "amhandler", "heap_tableam_handler",
            "amtype", "t")//
        .with("pg_tables", "schemaname", schema, "tablename", type.getName(), "tableowner", context.userName(),//
            "hasindexes", !type.getAllIndexes(false).isEmpty(), "hasrules", Boolean.FALSE, "hastriggers", Boolean.FALSE,//
            "rowsecurity", Boolean.FALSE)//
        .with("information_schema.tables", "table_catalog", schema, "table_schema", schema, "table_name",
            type.getName(), "table_type", "BASE TABLE", "is_insertable_into", "YES", "is_typed", "NO");
  }

  private static List<Row> columnRows(final Context context) {
    final String schema = context.schema();
    final List<Row> rows = new ArrayList<>();

    for (final DocumentType type : sortedTypes(context)) {
      int ordinal = 0;
      for (final Property property : sortedProperties(type)) {
        ++ordinal;
        final PostgresType pgType = PostgresType.getTypeFromArcade(property.getType(), property.getOfType());
        final Object defaultValue = property.getDefaultValueDefinition();
        final boolean notNull = property.isNotNull() || property.isMandatory();

        final Row row = tableRow(context, type)//
            .with("pg_attribute", "attrelid", oidOf(type.getName()), "attname", property.getName(), "attnum", ordinal,//
                "atttypid", pgType.code, "attlen", pgType.size, "atttypmod", -1, "attnotnull", notNull,//
                "atthasdef", defaultValue != null, "attisdropped", Boolean.FALSE, "attislocal", Boolean.TRUE,//
                "attinhcount", 0, "attndims", pgType.isArrayType() ? 1 : 0, "attcollation", 0, "attstattarget", -1,//
                "attidentity", "", "attgenerated", "", "atthasmissing", Boolean.FALSE, "attbyval",
                pgType.size > 0 && pgType.size <= 8, "attalign", "i", "attcompression", "",//
                // A variable-length value is stored "extended" in PostgreSQL, a fixed-length one "plain".
                "attstorage", pgType.size > 0 ? "p" : "x")//
            .with("information_schema.columns", "table_catalog", schema, "table_schema", schema, "table_name",
                type.getName(), "column_name", property.getName(), "ordinal_position", ordinal,//
                "column_default", defaultValue == null ? null : defaultValue.toString(),//
                "is_nullable", notNull ? "NO" : "YES", "data_type", pgType.typeName, "udt_catalog", schema,//
                "udt_schema", PG_CATALOG_SCHEMA, "udt_name", pgType.typeName, "is_identity", "NO", "is_generated", "NEVER",//
                "is_updatable", "YES", "numeric_precision_radix", numericPrecisionRadix(pgType));

        if (defaultValue != null)
          // pg_attrdef's adbin is a parsed expression that pg_get_expr() prints; here it holds the text
          // pg_get_expr() is to print, the same text information_schema.columns reports.
          row.with("pg_attrdef", "adrelid", oidOf(type.getName()), "adnum", ordinal, "adbin", defaultValue.toString(),
              "adsrc", defaultValue.toString());

        // The type row a client joins pg_attribute to in order to name the column's type. It describes the
        // column's own type, which is the only reading of that join that makes sense.
        rows.add(describeType(row, pgType).complete());
      }
    }

    return rows;
  }

  /** One row per column that declares a default: pg_attrdef's rows, each carrying its column's (issue #10228). */
  private static List<Row> defaultRows(final Context context) {
    final List<Row> rows = new ArrayList<>();
    for (final Row row : context.rows(Family.COLUMNS))
      if (row.relations.get("pg_attrdef").get("adrelid") != null)
        rows.add(row);
    return rows;
  }

  /**
   * information_schema.columns.numeric_precision_radix: 2 for the binary/approximate numeric types PostgreSQL
   * itself reports it for, but 10 for NUMERIC specifically - it is the one numeric type PostgreSQL stores and
   * reports precision in base 10 rather than base 2 (issue #6447). DECIMAL used to map to DOUBLE, so the flat
   * "every native scalar type answers 2" below was accidentally correct for it; now that it maps to NUMERIC,
   * it needs its own case.
   */
  private static Integer numericPrecisionRadix(final PostgresType pgType) {
    if (pgType == PostgresType.NUMERIC)
      return 10;
    return pgType.isNativeScalarType() ? 2 : null;
  }

  private static List<DocumentType> sortedTypes(final Context context) {
    final List<DocumentType> types = new ArrayList<>(context.database().getSchema().getTypes());
    types.sort(Comparator.comparing(DocumentType::getName));
    return types;
  }

  /**
   * The properties of a type, super-type properties included and in a stable order, so that a column's
   * ordinal position does not depend on the order a hash set happens to iterate in.
   */
  private static List<Property> sortedProperties(final DocumentType type) {
    final List<Property> properties = new ArrayList<>();
    for (final Property property : type.getPolymorphicProperties())
      if (!property.isHidden())
        properties.add(property);
    properties.sort(Comparator.comparing(Property::getName));
    return properties;
  }

  /**
   * A stable OID for a type. PostgreSQL hands a client an OID and expects to be asked about it again - the
   * table list gives {@code pg_class.oid}, the column list asks for {@code attrelid = <that oid>} - so what
   * matters is that the same name yields the same number for the life of the schema, not which number.
   * Deriving it from the name rather than from a position keeps it stable when another type is created.
   */
  private static int oidOf(final String typeName) {
    return FIRST_USER_OID + Math.floorMod(typeName.hashCode(), OID_SPACE);
  }

  // ---------------------------------------------------------------- projection

  /** An emulated row together with which of the query's FROM entries an outer join left unmatched on it. */
  private record Joined(Row row, boolean[] nulled) {
  }

  private static Answer project(final Statement statement, final List<FromEntry> from, final List<Row> rows,
      final Context context, final Map<String, Collection<String>> columnsByRelation,
      final boolean toleratesUnreadableFilter, final boolean[] secondAliases, final RowResolver outer) {
    final Projection projection = parseProjection(statement.projection, from, columnsByRelation);
    if (projection == null)
      return DECLINED;

    // What a row has to satisfy: the WHERE clause and the ON condition of every inner join, which for an inner
    // join mean the same thing - psqlodbc puts its table-name filter in the ON clause, and skipping join
    // conditions answered SQLColumns("Person") with every column of every type (issue #10228). Each conjunct is
    // read on its own, so that one this evaluator cannot read does not take the readable ones down with it; see
    // the class comment on why an unreadable one does not get to remove rows.
    final List<PostgresCatalogExpression> filters = new ArrayList<>();
    if (!readConjuncts(statement.where, filters, toleratesUnreadableFilter))
      return DECLINED;

    // An outer join's condition does not remove the row: where it does not hold, the joined relation reads NULL,
    // as it does in PostgreSQL. RIGHT and FULL joins keep the rows of the side this row model does not enumerate,
    // so their conditions stay unread, as every join condition used to be.
    PostgresCatalogExpression[] outerJoins = null;
    for (int i = 0; i < from.size(); i++) {
      final FromEntry entry = from.get(i);
      if (entry.on == null)
        continue;

      if (entry.join == Join.LEFT) {
        final PostgresCatalogExpression condition = PostgresCatalogExpression.parse(entry.on);
        if (condition == null) {
          if (secondAliases != null && secondAliases[i])
            // Whether the second alias matched cannot be told, and it reads the subject's map if it did.
            return DECLINED;
          continue;
        }
        if (outerJoins == null)
          outerJoins = new PostgresCatalogExpression[from.size()];
        outerJoins[i] = condition;
      } else if (entry.join != Join.RIGHT && entry.join != Join.FULL && !readConjuncts(entry.on, filters,
          toleratesUnreadableFilter))
        return DECLINED;
    }

    final List<Joined> surviving = new ArrayList<>(rows.size());
    for (final Row row : rows) {
      final boolean[] nulled = outerJoins == null ? null : new boolean[from.size()];
      final RowResolver resolver = new RowResolver(row, nulled, from, context, outer);

      if (outerJoins != null)
        // In FROM order, so that a condition reading an earlier outer-joined relation sees it already nulled.
        for (int i = 0; i < outerJoins.length; i++)
          if (outerJoins[i] != null) {
            final Object matched = outerJoins[i].evaluate(resolver);
            if (matched != PostgresCatalogExpression.UNKNOWN && !PostgresCatalogExpression.isTrue(matched))
              nulled[i] = true;
          }

      if (secondAliases != null)
        for (int i = 0; i < secondAliases.length; i++)
          if (secondAliases[i] && (nulled == null || !nulled[i]))
            // The second alias matched a row, and the only row this model can hand it is the subject's own.
            return DECLINED;

      final Object passes = passes(filters, resolver);
      if (passes == PostgresCatalogExpression.UNKNOWN && !toleratesUnreadableFilter)
        return DECLINED;
      if (!Boolean.FALSE.equals(passes))
        surviving.add(new Joined(row, nulled));
    }

    // Window functions are the one thing that cannot be computed a row at a time: row_number() is defined by
    // the other rows of its partition. They are computed here, over the rows that survived the filter.
    final Map<PostgresCatalogExpression.WindowCall, Object[]> windows = computeWindows(projection.items, surviving,
        from, context, outer);
    if (windows == null)
      return DECLINED;

    final List<Map<String, Object>> answered;
    if (projection.aggregated || statement.groupBy != null) {
      if (!windows.isEmpty())
        return DECLINED;
      answered = aggregate(statement, projection, surviving, from, context, columnsByRelation, outer);
      if (answered == null)
        return DECLINED;
    } else {
      answered = new ArrayList<>(surviving.size());
      for (int i = 0; i < surviving.size(); i++) {
        final Joined joined = surviving.get(i);
        final Map<String, Object> projected = projectRow(projection,
            new RowResolver(joined.row(), joined.nulled(), from, context, outer).withWindows(windows, i));
        if (projected == null)
          // A projected column this catalog cannot compute: answering the rest of the row would be answering
          // a different question from the one asked.
          return DECLINED;
        answered.add(projected);
      }

      // With no rows to project - an empty database, or a filter that matched nothing - the projection was
      // never evaluated, so a column this catalog cannot produce would go unnoticed and the client would be
      // told the query succeeded. Evaluate it once against an all-null row of the same shape to find out.
      if (answered.isEmpty() && projectRow(projection,
          new RowResolver(probeRow(from, columnsByRelation), null, from, context, outer).withWindows(windows, 0)) == null)
        return DECLINED;
    }

    List<Map<String, Object>> result = answered;

    if (statement.distinct) {
      final List<Map<String, Object>> distinct = new ArrayList<>(result.size());
      final Set<List<Object>> seen = new HashSet<>();
      for (final Map<String, Object> row : result)
        if (seen.add(distinctKey(new ArrayList<>(row.values()))))
          distinct.add(row);
      result = distinct;
    }

    if (statement.orderBy != null)
      result = sort(statement.orderBy, result);
    if (result == null)
      return DECLINED;

    if (statement.limit != null && result.size() > statement.limit)
      result = new ArrayList<>(result.subList(0, statement.limit));

    return new Answer(columnsOf(projection.keys(), result), result, !toleratesUnreadableFilter);
  }

  /** The projected row, or null when one of its columns cannot be computed. */
  private static Map<String, Object> projectRow(final Projection projection, final RowResolver resolver) {
    final Map<String, Object> projected = new LinkedHashMap<>(projection.items.size());
    for (final ProjectionItem item : projection.items) {
      final Object value = item.expression.evaluate(resolver);
      if (value == PostgresCatalogExpression.UNKNOWN)
        return null;
      projected.put(item.alias, value);
    }
    return projected;
  }

  /**
   * Whether a row passes every filter: FALSE as soon as one is false or NULL - NULL AND anything is never TRUE -
   * otherwise UNKNOWN when one could not be evaluated, otherwise TRUE.
   */
  private static Object passes(final List<PostgresCatalogExpression> filters, final RowResolver resolver) {
    boolean unknown = false;
    for (final PostgresCatalogExpression filter : filters) {
      final Object value = filter.evaluate(resolver);
      if (value == PostgresCatalogExpression.UNKNOWN)
        unknown = true;
      else if (!PostgresCatalogExpression.isTrue(value))
        return Boolean.FALSE;
    }
    return unknown ? PostgresCatalogExpression.UNKNOWN : Boolean.TRUE;
  }

  /**
   * Parses a condition into its top-level conjuncts and adds the readable ones to {@code into}.
   *
   * @return false when a conjunct cannot be read and the rows do not tolerate an unreadable filter
   */
  private static boolean readConjuncts(final List<PostgresCatalogToken> tokens,
      final List<PostgresCatalogExpression> into, final boolean toleratesUnreadableFilter) {
    if (tokens == null)
      return true;
    for (final List<PostgresCatalogToken> conjunct : splitConjuncts(tokens)) {
      final PostgresCatalogExpression expression = PostgresCatalogExpression.parse(conjunct);
      if (expression != null)
        into.add(expression);
      else if (!toleratesUnreadableFilter)
        return false;
    }
    return true;
  }

  /**
   * Projects an aggregated query: one row per group of GROUP BY, or one row over every row without one - which
   * is a row even when there is nothing to aggregate, {@code count(*)} being 0 then (issue #10228).
   *
   * @return the rows, or null when a group key or a projected column cannot be computed
   */
  private static List<Map<String, Object>> aggregate(final Statement statement, final Projection projection,
      final List<Joined> surviving, final List<FromEntry> from, final Context context,
      final Map<String, Collection<String>> columnsByRelation, final RowResolver outer) {
    final Map<List<Object>, List<Joined>> groups = new LinkedHashMap<>();

    if (statement.groupBy == null)
      groups.put(List.of(), surviving);
    else {
      final List<PostgresCatalogExpression> keys = groupKeys(statement.groupBy, projection);
      if (keys == null)
        return null;
      for (final Joined joined : surviving) {
        final RowResolver resolver = new RowResolver(joined.row(), joined.nulled(), from, context, outer);
        final List<Object> key = new ArrayList<>(keys.size());
        for (final PostgresCatalogExpression expression : keys) {
          final Object value = expression.evaluate(resolver);
          if (value == PostgresCatalogExpression.UNKNOWN)
            return null;
          key.add(value);
        }
        groups.computeIfAbsent(distinctKey(key), k -> new ArrayList<>()).add(joined);
      }
    }

    final List<Map<String, Object>> answered = new ArrayList<>(groups.size());
    for (final List<Joined> group : groups.values()) {
      // A column outside the aggregates is read off the group's first row, which is the value every row of the
      // group shares when the column is grouped by.
      final Joined first = group.isEmpty() ? new Joined(probeRow(from, columnsByRelation), null) : group.get(0);
      final Map<String, Object> projected = projectRow(projection,
          new RowResolver(first.row(), first.nulled(), from, context, outer).withGroup(group));
      if (projected == null)
        return null;
      answered.add(projected);
    }
    return answered;
  }

  /** GROUP BY's keys: expressions, or ordinals naming a projected column. */
  private static List<PostgresCatalogExpression> groupKeys(final List<PostgresCatalogToken> groupBy,
      final Projection projection) {
    final List<List<PostgresCatalogToken>> items = splitTopLevel(groupBy);
    if (items == null)
      return null;

    final List<PostgresCatalogExpression> keys = new ArrayList<>(items.size());
    for (final List<PostgresCatalogToken> item : items) {
      if (item.size() == 1 && item.get(0).type == PostgresCatalogToken.Type.NUMBER) {
        final int ordinal;
        try {
          ordinal = Integer.parseInt(item.get(0).text);
        } catch (final NumberFormatException e) {
          return null;
        }
        if (ordinal < 1 || ordinal > projection.items.size())
          return null;
        keys.add(projection.items.get(ordinal - 1).expression);
        continue;
      }

      final PostgresCatalogExpression.Parser parser = PostgresCatalogExpression.parser(item);
      final PostgresCatalogExpression key = parser.parseExpression();
      if (key == null || !parser.atEnd() || parser.isAggregated())
        return null;
      keys.add(key);
    }
    return keys;
  }

  /** An all-null row carrying every column the query's relations have, used to validate a projection. */
  private static Row probeRow(final List<FromEntry> from, final Map<String, Collection<String>> columnsByRelation) {
    final Row row = new Row();
    for (final FromEntry entry : from) {
      final Map<String, Object> columns = row.of(entry.relation);
      final Collection<String> known = columnsByRelation.get(entry.relation);
      if (known != null)
        for (final String column : known)
          columns.put(column, null);
    }
    return row.complete();
  }

  /**
   * Computes each window function's value for every row, before any row is projected.
   *
   * @return the values per window call, or null when one of them is a function whose meaning this catalog
   * does not know - in which case the query is declined rather than answered with a number that looks right
   */
  private static Map<PostgresCatalogExpression.WindowCall, Object[]> computeWindows(
      final List<ProjectionItem> projection, final List<Joined> rows, final List<FromEntry> from,
      final Context context, final RowResolver outer) {
    Map<PostgresCatalogExpression.WindowCall, Object[]> windows = null;

    for (final ProjectionItem item : projection) {
      if (!(item.expression instanceof PostgresCatalogExpression.WindowCall call))
        continue;

      if (!"row_number".equals(call.name))
        return null;

      final Object[] values = new Object[rows.size()];
      final Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();

      for (int i = 0; i < rows.size(); i++) {
        final RowResolver resolver = new RowResolver(rows.get(i).row(), rows.get(i).nulled(), from, context, outer);
        final List<Object> key = new ArrayList<>(call.partitionBy.size());
        for (final PostgresCatalogExpression expression : call.partitionBy) {
          final Object value = expression.evaluate(resolver);
          if (value == PostgresCatalogExpression.UNKNOWN)
            return null;
          key.add(value);
        }
        partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
      }

      for (final List<Integer> partition : partitions.values()) {
        if (!call.orderBy.isEmpty()) {
          final Map<Integer, List<Object>> keys = new LinkedHashMap<>();
          for (final Integer index : partition) {
            final RowResolver resolver = new RowResolver(rows.get(index).row(), rows.get(index).nulled(), from, context,
                outer);
            final List<Object> key = new ArrayList<>(call.orderBy.size());
            for (final PostgresCatalogExpression expression : call.orderBy)
              key.add(expression.evaluate(resolver));
            keys.put(index, key);
          }
          partition.sort((left, right) -> {
            final List<Object> l = keys.get(left);
            final List<Object> r = keys.get(right);
            for (int i = 0; i < l.size(); i++) {
              final int comparison = compareValues(l.get(i), r.get(i));
              if (comparison != 0)
                // DESC numbers the partition the other way round, which is the whole point of writing it.
                return call.orderByDescending.get(i) ? -comparison : comparison;
            }
            return 0;
          });
        }

        long number = 0;
        for (final Integer index : partition)
          values[index] = ++number;
      }

      if (windows == null)
        windows = new LinkedHashMap<>();
      windows.put(call, values);
    }

    return windows == null ? Map.of() : windows;
  }

  /**
   * The columns to announce. The type comes from the first value that is not null, exactly as the executor
   * types a column from a sample row; a column that is null in every row is a varchar, which is what an
   * all-null column has always been announced as.
   */
  private static LinkedHashMap<String, PostgresType> columnsOf(final Collection<String> keys,
      final List<Map<String, Object>> rows) {
    final LinkedHashMap<String, PostgresType> columns = new LinkedHashMap<>(keys.size());
    for (final String key : keys) {
      PostgresType type = PostgresType.VARCHAR;
      for (final Map<String, Object> row : rows) {
        final Object value = row.get(key);
        if (value != null) {
          final PostgresType sampled = PostgresType.getTypeForValue(value);
          type = sampled.isArrayType() || sampled.isNativeScalarType() ? sampled : PostgresType.VARCHAR;
          break;
        }
      }
      columns.put(key, type);
    }
    return columns;
  }


  /**
   * Sorts the projected rows. ORDER BY items may name a projected alias, an ordinal position or a source
   * column; an item that resolves to none of those leaves the rows in the order the catalog built them,
   * which is by name, rather than declining a query whose data is perfectly answerable.
   *
   * @return the sorted rows, or null when an item names two output columns alike - ambiguous, so declined
   */
  private static List<Map<String, Object>> sort(final List<PostgresCatalogToken> orderBy,
      final List<Map<String, Object>> projected) {
    final List<List<PostgresCatalogToken>> items = splitTopLevel(orderBy);
    if (items == null || items.isEmpty())
      return projected;

    final List<Integer> keys = new ArrayList<>(items.size());
    final List<Boolean> descending = new ArrayList<>(items.size());
    final List<String> aliases = new ArrayList<>(projected.isEmpty() ? List.of() : projected.get(0).keySet());

    for (final List<PostgresCatalogToken> item : items) {
      if (item.isEmpty())
        return projected;

      final List<PostgresCatalogToken> key = new ArrayList<>(item);
      boolean desc = false;
      while (!key.isEmpty()) {
        final PostgresCatalogToken last = key.get(key.size() - 1);
        if (last.isKeyword("ASC") || last.isKeyword("DESC") || last.isKeyword("NULLS") || last.isKeyword("FIRST")
            || last.isKeyword("LAST")) {
          desc |= last.isKeyword("DESC");
          key.remove(key.size() - 1);
        } else
          break;
      }

      if (key.size() != 1)
        return projected;

      final PostgresCatalogToken token = key.get(0);
      int index = -1;
      if (token.type == PostgresCatalogToken.Type.NUMBER) {
        try {
          index = Integer.parseInt(token.text) - 1;
        } catch (final NumberFormatException e) {
          return projected;
        }
      } else {
        for (int i = 0; i < aliases.size(); i++)
          if (announcedName(aliases.get(i)).equalsIgnoreCase(token.text)) {
            if (index >= 0)
              // Two output columns go by this name: PostgreSQL refuses an ambiguous ORDER BY rather than pick one.
              return null;
            index = i;
          }
      }

      if (index < 0 || index >= aliases.size())
        return projected;

      keys.add(index);
      descending.add(desc);
    }

    final List<Map<String, Object>> sorted = new ArrayList<>(projected);
    sorted.sort((left, right) -> {
      for (int i = 0; i < keys.size(); i++) {
        final String alias = aliases.get(keys.get(i));
        final int comparison = compareValues(left.get(alias), right.get(alias));
        if (comparison != 0)
          return descending.get(i) ? -comparison : comparison;
      }
      return 0;
    });
    return sorted;
  }

  /** One projected expression and the key its column is carried under, see {@link #announcedName}. */
  private static class ProjectionItem {
    final PostgresCatalogExpression expression;
    final String                    alias;

    ProjectionItem(final PostgresCatalogExpression expression, final String alias) {
      this.expression = expression;
      this.alias = alias;
    }
  }

  /** A parsed projection, and whether it aggregates - which turns the rows it is evaluated over into groups. */
  private record Projection(List<ProjectionItem> items, boolean aggregated) {
    List<String> keys() {
      final List<String> keys = new ArrayList<>(items.size());
      for (final ProjectionItem item : items)
        keys.add(item.alias);
      return keys;
    }
  }

  private static Projection parseProjection(final List<PostgresCatalogToken> tokens, final List<FromEntry> from,
      final Map<String, Collection<String>> columnsByRelation) {
    final List<List<PostgresCatalogToken>> items = splitTopLevel(tokens);
    if (items == null || items.isEmpty())
      return null;

    final List<ProjectionItem> projection = new ArrayList<>(items.size());
    final Set<String> keys = new HashSet<>();
    boolean aggregated = false;

    for (final List<PostgresCatalogToken> item : items) {
      if (item.isEmpty())
        return null;

      // "*" and "alias.*" expand to the columns of the relations they name.
      if (item.size() == 1 && item.get(0).isSymbol("*")) {
        for (final FromEntry entry : from)
          for (final String column : columnsByRelation.getOrDefault(entry.relation, List.of()))
            projection.add(new ProjectionItem(new PostgresCatalogExpression.ColumnReference(entry.alias, column),
                uniqueColumnKey(announcedName(column), keys)));
        continue;
      }
      if (item.size() == 3 && item.get(1).isSymbol(".") && item.get(2).isSymbol("*")) {
        final String qualifier = item.get(0).text.toLowerCase(Locale.ENGLISH);
        final FromEntry entry = entryFor(from, qualifier);
        if (entry == null)
          return null;
        for (final String column : columnsByRelation.getOrDefault(entry.relation, List.of()))
          projection.add(new ProjectionItem(new PostgresCatalogExpression.ColumnReference(qualifier, column),
              uniqueColumnKey(announcedName(column), keys)));
        continue;
      }

      final PostgresCatalogExpression.Parser parser = PostgresCatalogExpression.parser(item);
      final PostgresCatalogExpression expression = parser.parseExpression();
      if (expression == null)
        return null;
      aggregated |= parser.isAggregated();

      String alias = null;
      if (!parser.atEnd()) {
        parser.skipKeyword("AS");
        final PostgresCatalogToken token = parser.peek();
        if (token == null)
          return null;
        if (token.type == PostgresCatalogToken.Type.QUOTED_IDENTIFIER)
          alias = token.text;
        else if (token.type == PostgresCatalogToken.Type.IDENTIFIER)
          // PostgreSQL folds every unquoted identifier to lower case.
          alias = token.text.toLowerCase(Locale.ENGLISH);
        else
          return null;

        // Nothing may follow the alias: if something does, this was not an aliased expression but a shape
        // that has not been read correctly.
        final List<PostgresCatalogToken> rest = item.subList(parser.getPosition(), item.size());
        if (rest.size() != 1)
          return null;
      }

      if (alias == null)
        alias = defaultAlias(expression);

      projection.add(new ProjectionItem(expression, uniqueColumnKey(alias, keys)));
    }

    return new Projection(projection, aggregated);
  }

  private static String defaultAlias(final PostgresCatalogExpression expression) {
    if (expression instanceof PostgresCatalogExpression.ColumnReference reference)
      return reference.name;
    if (expression instanceof PostgresCatalogExpression.FunctionCall call)
      return call.name;
    if (expression instanceof PostgresCatalogExpression.WindowCall call)
      // PostgreSQL names an un-aliased window column after its function, exactly as it does a plain call.
      return call.name;
    if (expression instanceof PostgresCatalogExpression.AggregateCall call)
      return call.name;
    if (expression instanceof PostgresCatalogExpression.Subquery subquery)
      return switch (subquery.kind) {
        case EXISTS -> "exists";
        case ARRAY -> "array";
        case SCALAR -> "?column?";
      };
    // PostgreSQL's own name for a column it cannot name from the expression.
    return "?column?";
  }

  private static FromEntry entryFor(final List<FromEntry> from, final String qualifier) {
    final int index = entryIndex(from, qualifier);
    return index < 0 ? null : from.get(index);
  }

  private static int entryIndex(final List<FromEntry> from, final String qualifier) {
    for (int i = 0; i < from.size(); i++) {
      final FromEntry entry = from.get(i);
      if (qualifier.equals(entry.alias) || qualifier.equals(entry.relation) || entry.relation.endsWith("." + qualifier)
          || (entry.alias == null && entry.relation.equals(qualifier)))
        return i;
    }
    return -1;
  }


  // ---------------------------------------------------------------- evaluation context

  /**
   * Resolves a query's column references, session functions, aggregates and sub-selects against one emulated
   * row. A sub-select's resolver has the resolver of the row it is correlated to as its {@code outer}, which is
   * where a name the sub-select's own relations do not have is looked up (issue #10228).
   */
  private static class RowResolver implements PostgresCatalogExpression.Resolver {
    private final Row             row;
    /** Per FROM entry, whether an outer join left it unmatched on this row, so that it reads NULL. */
    private final boolean[]       nulled;
    private final List<FromEntry> from;
    private final Context         context;
    private final RowResolver     outer;

    private Map<PostgresCatalogExpression.WindowCall, Object[]> windows;
    private int                                                 rowIndex;
    /** The rows an aggregate is computed over, null when this resolver is not evaluating a group. */
    private List<Joined>                                        group;

    RowResolver(final Row row, final boolean[] nulled, final List<FromEntry> from, final Context context,
        final RowResolver outer) {
      this.row = row;
      this.nulled = nulled;
      this.from = from;
      this.context = context;
      this.outer = outer;
    }

    RowResolver withWindows(final Map<PostgresCatalogExpression.WindowCall, Object[]> windows, final int rowIndex) {
      this.windows = windows;
      this.rowIndex = rowIndex;
      return this;
    }

    RowResolver withGroup(final List<Joined> group) {
      this.group = group;
      return this;
    }

    @Override
    public Object window(final PostgresCatalogExpression.WindowCall call) {
      if (windows == null)
        return PostgresCatalogExpression.UNKNOWN;
      final Object[] values = windows.get(call);
      if (values == null || rowIndex >= values.length)
        return PostgresCatalogExpression.UNKNOWN;
      return values[rowIndex];
    }

    @Override
    public Object column(final String qualifier, final String name) {
      if (qualifier != null) {
        final int index = entryIndex(from, qualifier);
        if (index < 0)
          return outer != null ? outer.column(qualifier, name) : PostgresCatalogExpression.UNKNOWN;
        final Map<String, Object> columns = row.relations.get(from.get(index).relation);
        if (columns == null || !columns.containsKey(name) || row.isAmbiguous(columns, name))
          return PostgresCatalogExpression.UNKNOWN;
        return nulled != null && nulled[index] ? null : columns.get(name);
      }

      // Unqualified: the first relation in the FROM clause that has such a column, which is how PostgreSQL
      // resolves it too when only one of them does - and then the query around a sub-select.
      for (int i = 0; i < from.size(); i++) {
        final Map<String, Object> columns = row.relations.get(from.get(i).relation);
        if (columns != null && columns.containsKey(name)) {
          if (row.isAmbiguous(columns, name))
            return PostgresCatalogExpression.UNKNOWN;
          return nulled != null && nulled[i] ? null : columns.get(name);
        }
      }
      return outer != null ? outer.column(null, name) : PostgresCatalogExpression.UNKNOWN;
    }

    @Override
    public Object subquery(final List<PostgresCatalogToken> tokens, final PostgresCatalogExpression.SubqueryKind kind) {
      context.charge(tokens.size());
      final Answer answer = resolveStatement(tokens, context, this, true);
      if (answer == null || answer.rows == null)
        return PostgresCatalogExpression.UNKNOWN;

      if (kind == PostgresCatalogExpression.SubqueryKind.EXISTS)
        return !answer.rows.isEmpty();
      if (answer.columns.size() != 1)
        return PostgresCatalogExpression.UNKNOWN;

      if (kind == PostgresCatalogExpression.SubqueryKind.SCALAR) {
        if (answer.rows.size() > 1)
          // PostgreSQL's "more than one row returned by a subquery used as an expression": an error, not a value.
          return PostgresCatalogExpression.UNKNOWN;
        return answer.rows.isEmpty() ? null : answer.rows.get(0).values().iterator().next();
      }

      final List<Object> values = new ArrayList<>(answer.rows.size());
      for (final Map<String, Object> innerRow : answer.rows)
        values.add(innerRow.values().iterator().next());
      return values;
    }

    @Override
    public Object aggregate(final PostgresCatalogExpression.AggregateCall call) {
      if (group == null)
        return PostgresCatalogExpression.UNKNOWN;

      final List<Object> values = new ArrayList<>(group.size());
      final Set<List<Object>> seen = call.distinct ? new HashSet<>() : null;
      final List<Object> separators = "string_agg".equals(call.name) ? new ArrayList<>(group.size()) : null;
      for (final Joined joined : group) {
        if (call.argument == null) {
          // count(*): every row counts, whatever it holds.
          values.add(Boolean.TRUE);
          continue;
        }
        final RowResolver resolver = new RowResolver(joined.row(), joined.nulled(), from, context, outer);
        final Object value = call.argument.evaluate(resolver);
        if (value == PostgresCatalogExpression.UNKNOWN)
          return PostgresCatalogExpression.UNKNOWN;
        // Aggregates skip NULLs; array_agg is the one that collects them.
        if (value == null && !"array_agg".equals(call.name))
          continue;
        if (seen != null && !seen.add(distinctKey(Collections.singletonList(value))))
          continue;
        values.add(value);
        if (separators != null) {
          // string_agg's delimiter is read off each row, as PostgreSQL does: the one before a value is its row's.
          final Object separator = call.extraArguments.get(0).evaluate(resolver);
          if (separator == PostgresCatalogExpression.UNKNOWN)
            return PostgresCatalogExpression.UNKNOWN;
          separators.add(separator);
        }
      }

      return aggregateOf(call.name, values, separators);
    }

    @Override
    public Object function(final String name, final List<Object> arguments) {
      return switch (name) {
        case "current_schema", "current_database", "current_catalog" -> context.schema();
        case "current_user", "session_user", "user", "current_role" -> context.userName();
        case "current_schemas" -> List.of(context.schema());
        case "version" -> "PostgreSQL " + PostgresNetworkExecutor.PG_SERVER_VERSION;
        case "pg_get_userbyid" -> context.userName();
        // Everything this catalog produces is visible to the connection that asked: there is one schema and
        // it is on the search path by definition.
        case "pg_table_is_visible", "pg_type_is_visible", "pg_function_is_visible" -> Boolean.TRUE;
        case "pg_encoding_to_char" -> "UTF8";
        case "format_type" -> formatType(arguments);
        // PostgreSQL's OID-alias types, reached either as a cast ('Article'::regclass) or as the function
        // spelling of the same lookup (to_regclass('Article')). See regClass for which direction each
        // argument type means.
        case "regclass", "to_regclass" -> regClass(arguments, context);
        case "regtype", "to_regtype" -> regType(arguments);
        // The one expression this catalog stores is a column default, whose text pg_attrdef.adbin holds
        // (issue #10228). Anything else - a partition bound, a policy - has no ArcadeDB equivalent, and NULL is
        // what PostgreSQL answers for an object that has none.
        case "pg_get_expr" -> arguments.isEmpty() || arguments.get(0) == null
            || arguments.get(0) == PostgresCatalogExpression.UNKNOWN ? null :
            PostgresCatalogExpression.asString(arguments.get(0));
        case "array_to_string" -> arrayToString(arguments);
        // Comments, defaults and ACLs have no ArcadeDB equivalent, and NULL is what PostgreSQL answers for an
        // object that has none.
        case "obj_description", "col_description", "shobj_description", "pg_get_indexdef", "pg_get_viewdef",
            "pg_get_constraintdef" -> null;
        default -> PostgresCatalogExpression.UNKNOWN;
      };
    }

    /**
     * {@code array_to_string(array, separator)}. An array this catalog cannot compute answers NULL rather than
     * declining the query, which is what it always answered: psql's {@code \d+} builds the reloptions list with
     * one out of a sub-select over {@code unnest()}, and a table here has no options to list.
     */
    private static Object arrayToString(final List<Object> arguments) {
      if (arguments.size() < 2 || !(arguments.get(0) instanceof List<?> list) || arguments.get(1) == null
          || arguments.get(1) == PostgresCatalogExpression.UNKNOWN)
        return null;
      final String separator = PostgresCatalogExpression.asString(arguments.get(1));
      final StringBuilder buffer = new StringBuilder();
      for (final Object element : list) {
        if (element == null)
          continue;
        if (!buffer.isEmpty())
          buffer.append(separator);
        buffer.append(PostgresCatalogExpression.asString(element));
      }
      return buffer.toString();
    }


    /**
     * {@code format_type(oid, typmod)}: the SQL name PostgreSQL prints a type under - {@code integer}, not
     * {@code int4} - which is what psql's {@code \d} shows in its Type column (issue #10228).
     */
    private static Object formatType(final List<Object> arguments) {
      if (arguments.isEmpty() || !(arguments.get(0) instanceof Number oid))
        return PostgresCatalogExpression.UNKNOWN;
      final PostgresType type = PostgresType.byCode(oid.intValue());
      if (type == null)
        return null;
      // The one name format_type() and regtype disagree on: blank-padded char without a length is "bpchar".
      if (type == PostgresType.BPCHAR && (arguments.size() < 2 || !(arguments.get(1) instanceof Number typmod)
          || typmod.intValue() < 0))
        return type.typeName;
      return displayName(type);
    }

    /** The name PostgreSQL prints a type under, which is what {@code regtype} and {@code format_type()} answer. */
    private static String displayName(final PostgresType type) {
      if (type.isArrayType()) {
        final PostgresType element = PostgresType.byCode(type.elementCode);
        return element == null ? type.typeName : displayName(element) + "[]";
      }
      return switch (type) {
        case SMALLINT -> "smallint";
        case INTEGER -> "integer";
        case LONG -> "bigint";
        case REAL -> "real";
        case DOUBLE -> "double precision";
        case CHAR -> "\"char\"";
        case BOOLEAN -> "boolean";
        case TIMESTAMP -> "timestamp without time zone";
        case VARCHAR -> "character varying";
        case BPCHAR -> "character";
        default -> type.typeName;
      };
    }

    /**
     * {@code regclass}: a relation's OID, and a relation's name, depending on which one it was given.
     * <p>
     * A real {@code regclass} is one value that holds an OID and prints as the name; there is no such value
     * here, so the direction the client wrote decides which half it gets - and in both directions it gets
     * the half it is about to use. A name is what a client casts in order to compare against an OID column
     * ({@code cls.oid = 'Article'::regclass}, which is how the Apache Arrow ADBC driver asks for a table's
     * columns, issue #7180), so text answers the OID. An OID is what a client casts in order to print a
     * relation's name ({@code attrelid::regclass}), so a number answers the name. Digits given as text are
     * an OID spelled out, which is how PostgreSQL reads them too.
     * <p>
     * A name no type carries answers NULL - {@code to_regclass}'s answer, rather than the error the cast
     * raises in PostgreSQL. That matters for the WHERE clause: NULL makes the equality false and the query
     * selects nothing, which is the honest answer to "describe the table that is not there", whereas
     * declining to read the cast would leave the permissive-filter rule describing <b>every</b> table.
     */
    private static Object regClass(final List<Object> arguments, final Context context) {
      if (arguments.size() != 1)
        return PostgresCatalogExpression.UNKNOWN;

      final Object value = arguments.get(0);
      if (value == null || value == PostgresCatalogExpression.UNKNOWN)
        return value;

      if (value instanceof Number oid)
        return context.relationNamed(oid.intValue());

      final String written = PostgresCatalogExpression.asString(value).trim();
      if (isDigits(written))
        try {
          return Integer.valueOf(written);
        } catch (final NumberFormatException e) {
          // PostgreSQL's OID space is unsigned 32-bit, and this one is int. A number too big to be an OID
          // this catalog ever handed out is deliberately read as "no such relation" rather than as an
          // error: oidOf never produces one, so nothing it could name exists.
          return null;
        }

      final DocumentType type = context.typeNamed(unqualify(written));
      return type == null ? null : oidOf(type.getName());
    }

    /**
     * {@code regtype}: the same two directions as {@link #regClass}, over the types this protocol can
     * produce. {@code atttypid::regtype} is how a client prints a column's type; {@code 'int4'::regtype} is
     * how it names one to filter by.
     */
    private static Object regType(final List<Object> arguments) {
      if (arguments.size() != 1)
        return PostgresCatalogExpression.UNKNOWN;

      final Object value = arguments.get(0);
      if (value == null || value == PostgresCatalogExpression.UNKNOWN)
        return value;

      if (value instanceof Number oid)
        return typeNamed(oid.intValue());

      final String written = PostgresCatalogExpression.asString(value).trim();
      if (isDigits(written))
        try {
          return typeNamed(Integer.parseInt(written));
        } catch (final NumberFormatException e) {
          return null;
        }

      final PostgresType type = PostgresType.byName(unqualify(written));
      return type == null ? null : type.code;
    }

    /**
     * The relation name out of what a client wrote inside a {@code regclass} literal: the last component of
     * a qualified name, with the double quotes {@code PQescapeIdentifier} wraps it in taken off. An unquoted
     * name is left as written rather than folded to lower case, because the fold is undone by
     * {@link Context#typeNamed} anyway and the verbatim spelling is the one that can match exactly.
     */
    private static String typeNamed(final int oid) {
      final PostgresType type = PostgresType.byCode(oid);
      return type == null ? null : displayName(type);
    }

    private static String unqualify(final String written) {
      final StringBuilder current = new StringBuilder(written.length());
      boolean quoted = false;

      for (int i = 0; i < written.length(); i++) {
        final char c = written.charAt(i);
        if (c == '"') {
          // "" inside a quoted identifier is one literal quote.
          if (quoted && i + 1 < written.length() && written.charAt(i + 1) == '"') {
            current.append('"');
            ++i;
          } else
            quoted = !quoted;
        } else if (c == '.' && !quoted)
          // A new component starts here, so everything read so far was the schema qualification.
          current.setLength(0);
        else
          current.append(c);
      }

      return current.toString();
    }

    private static boolean isDigits(final String text) {
      if (text.isEmpty())
        return false;
      for (int i = 0; i < text.length(); i++)
        if (text.charAt(i) < '0' || text.charAt(i) > '9')
          return false;
      return true;
    }
  }
}
