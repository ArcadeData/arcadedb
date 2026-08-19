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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
  /** The OID of the bootstrap superuser in a stock PostgreSQL, used for every "owner" column. */
  private static final int OWNER_OID      = 10;

  /** A catalog query whose shape this class will not answer. The caller sends an empty result set. */
  public static final Answer DECLINED = new Answer(new LinkedHashMap<>(), null);

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

    private Answer(final LinkedHashMap<String, PostgresType> columns, final List<Map<String, Object>> rows) {
      this.columns = columns;
      this.rows = rows;
    }
  }

  /** The families of rows a catalog query can be about. */
  private enum Family {
    SCHEMAS, TABLES, COLUMNS, DATABASES, ROLES, PRIVILEGES, CHARACTER_SETS, COLLATIONS, VIEWS
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
        "relacl", "reloptions", "relhasoids");
    relation("pg_tables", Family.TABLES, "schemaname", "tablename", "tableowner", "tablespace", "hasindexes",
        "hasrules", "hastriggers", "rowsecurity");
    relation("information_schema.tables", Family.TABLES, "table_catalog", "table_schema", "table_name", "table_type",
        "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema",
        "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action");

    relation("pg_attribute", Family.COLUMNS, "attrelid", "attname", "atttypid", "attstattarget", "attlen", "attnum",
        "attndims", "attcacheoff", "atttypmod", "attbyval", "attstorage", "attalign", "attnotnull", "atthasdef",
        "atthasmissing", "attidentity", "attgenerated", "attisdropped", "attislocal", "attinhcount", "attcollation",
        "attacl", "attoptions", "attfdwoptions");
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

    // Decorating relations: they never decide the family, they only contribute columns to a row. Every one of
    // them is something ArcadeDB has no equivalent of, so their columns are all NULL - which is exactly what a
    // LEFT JOIN against them yields in PostgreSQL when there is no comment, no default and no inheritance.
    relation("pg_description", null, "objoid", "classoid", "objsubid", "description");
    relation("pg_attrdef", null, "oid", "adrelid", "adnum", "adbin", "adsrc");
    relation("pg_type", null, "oid", "typname", "typnamespace", "typowner", "typlen", "typbyval", "typtype",
        "typcategory", "typispreferred", "typisdefined", "typdelim", "typrelid", "typelem", "typarray", "typinput",
        "typoutput", "typbasetype", "typtypmod", "typnotnull", "typndims", "typcollation", "typdefault");
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
    if (query == null || database == null || !mightBeCatalogQuery(query))
      return null;

    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize(query);
    if (tokens == null || tokens.isEmpty())
      return null;

    // A trailing statement terminator is not part of the statement.
    while (!tokens.isEmpty() && tokens.get(tokens.size() - 1).isSymbol(";"))
      tokens.remove(tokens.size() - 1);

    substituteParameters(tokens, parameters);

    return resolveStatement(tokens, new Context(database, userName));
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

  private static Answer resolveStatement(final List<PostgresCatalogToken> tokens, final Context context) {
    if (tokens.isEmpty() || !tokens.get(0).isKeyword("SELECT"))
      // Not a SELECT at all: a WITH, an INSERT, something else entirely. If it names an emulated relation it
      // is still a catalog query, and answering it is not this class's job either way.
      return namesAnEmulatedRelation(tokens) ? DECLINED : null;

    final Statement statement = Statement.split(tokens);
    if (statement == null)
      return namesAnEmulatedRelation(tokens) ? DECLINED : null;

    final List<FromEntry> from = parseFrom(statement.from);
    if (from == null || from.isEmpty())
      return namesAnEmulatedRelation(tokens) ? DECLINED : null;

    final Map<String, Collection<String>> columnsByRelation = new LinkedHashMap<>();
    final List<Row> rows;

    if (from.size() == 1 && from.get(0).derived != null) {
      // A derived table: the inner SELECT is the catalog query and this one is a filter over its output. The
      // JDBC driver's column list is written this way, so answering it means answering both levels.
      final Answer inner = resolveStatement(from.get(0).derived, context);
      if (inner == null || inner.rows == null)
        return inner == null ? null : DECLINED;

      final String name = from.get(0).alias == null ? "" : from.get(0).alias;
      rows = new ArrayList<>(inner.rows.size());
      for (final Map<String, Object> innerRow : inner.rows)
        rows.add(new Row().withAll(name, innerRow));
      columnsByRelation.put(name, inner.columns.keySet());
      from.set(0, new FromEntry(name, null, from.get(0).alias, null));
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

      rows = buildRows(family, context);
    }

    return project(statement, from, rows, context, columnsByRelation);
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

  private static int rank(final Family family) {
    return switch (family) {
      case SCHEMAS -> 1;
      case TABLES, VIEWS -> 2;
      case COLUMNS -> 3;
      default -> 0;
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

  // ---------------------------------------------------------------- statement structure

  /** The clauses of a SELECT, as token ranges. */
  private static class Statement {
    boolean                    distinct;
    List<PostgresCatalogToken> projection;
    List<PostgresCatalogToken> from;
    List<PostgresCatalogToken> where;
    List<PostgresCatalogToken> orderBy;
    Integer                    limit;

    /**
     * Splits a token list at the top-level clause keywords. Returns null for anything with a shape this class
     * does not read: a set operation, a GROUP BY, a sub-select in place of a FROM item.
     */
    static Statement split(final List<PostgresCatalogToken> tokens) {
      final Statement statement = new Statement();
      int i = 1; // past SELECT

      if (i < tokens.size() && tokens.get(i).isKeyword("DISTINCT")) {
        ++i;
        if (i < tokens.size() && tokens.get(i).isKeyword("ON"))
          return null;
        statement.distinct = true;
      } else if (i < tokens.size() && tokens.get(i).isKeyword("ALL"))
        ++i;

      final int projectionStart = i;
      int depth = 0;
      int fromStart = -1;

      for (; i < tokens.size(); i++) {
        final PostgresCatalogToken token = tokens.get(i);
        if (token.isSymbol("(") || token.isSymbol("["))
          ++depth;
        else if (token.isSymbol(")") || token.isSymbol("]"))
          --depth;
        else if (depth == 0 && token.isKeyword("FROM")) {
          fromStart = i + 1;
          break;
        }
      }

      if (fromStart < 0)
        return null;

      statement.projection = new ArrayList<>(tokens.subList(projectionStart, fromStart - 1));
      if (statement.projection.isEmpty())
        return null;

      int end = tokens.size();
      int whereStart = -1;
      int orderStart = -1;
      int limitStart = -1;
      depth = 0;

      for (i = fromStart; i < tokens.size(); i++) {
        final PostgresCatalogToken token = tokens.get(i);
        if (token.isSymbol("(") || token.isSymbol("["))
          ++depth;
        else if (token.isSymbol(")") || token.isSymbol("]"))
          --depth;
        else if (depth == 0) {
          if (token.isKeyword("UNION") || token.isKeyword("INTERSECT") || token.isKeyword("EXCEPT")
              || token.isKeyword("HAVING") || token.isKeyword("GROUP") || token.isKeyword("WINDOW")
              || token.isKeyword("FETCH") || token.isKeyword("OFFSET"))
            return null;
          if (token.isKeyword("WHERE") && whereStart < 0 && orderStart < 0)
            whereStart = i + 1;
          else if (token.isKeyword("ORDER") && orderStart < 0) {
            if (i + 1 >= tokens.size() || !tokens.get(i + 1).isKeyword("BY"))
              return null;
            orderStart = i + 2;
          } else if (token.isKeyword("LIMIT") && limitStart < 0)
            limitStart = i + 1;
        }
      }

      if (limitStart > 0) {
        if (limitStart >= tokens.size())
          return null;
        final PostgresCatalogToken limitToken = tokens.get(limitStart);
        if (limitToken.type != PostgresCatalogToken.Type.NUMBER)
          return null;
        try {
          statement.limit = Integer.valueOf(limitToken.text);
        } catch (final NumberFormatException e) {
          return null;
        }
        end = Math.min(end, limitStart - 1);
      }

      if (orderStart > 0) {
        statement.orderBy = new ArrayList<>(tokens.subList(orderStart, end));
        end = Math.min(end, orderStart - 2);
      }

      if (whereStart > 0) {
        statement.where = new ArrayList<>(tokens.subList(whereStart, end));
        end = Math.min(end, whereStart - 1);
      }

      statement.from = new ArrayList<>(tokens.subList(fromStart, end));
      return statement.from.isEmpty() ? null : statement;
    }
  }

  /** One item of the FROM clause: an emulated relation, or a derived table with the tokens of its SELECT. */
  private static class FromEntry {
    final String                     relation;
    /** The name as the client wrote it, which is what a case-sensitive type lookup has to be given. */
    final String                     writtenAs;
    final String                     alias;
    final List<PostgresCatalogToken> derived;

    FromEntry(final String relation, final String writtenAs, final String alias,
        final List<PostgresCatalogToken> derived) {
      this.relation = relation;
      this.writtenAs = writtenAs;
      this.alias = alias;
      this.derived = derived;
    }
  }

  /**
   * Reads the relations out of a FROM clause: comma-separated items and explicit JOINs alike. The join
   * conditions themselves are skipped rather than evaluated - every emulated row already carries the columns
   * of every relation it can be joined to, so the join is satisfied by construction.
   */
  private static List<FromEntry> parseFrom(final List<PostgresCatalogToken> tokens) {
    final List<FromEntry> entries = new ArrayList<>();
    int i = 0;

    while (i < tokens.size()) {
      // Join decorations before the relation name.
      while (i < tokens.size() && isJoinWord(tokens.get(i)))
        ++i;

      if (i >= tokens.size())
        return entries.isEmpty() ? null : entries;

      final PostgresCatalogToken token = tokens.get(i);

      if (token.isSymbol("(")) {
        // A derived table: read the parenthesised SELECT whole and hand it back for its own resolution.
        int depth = 0;
        final int start = i + 1;
        while (i < tokens.size()) {
          if (tokens.get(i).isSymbol("("))
            ++depth;
          else if (tokens.get(i).isSymbol(")")) {
            --depth;
            if (depth == 0)
              break;
          }
          ++i;
        }
        if (depth != 0 || i >= tokens.size())
          return null;

        final List<PostgresCatalogToken> derived = new ArrayList<>(tokens.subList(start, i));
        ++i; // past ")"

        String derivedAlias = null;
        if (i < tokens.size() && tokens.get(i).isKeyword("AS"))
          ++i;
        if (i < tokens.size() && (tokens.get(i).type == PostgresCatalogToken.Type.IDENTIFIER
            || tokens.get(i).type == PostgresCatalogToken.Type.QUOTED_IDENTIFIER) && !isStructuralWord(tokens.get(i))) {
          derivedAlias = tokens.get(i).text.toLowerCase(Locale.ENGLISH);
          ++i;
        }

        entries.add(new FromEntry(derivedAlias == null ? "" : derivedAlias, null, derivedAlias, derived));

        if (i < tokens.size() && tokens.get(i).isSymbol(","))
          ++i;
        continue;
      }

      if (token.type != PostgresCatalogToken.Type.IDENTIFIER && token.type != PostgresCatalogToken.Type.QUOTED_IDENTIFIER)
        // A function call, a VALUES list: not a shape this catalog reads.
        return null;

      final StringBuilder relation = new StringBuilder(token.text.toLowerCase(Locale.ENGLISH));
      final StringBuilder written = new StringBuilder(token.text);
      ++i;
      while (i + 1 < tokens.size() && tokens.get(i).isSymbol(".")) {
        final PostgresCatalogToken part = tokens.get(i + 1);
        if (part.type != PostgresCatalogToken.Type.IDENTIFIER && part.type != PostgresCatalogToken.Type.QUOTED_IDENTIFIER)
          return null;
        relation.append('.').append(part.text.toLowerCase(Locale.ENGLISH));
        written.setLength(0);
        written.append(part.text);
        i += 2;
      }

      String name = relation.toString();
      // pg_catalog is the schema every pg_ relation lives in, and naming it changes nothing.
      if (name.startsWith("pg_catalog."))
        name = name.substring("pg_catalog.".length());

      String alias = null;
      if (i < tokens.size() && tokens.get(i).isKeyword("AS")) {
        ++i;
        if (i >= tokens.size())
          return null;
        alias = tokens.get(i).text.toLowerCase(Locale.ENGLISH);
        ++i;
      } else if (i < tokens.size() && (tokens.get(i).type == PostgresCatalogToken.Type.IDENTIFIER
          || tokens.get(i).type == PostgresCatalogToken.Type.QUOTED_IDENTIFIER) && !isStructuralWord(tokens.get(i))) {
        alias = tokens.get(i).text.toLowerCase(Locale.ENGLISH);
        ++i;
      }

      entries.add(new FromEntry(name, written.toString(), alias, null));

      // Skip an ON or USING condition, up to the next comma or join at this level.
      if (i < tokens.size() && (tokens.get(i).isKeyword("ON") || tokens.get(i).isKeyword("USING"))) {
        int depth = 0;
        ++i;
        while (i < tokens.size()) {
          final PostgresCatalogToken t = tokens.get(i);
          if (t.isSymbol("(") || t.isSymbol("["))
            ++depth;
          else if (t.isSymbol(")") || t.isSymbol("]"))
            --depth;
          else if (depth == 0 && (t.isSymbol(",") || isJoinWord(t)))
            break;
          ++i;
        }
      }

      if (i < tokens.size() && tokens.get(i).isSymbol(","))
        ++i;
    }

    return entries.isEmpty() ? null : entries;
  }

  private static boolean isJoinWord(final PostgresCatalogToken token) {
    return token.isKeyword("JOIN") || token.isKeyword("LEFT") || token.isKeyword("RIGHT") || token.isKeyword("FULL")
        || token.isKeyword("INNER") || token.isKeyword("OUTER") || token.isKeyword("CROSS") || token.isKeyword("NATURAL");
  }

  private static boolean isStructuralWord(final PostgresCatalogToken token) {
    return isJoinWord(token) || token.isKeyword("ON") || token.isKeyword("USING") || token.isKeyword("WHERE")
        || token.isKeyword("ORDER") || token.isKeyword("GROUP") || token.isKeyword("LIMIT") || token.isKeyword("AS");
  }

  // ---------------------------------------------------------------- rows

  /** What the emulated catalog is a view of: one database, seen by one authenticated user. */
  private record Context(Database database, String userName) {
    String schema() {
      return database.getName();
    }
  }

  /** One emulated catalog row, holding the columns of every relation the query could have joined. */
  private static class Row {
    final Map<String, Map<String, Object>> relations = new HashMap<>();

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
      return this;
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
      case SCHEMAS -> List.of(schemaRow(context).complete());
      case TABLES -> tableRows(context);
      case COLUMNS -> columnRows(context);
      case DATABASES -> List.of(databaseRow(context).complete());
      case ROLES -> List.of(roleRow(context).complete());
      case PRIVILEGES -> List.of(privilegeRow(context).complete());
      case CHARACTER_SETS -> List.of(characterSetRow(context).complete());
      case COLLATIONS -> List.of(collationRow(context).complete());
      // ArcadeDB has no relation that a PostgreSQL client would render as a view.
      case VIEWS -> List.of();
    };
  }

  private static Row schemaRow(final Context context) {
    final String schema = context.schema();
    return new Row()//
        .with("pg_namespace", "oid", SCHEMA_OID, "nspname", schema, "nspowner", OWNER_OID)//
        .with("information_schema.schemata", "catalog_name", schema, "schema_name", schema, "schema_owner",
            context.userName());
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

    return schemaRow(context)//
        .with("pg_class", "oid", oid, "relname", type.getName(), "relnamespace", SCHEMA_OID, "relowner", OWNER_OID,//
            "relkind", "r", "relpersistence", "p", "relnatts", type.getPropertyNames().size(),//
            "relhasindex", !type.getAllIndexes(false).isEmpty(), "relhasrules", Boolean.FALSE,//
            "relhastriggers", Boolean.FALSE, "relhassubclass", !type.getSubTypes().isEmpty(),//
            "relispartition", Boolean.FALSE, "relisshared", Boolean.FALSE, "relispopulated", Boolean.TRUE,//
            "relrowsecurity", Boolean.FALSE, "relforcerowsecurity", Boolean.FALSE, "relchecks", 0,//
            // -1 is PostgreSQL's own "never analysed, count unknown", which is honest here: counting the
            // records of every type to answer a table list would turn a metadata query into a full scan.
            "reltuples", -1, "relpages", 0, "relam", 0, "reltype", 0, "relfilenode", oid, "reltablespace", 0)//
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
                pgType.size > 0 && pgType.size <= 8, "attstorage", "p", "attalign", "i")//
            .with("information_schema.columns", "table_catalog", schema, "table_schema", schema, "table_name",
                type.getName(), "column_name", property.getName(), "ordinal_position", ordinal,//
                "column_default", defaultValue == null ? null : defaultValue.toString(),//
                "is_nullable", notNull ? "NO" : "YES", "data_type", pgType.typeName, "udt_catalog", schema,//
                "udt_schema", "pg_catalog", "udt_name", pgType.typeName, "is_identity", "NO", "is_generated", "NEVER",//
                "is_updatable", "YES", "numeric_precision_radix", numericPrecisionRadix(pgType));

        // The type row a client joins pg_attribute to in order to name the column's type. It describes the
        // column's own type, which is the only reading of that join that makes sense.
        final Map<String, Object> typeColumns = row.of("pg_type");
        for (final String column : PostgresTypeCatalog.COLUMNS)
          typeColumns.put(column, PostgresTypeCatalog.columnValue(pgType, column));
        typeColumns.put("typnamespace", 11);

        rows.add(row.complete());
      }
    }

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

  private static Row databaseRow(final Context context) {
    return new Row().with("pg_database", "oid", FIRST_USER_OID, "datname", context.schema(), "datdba", OWNER_OID,//
        // 6 is UTF8 in PostgreSQL's pg_encoding table, which is the only encoding this protocol speaks.
        "encoding", 6, "datcollate", "C", "datctype", "C", "datistemplate", Boolean.FALSE, "datallowconn", Boolean.TRUE,//
        "datconnlimit", -1, "dattablespace", 0);
  }

  private static Row roleRow(final Context context) {
    // Only the connected user: enumerating the server's accounts is not something an emulated catalog should
    // hand out, and no client needs it to browse the database it is connected to.
    return new Row()//
        .with("pg_roles", "oid", OWNER_OID, "rolname", context.userName(), "rolsuper", Boolean.FALSE, "rolinherit",
            Boolean.TRUE, "rolcreaterole", Boolean.FALSE, "rolcreatedb", Boolean.FALSE, "rolcanlogin", Boolean.TRUE,//
            "rolreplication", Boolean.FALSE, "rolconnlimit", -1, "rolbypassrls", Boolean.FALSE)//
        .with("pg_user", "usename", context.userName(), "usesysid", OWNER_OID, "usecreatedb", Boolean.FALSE,//
            "usesuper", Boolean.FALSE, "userepl", Boolean.FALSE, "usebypassrls", Boolean.FALSE);
  }

  private static Row privilegeRow(final Context context) {
    final String schema = context.schema();
    return new Row().with("information_schema.usage_privileges", "grantor", context.userName(), "grantee",
        context.userName(), "object_catalog", schema, "object_schema", schema, "object_name", schema, "object_type",
        "SCHEMA", "privilege_type", "USAGE", "is_grantable", "NO");
  }

  private static Row characterSetRow(final Context context) {
    return new Row().with("information_schema.character_sets", "character_set_catalog", null, "character_set_schema",
        null, "character_set_name", "UTF8", "character_repertoire", "UCS", "form_of_use", "UTF8",
        "default_collate_catalog", context.schema(), "default_collate_schema", "pg_catalog", "default_collate_name",
        "default");
  }

  private static Row collationRow(final Context context) {
    return new Row().with("information_schema.collations", "collation_catalog", context.schema(), "collation_schema",
        "pg_catalog", "collation_name", "default", "pad_attribute", "NO PAD");
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

  private static Answer project(final Statement statement, final List<FromEntry> from, final List<Row> rows,
      final Context context, final Map<String, Collection<String>> columnsByRelation) {
    final List<ProjectionItem> projection = parseProjection(statement.projection, from, columnsByRelation);
    if (projection == null)
      return DECLINED;

    // A predicate that will not parse as a whole leaves the rows as they are - see the class comment on why
    // an unreadable filter does not get to empty an answer whose every row is one of the user's own types.
    final PostgresCatalogExpression where = statement.where == null ? null :
        PostgresCatalogExpression.parse(statement.where);

    final List<Row> surviving = new ArrayList<>(rows.size());
    for (final Row row : rows)
      if (where == null || PostgresCatalogExpression.isTrue(where.evaluate(new RowResolver(row, from, context, null, 0))))
        surviving.add(row);

    // Window functions are the one thing that cannot be computed a row at a time: row_number() is defined by
    // the other rows of its partition. They are computed here, over the rows that survived the filter.
    final Map<PostgresCatalogExpression.WindowCall, Object[]> windows = computeWindows(projection, surviving, from,
        context);
    if (windows == null)
      return DECLINED;

    final List<Map<String, Object>> answered = new ArrayList<>(surviving.size());
    for (int i = 0; i < surviving.size(); i++) {
      final RowResolver resolver = new RowResolver(surviving.get(i), from, context, windows, i);

      final Map<String, Object> projected = new LinkedHashMap<>(projection.size());
      for (final ProjectionItem item : projection) {
        final Object value = item.expression.evaluate(resolver);
        if (value == PostgresCatalogExpression.UNKNOWN)
          // A projected column this catalog cannot compute: answering the rest of the row would be answering
          // a different question from the one asked.
          return DECLINED;
        projected.put(item.alias, value);
      }

      answered.add(projected);
    }

    // With no rows to project - an empty database, or a filter that matched nothing - the projection was
    // never evaluated, so a column this catalog cannot produce would go unnoticed and the client would be
    // told the query succeeded. Evaluate it once against an all-null row of the same shape to find out.
    if (answered.isEmpty()) {
      final RowResolver probe = new RowResolver(probeRow(from, columnsByRelation), from, context, windows, 0);
      for (final ProjectionItem item : projection)
        if (item.expression.evaluate(probe) == PostgresCatalogExpression.UNKNOWN)
          return DECLINED;
    }

    List<Map<String, Object>> result = answered;

    if (statement.distinct) {
      final List<Map<String, Object>> distinct = new ArrayList<>(result.size());
      final Set<List<Object>> seen = new LinkedHashSet<>();
      for (final Map<String, Object> row : result)
        if (seen.add(new ArrayList<>(row.values())))
          distinct.add(row);
      result = distinct;
    }

    if (statement.orderBy != null)
      result = sort(statement.orderBy, result);

    if (statement.limit != null && result.size() > statement.limit)
      result = new ArrayList<>(result.subList(0, statement.limit));

    return new Answer(columnsOf(projection, result), result);
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
      final List<ProjectionItem> projection, final List<Row> rows, final List<FromEntry> from,
      final Context context) {
    Map<PostgresCatalogExpression.WindowCall, Object[]> windows = null;

    for (final ProjectionItem item : projection) {
      if (!(item.expression instanceof PostgresCatalogExpression.WindowCall call))
        continue;

      if (!"row_number".equals(call.name))
        return null;

      final Object[] values = new Object[rows.size()];
      final Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();

      for (int i = 0; i < rows.size(); i++) {
        final RowResolver resolver = new RowResolver(rows.get(i), from, context, null, i);
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
            final RowResolver resolver = new RowResolver(rows.get(index), from, context, null, index);
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
  private static LinkedHashMap<String, PostgresType> columnsOf(final List<ProjectionItem> projection,
      final List<Map<String, Object>> rows) {
    final LinkedHashMap<String, PostgresType> columns = new LinkedHashMap<>(projection.size());
    for (final ProjectionItem item : projection) {
      PostgresType type = PostgresType.VARCHAR;
      for (final Map<String, Object> row : rows) {
        final Object value = row.get(item.alias);
        if (value != null) {
          final PostgresType sampled = PostgresType.getTypeForValue(value);
          type = sampled.isArrayType() || sampled.isNativeScalarType() ? sampled : PostgresType.VARCHAR;
          break;
        }
      }
      columns.put(item.alias, type);
    }
    return columns;
  }

  /**
   * Sorts the projected rows. ORDER BY items may name a projected alias, an ordinal position or a source
   * column; an item that resolves to none of those leaves the rows in the order the catalog built them,
   * which is by name, rather than declining a query whose data is perfectly answerable.
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
          if (aliases.get(i).equalsIgnoreCase(token.text)) {
            index = i;
            break;
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

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static int compareValues(final Object left, final Object right) {
    if (left == null && right == null)
      return 0;
    // PostgreSQL sorts nulls last by default in ascending order.
    if (left == null)
      return 1;
    if (right == null)
      return -1;
    if (left instanceof Number l && right instanceof Number r)
      return Double.compare(l.doubleValue(), r.doubleValue());
    if (left instanceof Comparable && left.getClass() == right.getClass())
      return ((Comparable) left).compareTo(right);
    return String.valueOf(left).compareTo(String.valueOf(right));
  }

  /** One projected expression and the name its column must be announced under. */
  private static class ProjectionItem {
    final PostgresCatalogExpression expression;
    final String                    alias;

    ProjectionItem(final PostgresCatalogExpression expression, final String alias) {
      this.expression = expression;
      this.alias = alias;
    }
  }

  private static List<ProjectionItem> parseProjection(final List<PostgresCatalogToken> tokens,
      final List<FromEntry> from, final Map<String, Collection<String>> columnsByRelation) {
    final List<List<PostgresCatalogToken>> items = splitTopLevel(tokens);
    if (items == null || items.isEmpty())
      return null;

    final List<ProjectionItem> projection = new ArrayList<>(items.size());

    for (final List<PostgresCatalogToken> item : items) {
      if (item.isEmpty())
        return null;

      // "*" and "alias.*" expand to the columns of the relations they name.
      if (item.size() == 1 && item.get(0).isSymbol("*")) {
        for (final FromEntry entry : from)
          for (final String column : columnsByRelation.getOrDefault(entry.relation, List.of()))
            projection.add(new ProjectionItem(new PostgresCatalogExpression.ColumnReference(entry.alias, column), column));
        continue;
      }
      if (item.size() == 3 && item.get(1).isSymbol(".") && item.get(2).isSymbol("*")) {
        final String qualifier = item.get(0).text.toLowerCase(Locale.ENGLISH);
        final FromEntry entry = entryFor(from, qualifier);
        if (entry == null)
          return null;
        for (final String column : columnsByRelation.getOrDefault(entry.relation, List.of()))
          projection.add(new ProjectionItem(new PostgresCatalogExpression.ColumnReference(qualifier, column), column));
        continue;
      }

      final PostgresCatalogExpression.Parser parser = PostgresCatalogExpression.parser(item);
      final PostgresCatalogExpression expression = parser.parseExpression();
      if (expression == null)
        return null;

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

      projection.add(new ProjectionItem(expression, alias));
    }

    return projection;
  }

  private static String defaultAlias(final PostgresCatalogExpression expression) {
    if (expression instanceof PostgresCatalogExpression.ColumnReference reference)
      return reference.name;
    if (expression instanceof PostgresCatalogExpression.FunctionCall call)
      return call.name;
    if (expression instanceof PostgresCatalogExpression.WindowCall call)
      // PostgreSQL names an un-aliased window column after its function, exactly as it does a plain call.
      return call.name;
    // PostgreSQL's own name for a column it cannot name from the expression.
    return "?column?";
  }

  private static FromEntry entryFor(final List<FromEntry> from, final String qualifier) {
    for (final FromEntry entry : from)
      if (qualifier.equals(entry.alias) || qualifier.equals(entry.relation)
          || entry.relation.endsWith("." + qualifier) || (entry.alias == null && entry.relation.equals(qualifier)))
        return entry;
    return null;
  }

  /** Splits a token list at the commas that are not inside parentheses or brackets. */
  private static List<List<PostgresCatalogToken>> splitTopLevel(final List<PostgresCatalogToken> tokens) {
    final List<List<PostgresCatalogToken>> items = new ArrayList<>();
    List<PostgresCatalogToken> current = new ArrayList<>();
    int depth = 0;

    for (final PostgresCatalogToken token : tokens) {
      if (token.isSymbol("(") || token.isSymbol("["))
        ++depth;
      else if (token.isSymbol(")") || token.isSymbol("]"))
        --depth;
      else if (depth == 0 && token.isSymbol(",")) {
        items.add(current);
        current = new ArrayList<>();
        continue;
      }
      current.add(token);
    }

    if (depth != 0)
      return null;

    items.add(current);
    return items;
  }

  // ---------------------------------------------------------------- evaluation context

  /** Resolves a query's column references and session functions against one emulated row. */
  private static class RowResolver implements PostgresCatalogExpression.Resolver {
    private final Row                                                     row;
    private final List<FromEntry>                                         from;
    private final Context                                                 context;
    private final Map<PostgresCatalogExpression.WindowCall, Object[]>     windows;
    private final int                                                     rowIndex;

    RowResolver(final Row row, final List<FromEntry> from, final Context context,
        final Map<PostgresCatalogExpression.WindowCall, Object[]> windows, final int rowIndex) {
      this.row = row;
      this.from = from;
      this.context = context;
      this.windows = windows;
      this.rowIndex = rowIndex;
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
        final FromEntry entry = entryFor(from, qualifier);
        if (entry == null)
          return PostgresCatalogExpression.UNKNOWN;
        final Map<String, Object> columns = row.relations.get(entry.relation);
        if (columns == null || !columns.containsKey(name))
          return PostgresCatalogExpression.UNKNOWN;
        return columns.get(name);
      }

      // Unqualified: the first relation in the FROM clause that has such a column, which is how PostgreSQL
      // resolves it too when only one of them does.
      for (final FromEntry entry : from) {
        final Map<String, Object> columns = row.relations.get(entry.relation);
        if (columns != null && columns.containsKey(name))
          return columns.get(name);
      }
      return PostgresCatalogExpression.UNKNOWN;
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
        // Comments, defaults and ACLs have no ArcadeDB equivalent, and NULL is what PostgreSQL answers for an
        // object that has none.
        case "obj_description", "col_description", "shobj_description", "pg_get_expr", "pg_get_indexdef",
            "pg_get_viewdef", "pg_get_constraintdef", "array_to_string" -> null;
        default -> PostgresCatalogExpression.UNKNOWN;
      };
    }

    private static Object formatType(final List<Object> arguments) {
      if (arguments.isEmpty() || !(arguments.get(0) instanceof Number oid))
        return PostgresCatalogExpression.UNKNOWN;
      final PostgresType type = PostgresType.byCode(oid.intValue());
      return type == null ? null : type.typeName;
    }
  }
}
