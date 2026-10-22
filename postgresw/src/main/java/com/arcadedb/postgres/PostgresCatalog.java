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
   * The relations a statement reads by joining their rows (issue #10473), because it reads one of them twice or because their
   * rows are not the ones of a table or a column. A statement naming any of them is answered by {@link #resolveJoined}; a type
   * the user called one of these names is theirs, as it is for every relation this catalog does not model.
   */
  private static final Set<String> JOINED_RELATIONS = Set.of("pg_index", "pg_indexes", "pg_constraint", "pg_inherits", "pg_policy",
      "pg_statistic_ext", "pg_publication", "pg_publication_rel", "pg_publication_namespace", "pg_range", "pg_enum",
      "information_schema.table_constraints", "information_schema.key_column_usage");

  /** The relations of {@link #JOINED_RELATIONS} that hold no row in this catalog, whatever the schema. */
  private static final Set<String> NEVER_HAVE_ROWS = Set.of("pg_policy", "pg_statistic_ext", "pg_publication",
      "pg_publication_rel", "pg_publication_namespace", "pg_range", "pg_enum");

  /**
   * A catalog query that would spend more work than this server gives one statement - a client regular expression
   * that backtracks, sub-selects correlated deep enough to multiply. The client is told so, with PostgreSQL's
   * {@code program_limit_exceeded}, rather than handed an empty answer that would look like an empty catalog.
   */
  public static final class WorkLimitExceeded extends Refusal {
    public static final String SQLSTATE = "54000";

    WorkLimitExceeded() {
      super("The catalog query needs more work than one statement may spend: simplify it", SQLSTATE);
    }
  }

  /**
   * A catalog query this server refuses, with the SQLSTATE the client acts on. The text is written by this class and holds no
   * stored value (at most the name of a relation the client typed), and the cause is the client's query, never a fault of the
   * server: so production mode keeps the message, as it does for a refused SET, and logs no SEVERE stack trace for it.
   */
  public abstract static class Refusal extends RuntimeException {
    public final String sqlState;

    Refusal(final String message, final String sqlState) {
      super(message);
      this.sqlState = sqlState;
    }
  }

  /**
   * A catalog query that cannot be answered honestly: it names a relation this class does not model, or has a shape it cannot
   * evaluate. The caller refuses it with {@link UnsupportedCatalogQuery} (issue #10371) - an empty result set would read as an
   * empty catalog, and an ungrouped {@code count(*)} over it would lose its one row.
   */
  public static final Answer DECLINED = new Answer(new LinkedHashMap<>(), null, false);

  /**
   * A catalog query this server will not answer, told to the client with PostgreSQL's {@code feature_not_supported} instead of
   * an empty answer that would read as an empty catalog (issue #10371): the reasoning that refuses a statement for its cost
   * with {@link WorkLimitExceeded} does not depend on why the statement was refused. PostgreSQL's own {@code undefined_table}
   * would claim the relation does not exist, which a real PostgreSQL has.
   */
  public static final class UnsupportedCatalogQuery extends Refusal {
    public static final String SQLSTATE = "0A000";

    UnsupportedCatalogQuery(final String message) {
      super(message, SQLSTATE);
    }
  }

  /**
   * The refusal of a query {@link #resolve} answered {@link #DECLINED}, naming the catalog relation it could not model when
   * there is one, so the client reads WHY rather than only that.
   */
  public static UnsupportedCatalogQuery refusalOf(final String query) {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize(query);
    final String relation = tokens == null ? null : unmodelledRelation(tokens);
    return new UnsupportedCatalogQuery(relation != null
        ? "The catalog relation \"" + relation + "\" is not supported by this server"
        : "This catalog query has a shape this server does not support");
  }

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
    SCHEMAS, TABLES, COLUMNS, DATABASES, ROLES, PRIVILEGES, CHARACTER_SETS, COLLATIONS, VIEWS, TYPES, ATTRDEFS,
    /** pg_class: the types, and the indexes (issue #10473), which PostgreSQL keeps in the same relation. pg_tables is TABLES. */
    CLASSES,
    /** pg_attribute: the columns of the types, and the key columns of their indexes (issue #10473). */
    ATTRIBUTES
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

    relation("pg_class", Family.CLASSES, "oid", "relname", "relnamespace", "reltype", "reloftype", "relowner", "relam",
        "relfilenode", "reltablespace", "relpages", "reltuples", "relallvisible", "reltoastrelid", "relhasindex",
        "relisshared", "relpersistence", "relkind", "relnatts", "relchecks", "relhasrules", "relhastriggers",
        "relhassubclass", "relrowsecurity", "relforcerowsecurity", "relispopulated", "relreplident", "relispartition",
        "relacl", "reloptions", "relhasoids", "relrewrite", "relfrozenxid", "relminmxid", "relpartbound");
    relation("pg_tables", Family.TABLES, "schemaname", "tablename", "tableowner", "tablespace", "hasindexes",
        "hasrules", "hastriggers", "rowsecurity");
    relation("information_schema.tables", Family.TABLES, "table_catalog", "table_schema", "table_name", "table_type",
        "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema",
        "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action");

    relation("pg_attribute", Family.ATTRIBUTES, "attrelid", "attname", "atttypid", "attstattarget", "attlen", "attnum",
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

    // The relations a statement joins instead of reading one row per entity (issue #10473): the indexes and constraints of the
    // types, and what psql's \d asks about a table besides its columns. None has a family, so none of them decides what a statement
    // is about; see JOINED_RELATIONS.
    relation("pg_index", null, "indexrelid", "indrelid", "indnatts", "indnkeyatts", "indisunique", "indnullsnotdistinct",
        "indisprimary", "indisexclusion", "indimmediate", "indisclustered", "indisvalid", "indcheckxmin", "indisready",
        "indislive", "indisreplident", "indkey", "indcollation", "indclass", "indoption", "indexprs", "indpred");
    relation("pg_constraint", null, "oid", "conname", "connamespace", "contype", "condeferrable", "condeferred", "convalidated",
        "conrelid", "contypid", "conindid", "conparentid", "confrelid", "confupdtype", "confdeltype", "confmatchtype",
        "conislocal", "coninhcount", "connoinherit", "conkey", "confkey", "conpfeqop", "conppeqop", "conffeqop", "confdelsetcols",
        "conexclop", "conbin");
    relation("pg_indexes", null, "schemaname", "tablename", "indexname", "tablespace", "indexdef");
    relation("information_schema.table_constraints", null, "constraint_catalog", "constraint_schema", "constraint_name",
        "table_catalog", "table_schema", "table_name", "constraint_type", "is_deferrable", "initially_deferred", "enforced",
        "nulls_distinct");
    relation("information_schema.key_column_usage", null, "constraint_catalog", "constraint_schema", "constraint_name",
        "table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "position_in_unique_constraint");
    // The inheritance of the types: the super-types of each one, in the order they are declared
    relation("pg_inherits", null, "inhrelid", "inhparent", "inhseqno", "inhdetachpending");
    // ArcadeDB has no row-level security policy, extended statistics object or publication: these have no rows, which is the
    // answer to "which does this table have". psql's \d asks all three of every table it describes.
    relation("pg_policy", null, "oid", "polname", "polrelid", "polcmd", "polpermissive", "polroles", "polqual", "polwithcheck");
    relation("pg_statistic_ext", null, "oid", "stxrelid", "stxname", "stxnamespace", "stxowner", "stxkeys", "stxstattarget",
        "stxkind", "stxexprs");
    relation("pg_publication", null, "oid", "pubname", "pubowner", "puballtables", "pubinsert", "pubupdate", "pubdelete",
        "pubtruncate", "pubviaroot");
    relation("pg_publication_rel", null, "oid", "prpubid", "prrelid", "prqual", "prattrs");
    relation("pg_publication_namespace", null, "oid", "pnpubid", "pnnspid");
    // The types a driver asks about besides the built-in ones: a range of another type, and an enumeration with its labels. The types
    // this protocol produces are none of them, so these have no rows, which is what a driver loading every type up front joins them for.
    relation("pg_range", null, "rngtypid", "rngsubtype", "rngmultitypid", "rngcollation", "rngsubopc", "rngcanonical",
        "rngsubdiff");
    relation("pg_enum", null, "oid", "enumtypid", "enumsortorder", "enumlabel");
  }

  private PostgresCatalog() {
  }

  /** The names of the relations this catalog models, for a test that holds every one of them to the same contract. */
  static Set<String> modelledRelations() {
    return Collections.unmodifiableSet(RELATIONS.keySet());
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
      return answer == DECLINED ? null : textOfVectors(answer);
    }

    return textOfVectors(resolveBounded(tokens, new Context(database, userName), false));
  }

  /**
   * The answer with its {@code int2vector} and {@code oidvector} values (an index's {@code indkey}) as the text a client reads
   * them as, the values separated by a space. Done on the answer of the statement, not of a derived table or a sub-select: those
   * go on reading them as the arrays they are, so that {@code indkey[0]} is the first key.
   */
  private static Answer textOfVectors(final Answer answer) {
    if (answer == null || answer.rows == null)
      return answer;
    for (final Map<String, Object> row : answer.rows)
      for (final Map.Entry<String, Object> column : row.entrySet())
        if (column.getValue() instanceof PostgresCatalogIndexes.Vector vector)
          column.setValue(vector.toString());
    return answer;
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

    if (shadowedByUserType(from, context))
      return null;
    if (readsJoinedRelation(statement, from, nested))
      return resolveJoined(statement, from, context, outer);

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
        if (entry.function != null)
          // A table function is read only by the join path (readsJoinedRelation), which did not take this statement: it reads a
          // relation this catalog does not model, so it is a catalog query that cannot be answered only when it names one
          return namesAnEmulatedRelation(tokens) ? DECLINED : null;

        if (entry.derived != null)
          // A derived table joined to something else is read by the join path (readsJoinedRelation); what is left here is one
          // that path did not take, which is past what a catalog emulation should be pretending to do.
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

  /**
   * Whether the statement reads a relation of {@link #JOINED_RELATIONS} that a type of the user's has the name of: the type is
   * theirs, and the statement is the SQL engine's, as it is for every relation this catalog does not model.
   */
  private static boolean shadowedByUserType(final List<FromEntry> from, final Context context) {
    for (final FromEntry entry : from)
      if (entry.derived == null && JOINED_RELATIONS.contains(entry.relation) && (context.database().getSchema()
          .existsType(entry.relation) || entry.writtenAs != null && context.database().getSchema().existsType(entry.writtenAs)))
        return true;
    return false;
  }

  /**
   * Whether the statement is answered by joining the rows of its FROM entries ({@link #resolveJoined}), and every relation it reads
   * is one this catalog models. It is, when it reads:
   * <ul>
   * <li>a relation of {@link #JOINED_RELATIONS};</li>
   * <li>a table function ({@code generate_series}, {@code unnest}), or a derived table that is joined to something else or has an
   * alias list for its columns - the shapes a driver's type lookup is written with (issue #10495);</li>
   * <li>one relation through two aliases that are not the right side of an outer join, which the one-row-per-entity model cannot
   * represent: {@code pg_type t JOIN pg_type arr ON arr.oid = t.typarray}.</li>
   * </ul>
   * The last two are the statements the other path declines, so what that path answers is answered as it was. A statement that
   * reads no emulated relation at all is the join path's only when {@code nested}: a derived table or a sub-select of a catalog
   * query, whose rows come from a table function.
   */
  private static boolean readsJoinedRelation(final Statement statement, final List<FromEntry> from, final boolean nested) {
    boolean joined = false;
    boolean modelled = false;
    for (final FromEntry entry : from) {
      if (entry.function != null)
        joined = true;
      else if (entry.derived != null)
        joined |= from.size() > 1 || entry.columnAliases != null;
      else if (!RELATIONS.containsKey(entry.relation))
        return false;
      else {
        modelled = true;
        joined |= JOINED_RELATIONS.contains(entry.relation);
      }
    }
    if (!joined)
      joined = secondAliasesRead(statement, from) == null;
    return joined && (modelled || nested);
  }

  /**
   * Answers a statement that reads one relation more than once or reads a relation whose rows are not those of a table or a
   * column (issue #10473) - the indexes and constraints of the types - by joining the rows of its FROM entries
   * ({@link PostgresCatalogJoin}), each entry with its own, and then projecting what the join produced like any other statement.
   * <p>
   * The conditions of the WHERE clause and of the inner joins are split into conjuncts and each is evaluated as soon as the last
   * entry it reads is bound; the ON condition of a LEFT JOIN only decides whether a row of its entry matches. A RIGHT or FULL join
   * is declined, as it is for every other statement. The rows of the answer are the ones PostgreSQL's would be, minus the system
   * relations this catalog does not have.
   */
  private static Answer resolveJoined(final Statement statement, final List<FromEntry> from, final Context context,
      final RowResolver outer) {
    final int size = from.size();

    // What each entry reads, built once: the rows of a relation, or of a derived table or a table function
    final JoinedEntry[] entries = new JoinedEntry[size];
    for (int i = 0; i < size; i++) {
      entries[i] = joinedEntry(from.get(i), from, context, outer);
      if (entries[i] == null)
        return DECLINED;
    }

    final Map<String, Collection<String>> columnsByRelation = new LinkedHashMap<>();
    // the rows of a type the catalog only has a fixed set of are a real subset a filter selects, see resolveNested
    boolean tolerant = true;
    boolean neverHasRows = false;
    for (int i = 0; i < size; i++) {
      final FromEntry entry = from.get(i);
      columnsByRelation.put(entry.relation, entries[i].columns());
      if (entry.derived == null && entry.function == null) {
        tolerant &= FAMILIES.get(entry.relation) != Family.TYPES;
        neverHasRows |= NEVER_HAVE_ROWS.contains(entry.relation) && entry.join != Join.LEFT;
      }
    }

    final Projection projection = parseProjection(statement.projection, from, columnsByRelation);
    if (projection == null || readsMergedColumns(statement, from))
      return DECLINED;

    // The key each entry's columns are kept under in a row, so that two entries naming one relation read two rows
    final String[] keys = new String[size];
    final Set<String> keyed = new HashSet<>();
    for (int i = 0; i < size; i++)
      keys[i] = keyed.add(from.get(i).relation) ? from.get(i).relation : from.get(i).relation + '#' + i;

    // The entry an unqualified name belongs to: the first with such a column, as the resolver reads it
    final Map<String, Integer> owners = new HashMap<>();
    for (int i = 0; i < size; i++)
      for (final String column : entries[i].columns())
        owners.putIfAbsent(column.toLowerCase(Locale.ENGLISH), i);

    final List<PostgresCatalogExpression> where = new ArrayList<>();
    if (!readConjuncts(statement.where, where, tolerant))
      return DECLINED;
    final List<List<PostgresCatalogExpression>> matching = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final FromEntry entry = from.get(i);
      final List<PostgresCatalogExpression> own = new ArrayList<>();
      matching.add(own);
      if (entry.join == Join.RIGHT || entry.join == Join.FULL)
        return DECLINED;
      // USING (c) is the equality of the column with the first earlier entry that has it, written as an ON condition
      final List<PostgresCatalogToken> condition = entry.using != null ? usingCondition(i, from, entries) : entry.on;
      if (entry.using != null && condition == null)
        return DECLINED;
      if (condition == null)
        continue;
      // an inner join's condition filters like the WHERE clause; an outer join's decides what its entry matches
      if (!readConjuncts(condition, entry.join == Join.LEFT ? own : where, tolerant))
        return DECLINED;
    }

    final List<List<PostgresCatalogExpression>> atLevel = new ArrayList<>(size);
    for (int i = 0; i < size; i++)
      atLevel.add(new ArrayList<>());
    for (final PostgresCatalogExpression condition : where)
      atLevel.get(lastEntryRead(condition, from, owners)).add(condition);

    final PostgresCatalogJoin.Source[] sources = new PostgresCatalogJoin.Source[size];
    for (int i = 0; i < size; i++) {
      final FromEntry entry = from.get(i);
      final boolean outerJoin = entry.join == Join.LEFT;
      // Only a condition that decides which rows an entry matches finds them: a WHERE condition on an outer join's entry is
      // applied to the row the join produced, which may be the NULL one
      final List<PostgresCatalogExpression> candidates = outerJoin ? matching.get(i) : atLevel.get(i);
      final List<PostgresCatalogJoin.Access> accesses = new ArrayList<>();
      for (final PostgresCatalogExpression condition : candidates)
        accessesOf(condition, i, from, owners, accesses);
      sources[i] = new PostgresCatalogJoin.Source(entries[i].rows(), entries[i].nullRow(), outerJoin, matching.get(i), accesses);
    }

    final PostgresCatalogJoin.Evaluator evaluator = (expression, bound, nulled) -> {
      final Row row = new Row();
      row.keys = keys;
      for (int i = 0; i < bound.length; i++)
        if (bound[i] != null)
          row.relations.put(keys[i], bound[i]);
      return expression.evaluate(new RowResolver(row, nulled, from, context, outer));
    };

    final List<PostgresCatalogJoin.Binding> bindings;
    try {
      bindings = PostgresCatalogJoin.join(sources, atLevel, tolerant, evaluator, context::charge);
    } catch (final PostgresCatalogJoin.Declined e) {
      return DECLINED;
    }

    final List<Joined> surviving = new ArrayList<>(bindings.size());
    for (final PostgresCatalogJoin.Binding binding : bindings) {
      final Row row = new Row();
      row.keys = keys;
      for (int i = 0; i < size; i++)
        row.relations.put(keys[i], binding.rows()[i]);
      surviving.add(new Joined(row, binding.nulled()));
    }

    return finish(statement, projection, surviving, from, context, columnsByRelation, tolerant, outer, neverHasRows);
  }

  /**
   * What a join reads from one FROM entry.
   *
   * @param columns the names of its columns, in the order a {@code *} lists them
   * @param rows    the rows it can be bound to, one map of its columns each
   * @param nullRow what it reads when an outer join matched nothing: the same columns, all NULL
   */
  private record JoinedEntry(Collection<String> columns, List<Map<String, Object>> rows, Map<String, Object> nullRow) {
  }

  /** The rows of a FROM entry: those of the relation, the answer of the derived table, or the rows the table function produces. */
  private static JoinedEntry joinedEntry(final FromEntry entry, final List<FromEntry> from, final Context context,
      final RowResolver outer) {
    if (entry.function != null)
      return tableFunction(entry, from, context, outer);
    if (entry.derived != null)
      return derivedTable(entry, context, outer);
    return new JoinedEntry(RELATIONS.get(entry.relation), context.joinRows(entry.relation), nullRow(entry.relation));
  }

  /**
   * A derived table joined to other entries: the inner SELECT is the catalog query and its answer is the relation, its columns named
   * as the alias list says, else as the SELECT announces them. It is read whole, once, and sees no earlier entry of the FROM clause
   * (that would be a LATERAL join); the statement around it is the one it may be correlated to.
   *
   * @return null when the statement cannot be answered: the inner SELECT could not be, the derived table has no alias (PostgreSQL
   * demands one), or the alias list names more columns than it has
   */
  private static JoinedEntry derivedTable(final FromEntry entry, final Context context, final RowResolver outer) {
    if (entry.alias == null)
      return null;
    final Answer inner = resolveStatement(entry.derived, context, outer, true);
    if (inner == null || inner.rows == null)
      return null;

    final List<String> announced = new ArrayList<>(inner.columns.keySet());
    final List<String> names = new ArrayList<>(announced);
    if (entry.columnAliases != null) {
      if (entry.columnAliases.size() > names.size())
        return null;
      for (int i = 0; i < entry.columnAliases.size(); i++)
        names.set(i, entry.columnAliases.get(i));
    }

    final List<Map<String, Object>> rows = new ArrayList<>(inner.rows.size());
    for (final Map<String, Object> innerRow : inner.rows) {
      final Map<String, Object> row = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (int i = 0; i < names.size(); i++)
        row.put(names.get(i), innerRow.get(announced.get(i)));
      rows.add(row);
    }
    return new JoinedEntry(names, rows, nullColumns(names));
  }

  /**
   * A table function in FROM: {@code generate_series(start, stop [, step])} over integers and {@code unnest(array)}, each a relation
   * of one column, named by the alias list ({@code AS s(r)}), else after the alias, else after the function, as PostgreSQL names it.
   * The arguments are evaluated once, with no earlier entry of the FROM clause in sight (an implicit LATERAL join is not read); an
   * argument this catalog cannot evaluate, a function it does not know, and a series PostgreSQL itself refuses (a step of zero)
   * decline the statement. A series is charged to the work budget before it is built, so that a statement cannot ask for a billion
   * rows.
   *
   * @return null when the statement cannot be answered
   */
  private static JoinedEntry tableFunction(final FromEntry entry, final List<FromEntry> from, final Context context,
      final RowResolver outer) {
    final List<List<PostgresCatalogToken>> argumentTokens = splitTopLevel(entry.functionArguments);
    if (argumentTokens == null)
      return null;

    final RowResolver resolver = new RowResolver(new Row(), null, from, context, outer);
    final List<Object> arguments = new ArrayList<>(argumentTokens.size());
    for (final List<PostgresCatalogToken> tokens : argumentTokens) {
      final PostgresCatalogExpression expression = tokens.isEmpty() ? null : PostgresCatalogExpression.parse(tokens);
      if (expression == null)
        return null;
      final Object value = expression.evaluate(resolver);
      if (value == PostgresCatalogExpression.UNKNOWN)
        return null;
      arguments.add(value);
    }

    final List<Object> elements = switch (entry.function) {
      case "generate_series" -> generateSeries(arguments, context);
      case "unnest" -> arguments.size() == 1 ? unnest(arguments.get(0)) : null;
      default -> null;
    };
    if (elements == null)
      return null;

    if (entry.columnAliases != null && entry.columnAliases.size() != 1)
      return null;
    final String column = entry.columnAliases != null ? entry.columnAliases.get(0) : entry.alias;
    final List<Map<String, Object>> rows = new ArrayList<>(elements.size());
    for (final Object element : elements) {
      final Map<String, Object> row = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      row.put(column, element);
      rows.add(row);
    }
    return new JoinedEntry(List.of(column), rows, nullColumns(List.of(column)));
  }

  /**
   * {@code generate_series} over integers: the values from {@code start} to {@code stop} by {@code step} (1 by default), none when
   * the step does not lead from one to the other, and none for a NULL argument, as the function is strict.
   *
   * @return null for what PostgreSQL refuses (a step of zero) or this catalog does not read (a value that is no integer)
   */
  private static List<Object> generateSeries(final List<Object> arguments, final Context context) {
    if (arguments.size() < 2 || arguments.size() > 3)
      return null;
    for (final Object argument : arguments)
      if (argument != null && !(argument instanceof Long || argument instanceof Integer || argument instanceof Short
          || argument instanceof Byte))
        return null;
    if (arguments.contains(null))
      return new ArrayList<>();

    final long start = ((Number) arguments.get(0)).longValue();
    final long stop = ((Number) arguments.get(1)).longValue();
    final long step = arguments.size() == 3 ? ((Number) arguments.get(2)).longValue() : 1L;
    // a step of zero is an error in PostgreSQL; the one of the smallest long has no absolute value to divide the distance by
    if (step == 0L || step == Long.MIN_VALUE)
      return null;
    if (step > 0L ? start > stop : start < stop)
      return new ArrayList<>();

    // how many values: charged before they are built, and an overflow of the distance is more than any budget
    long count;
    try {
      count = Math.addExact(Math.subtractExact(step > 0L ? stop : start, step > 0L ? start : stop) / Math.abs(step), 1L);
    } catch (final ArithmeticException e) {
      count = Long.MAX_VALUE;
    }
    context.charge((int) Math.min(count, Integer.MAX_VALUE));

    final List<Object> values = new ArrayList<>((int) count);
    long value = start;
    for (long i = 0; i < count; i++) {
      values.add(value);
      value += step;
    }
    return values;
  }

  /** {@code unnest(array)}: the elements of an array, none for a NULL one; null when the argument is no array this catalog reads. */
  private static List<Object> unnest(final Object array) {
    if (array == null)
      return new ArrayList<>();
    return array instanceof List<?> list ? new ArrayList<>(list) : null;
  }

  private static Map<String, Object> nullColumns(final Collection<String> names) {
    final Map<String, Object> columns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (final String name : names)
      columns.put(name, null);
    return columns;
  }

  /**
   * The condition {@code USING (a, b)} stands for, as the tokens of an ON clause: each column of the entry equals the same column of
   * the first earlier entry that has it.
   *
   * @return null when the entry or every earlier one lacks a column it names, which PostgreSQL refuses
   */
  private static List<PostgresCatalogToken> usingCondition(final int index, final List<FromEntry> from,
      final JoinedEntry[] entries) {
    final List<PostgresCatalogToken> condition = new ArrayList<>();
    for (final String column : from.get(index).using) {
      if (!hasColumn(entries[index].columns(), column))
        return null;
      int left = -1;
      for (int i = 0; i < index && left < 0; i++)
        if (hasColumn(entries[i].columns(), column))
          left = i;
      if (left < 0)
        return null;

      if (!condition.isEmpty())
        condition.add(PostgresCatalogToken.keyword("AND"));
      addQualifiedColumn(condition, from.get(left), column);
      condition.add(PostgresCatalogToken.symbol("="));
      addQualifiedColumn(condition, from.get(index), column);
    }
    return condition;
  }

  private static void addQualifiedColumn(final List<PostgresCatalogToken> into, final FromEntry entry, final String column) {
    // the name a column reference resolves the entry by: its alias, else its relation without the schema
    final String qualifier = entry.alias != null ? entry.alias : entry.relation.substring(entry.relation.lastIndexOf('.') + 1);
    into.add(PostgresCatalogToken.keyword(qualifier));
    into.add(PostgresCatalogToken.symbol("."));
    into.add(PostgresCatalogToken.keyword(column));
  }

  private static boolean hasColumn(final Collection<String> columns, final String name) {
    for (final String column : columns)
      if (column.equalsIgnoreCase(name))
        return true;
    return false;
  }

  /**
   * Whether the statement lists the columns of an entry joined with USING by a {@code *}: PostgreSQL merges the columns USING names
   * into one, and this catalog lists the entries' own, so what such a list would answer is not what the client asked.
   */
  private static boolean readsMergedColumns(final Statement statement, final List<FromEntry> from) {
    boolean merged = false;
    for (final FromEntry entry : from)
      merged |= entry.using != null;
    if (!merged)
      return false;

    final List<List<PostgresCatalogToken>> items = splitTopLevel(statement.projection);
    if (items == null)
      return true;
    for (final List<PostgresCatalogToken> item : items)
      if (!item.isEmpty() && item.get(item.size() - 1).isSymbol("*") && (item.size() == 1 || item.size() == 3))
        return true;
    return false;
  }

  /** The index of the last FROM entry a condition reads, which is the level it can be evaluated at; the last entry when unknown. */
  private static int lastEntryRead(final PostgresCatalogExpression condition, final List<FromEntry> from,
      final Map<String, Integer> owners) {
    final int[] last = { 0 };
    condition.collectColumns(new PostgresCatalogExpression.Columns() {
      @Override
      public void column(final String qualifier, final String name) {
        last[0] = Math.max(last[0], entryOf(qualifier, name, from, owners));
      }

      @Override
      public void anyColumn() {
        last[0] = from.size() - 1;
      }
    });
    return last[0];
  }

  /** The FROM entry a column reference reads, -1 for a name of the statement around a sub-select. */
  private static int entryOf(final String qualifier, final String name, final List<FromEntry> from,
      final Map<String, Integer> owners) {
    if (qualifier != null)
      return entryIndex(from, qualifier.toLowerCase(Locale.ENGLISH));
    final Integer owner = owners.get(name.toLowerCase(Locale.ENGLISH));
    return owner == null ? -1 : owner;
  }

  /** Whether an expression reads no entry from {@code entry} on: what is decided once the entries before it are bound. */
  private static boolean readsOnlyBefore(final PostgresCatalogExpression expression, final int entry, final List<FromEntry> from,
      final Map<String, Integer> owners) {
    final boolean[] before = { true };
    expression.collectColumns(new PostgresCatalogExpression.Columns() {
      @Override
      public void column(final String qualifier, final String name) {
        before[0] &= entryOf(qualifier, name, from, owners) < entry;
      }

      @Override
      public void anyColumn() {
        before[0] = false;
      }
    });
    return before[0];
  }

  /** Adds how the rows of FROM entry {@code entry} can be found by {@code condition}, when it equates one of its columns. */
  private static void accessesOf(final PostgresCatalogExpression condition, final int entry, final List<FromEntry> from,
      final Map<String, Integer> owners, final List<PostgresCatalogJoin.Access> into) {
    final PostgresCatalogExpression.Equality equality = PostgresCatalogExpression.equalityOf(condition);
    if (equality == null)
      return;
    for (int side = 0; side < 2; side++) {
      final PostgresCatalogExpression.ColumnReference column = PostgresCatalogExpression.columnOf(
          side == 0 ? equality.left() : equality.right());
      final PostgresCatalogExpression key = side == 0 ? equality.right() : equality.left();
      if (column != null && entryOf(column.qualifier, column.name, from, owners) == entry && readsOnlyBefore(key, entry, from,
          owners))
        into.add(new PostgresCatalogJoin.Access(column.name, key));
    }
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
      case TABLES, VIEWS, CLASSES -> 3;
      case ATTRDEFS -> 4;
      case COLUMNS, ATTRIBUTES -> 5;
    };
  }

  private static boolean isCatalogRelationName(final String relation) {
    return relation.startsWith("pg_") || relation.startsWith("information_schema.");
  }

  /**
   * The catalog relations a statement reads, as written in the catalog ({@code pg_index}, {@code information_schema.tables}), in
   * the order they appear. A function call is not a relation, and neither is {@code pg_type.oid}: a name qualified by anything
   * but {@code pg_catalog} or {@code information_schema} is a column reference, whose relation is named where it is read from.
   */
  private static Set<String> catalogRelationsRead(final List<PostgresCatalogToken> tokens) {
    final Set<String> relations = new LinkedHashSet<>();
    for (int i = 0; i < tokens.size(); i++) {
      final PostgresCatalogToken token = tokens.get(i);
      if (token.type != PostgresCatalogToken.Type.IDENTIFIER)
        continue;

      String name = token.text.toLowerCase(Locale.ENGLISH);
      int last = i;
      if (i + 2 < tokens.size() && tokens.get(i + 1).isSymbol(".")
          && tokens.get(i + 2).type == PostgresCatalogToken.Type.IDENTIFIER) {
        last = i + 2;
        final String member = tokens.get(i + 2).text.toLowerCase(Locale.ENGLISH);
        if (name.equals("pg_catalog"))
          name = member;
        else if (name.equals("information_schema"))
          name = name + "." + member;
        else {
          i = last;
          continue;
        }
      }

      final boolean called = last + 1 < tokens.size() && tokens.get(last + 1).isSymbol("(");
      if (!called && isCatalogRelationName(name))
        relations.add(name);
      i = last;
    }
    return relations;
  }

  /** The first catalog relation the statement reads that this class does not model, or null when there is none. */
  private static String unmodelledRelation(final List<PostgresCatalogToken> tokens) {
    for (final String relation : catalogRelationsRead(tokens))
      if (!RELATIONS.containsKey(relation))
        return relation;
    return null;
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

    private List<PostgresCatalogIndexes.Info>         indexes;
    private Set<String>                               indexedTypes;
    private Map<Integer, PostgresCatalogIndexes.Info> indexesByOid;
    private Map<Integer, PostgresCatalogIndexes.Info> constraintsByOid;
    /** The rows of each relation a join reads, built at most once per statement for the same reason {@link #rowsByFamily} is. */
    private final Map<String, List<Map<String, Object>>> joinRows = new HashMap<>();

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

    /** The indexes of the database (issue #10473), built once per statement: the rows of {@code pg_index} and what refers to them. */
    List<PostgresCatalogIndexes.Info> indexes() {
      if (indexes == null) {
        final List<DocumentType> types = sortedTypes(this);
        final Set<Integer> typeOids = new HashSet<>();
        for (final DocumentType type : types)
          typeOids.add(oidOf(type.getName()));
        indexes = PostgresCatalogIndexes.of(types, type -> oidOf(type.getName()), PostgresCatalog::sortedProperties, typeOids,
            FIRST_USER_OID, OID_SPACE);

        indexedTypes = new HashSet<>();
        indexesByOid = new HashMap<>();
        constraintsByOid = new HashMap<>();
        for (final PostgresCatalogIndexes.Info info : indexes) {
          indexedTypes.add(info.type().getName());
          indexesByOid.put(info.oid(), info);
          if (info.hasConstraint())
            constraintsByOid.put(info.constraintOid(), info);
        }
      }
      return indexes;
    }

    boolean hasIndexes(final String typeName) {
      indexes();
      return indexedTypes.contains(typeName);
    }

    PostgresCatalogIndexes.Info indexWithOid(final int oid) {
      indexes();
      return indexesByOid.get(oid);
    }

    PostgresCatalogIndexes.Info constraintWithOid(final int oid) {
      indexes();
      return constraintsByOid.get(oid);
    }

    List<Map<String, Object>> joinRows(final String relation) {
      List<Map<String, Object>> rows = joinRows.get(relation);
      if (rows == null) {
        rows = buildJoinRows(relation, this);
        joinRows.put(relation, rows);
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
    /**
     * For a row of a join (issue #10473), the key {@link #relations} holds each FROM entry's columns under, so that two entries
     * naming one relation read two rows; null for the rows of the other statements, whose entries are told apart by relation.
     */
    String[]                               keys;
    /** Whether a derived table's row carries two columns announced alike, see {@link #announcedName}. */
    boolean duplicateNames;

    /** The columns FROM entry {@code index} reads in this row, null when it is not bound. */
    Map<String, Object> columnsOf(final List<FromEntry> from, final int index) {
      return relations.get(keys != null ? keys[index] : from.get(index).relation);
    }

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
        // a statement that reads one of these joins the rows of its own (issue #10473), so no row of another carries them
        if (JOINED_RELATIONS.contains(relation.getKey()))
          continue;
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
      case CLASSES -> classRows(context);
      case ATTRIBUTES -> attributeRows(context);
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

    // relhasindex and hasindexes follow pg_index (issue #10473): psql's \d reads the flag before it lists the indexes of a table
    final boolean hasIndexes = context.hasIndexes(type.getName());
    return singletonRow(context)//
        .with("pg_class", "oid", oid, "relname", type.getName(), "relnamespace", SCHEMA_OID, "relowner", OWNER_OID,//
            "relkind", "r", "relpersistence", "p", "relnatts", type.getPropertyNames().size(),//
            "relhasindex", hasIndexes, "relhasrules", Boolean.FALSE,//
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
            "hasindexes", hasIndexes, "hasrules", Boolean.FALSE, "hastriggers", Boolean.FALSE,//
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
            .with("pg_attribute", attributeValues(oidOf(type.getName()), property.getName(), ordinal, pgType, notNull,
                defaultValue != null))//
            .with("information_schema.columns", "table_catalog", schema, "table_schema", schema, "table_name",
                type.getName(), "column_name", property.getName(), "ordinal_position", ordinal,//
                "column_default", defaultValue == null ? null : defaultValue.toString(),//
                // data_type is the SQL standard name and udt_name the pg_type one: PostgreSQL keeps the two apart, and an
                // array is the literal ARRAY in the first (its element type is another relation's business) - issue #10370
                "is_nullable", notNull ? "NO" : "YES", "data_type", pgType.isArrayType() ? "ARRAY" : pgType.sqlName(),
                "udt_catalog", schema, "udt_schema", PG_CATALOG_SCHEMA, "udt_name", pgType.typeName, "is_identity", "NO",
                "is_generated", "NEVER", "is_updatable", "YES",//
                // no column here declares a length, a precision or a scale, which is PostgreSQL's typmod -1: the figures below
                // are the ones it answers for a built-in type declared bare
                "character_octet_length", characterOctetLength(pgType), "numeric_precision", numericPrecision(pgType),
                "numeric_precision_radix", numericPrecisionRadix(pgType), "numeric_scale", numericScale(pgType),
                "datetime_precision", datetimePrecision(pgType));

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

  /**
   * The {@code pg_attribute} columns of the {@code ordinal}th column of a relation, name and value in turn: the one answer for a
   * table's columns and for the key columns of its indexes (issue #10473), so a client reading one is told what it reads in the other.
   */
  private static Object[] attributeValues(final int relationOid, final String name, final int ordinal, final PostgresType pgType,
      final boolean notNull, final boolean hasDefault) {
    return new Object[] { "attrelid", relationOid, "attname", name, "attnum", ordinal, "atttypid", pgType.code, "attlen",
        pgType.size, "atttypmod", -1, "attnotnull", notNull, "atthasdef", hasDefault, "attisdropped", Boolean.FALSE, "attislocal",
        Boolean.TRUE, "attinhcount", 0, "attndims", pgType.isArrayType() ? 1 : 0, "attcollation", 0, "attstattarget", -1,
        "attidentity", "", "attgenerated", "", "atthasmissing", Boolean.FALSE, "attbyval", pgType.size > 0 && pgType.size <= 8,
        "attalign", "i", "attcompression", "",
        // A variable-length value is stored "extended" in PostgreSQL, a fixed-length one "plain".
        "attstorage", pgType.size > 0 ? "p" : "x" };
  }

  /** One row per column that declares a default: pg_attrdef's rows, each carrying its column's (issue #10228). */
  private static List<Row> defaultRows(final Context context) {
    final List<Row> rows = new ArrayList<>();
    for (final Row row : context.rows(Family.COLUMNS))
      if (row.relations.get("pg_attrdef").get("adrelid") != null)
        rows.add(row);
    return rows;
  }

  // ---------------------------------------------------------------- the rows of a join (issue #10473)

  /** A relation's row: every column it has, NULL unless {@code namesAndValues} gives it, so that a column no one set still reads. */
  private static Map<String, Object> completeMap(final String relation, final Object... namesAndValues) {
    final Map<String, Object> columns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (final String column : RELATIONS.get(relation))
      columns.put(column, null);
    for (int i = 0; i < namesAndValues.length; i += 2)
      columns.put((String) namesAndValues[i], namesAndValues[i + 1]);
    return columns;
  }

  /** What an entry reads when an outer join matched nothing: the columns of its relation, all NULL. */
  private static Map<String, Object> nullRow(final String relation) {
    return completeMap(relation);
  }

  /**
   * The rows a join reads from {@code relation}, one map of its columns each. The relations of a table, a column or a schema
   * are the ones the other statements are answered from, so the two cannot disagree; the indexes, the constraints and the
   * inheritance are built here, and a relation with no equivalent in ArcadeDB has no row.
   */
  private static List<Map<String, Object>> buildJoinRows(final String relation, final Context context) {
    return switch (relation) {
      case "pg_am" -> accessMethodRows();
      case "pg_namespace" -> namespaceRows(context);
      case "pg_range", "pg_enum" -> List.of();
      case "pg_index" -> indexRows(context);
      case "pg_indexes" -> indexListRows(context);
      case "pg_constraint" -> constraintRows(context);
      case "information_schema.table_constraints" -> tableConstraintRows(context);
      case "information_schema.key_column_usage" -> keyColumnRows(context);
      case "pg_inherits" -> inheritanceRows(context);
      default -> familyRows(relation, context);
    };
  }

  /** The rows of a relation that belongs to a family, read off the rows of that family; none for one that belongs to none. */
  private static List<Map<String, Object>> familyRows(final String relation, final Context context) {
    final Family family = FAMILIES.get(relation);
    if (family == null)
      return List.of();
    final List<Row> rows = context.rows(family);
    final List<Map<String, Object>> maps = new ArrayList<>(rows.size());
    for (final Row row : rows)
      maps.add(row.relations.get(relation));
    return maps;
  }

  /**
   * {@code pg_class}: a row per type and then one per index, which PostgreSQL keeps in the same relation under their own kind
   * (issue #10473). An index row carries the access method of the index as its {@code pg_am}.
   */
  private static List<Row> classRows(final Context context) {
    final List<Row> rows = new ArrayList<>(tableRows(context));
    for (final PostgresCatalogIndexes.Info index : context.indexes())
      rows.add(indexRow(context, index).complete());
    return rows;
  }

  /** The row of an index as a relation: the schema it lives in and its owner, its {@code pg_class} and its access method. */
  private static Row indexRow(final Context context, final PostgresCatalogIndexes.Info index) {
    return singletonRow(context)//
        .with("pg_class", PostgresCatalogIndexes.classValues(index, SCHEMA_OID, OWNER_OID))//
        .with("pg_am", PostgresCatalogIndexes.accessMethodValues(index.method()));
  }

  /**
   * {@code pg_attribute}: a row per column of a type and then one per key column of an index, numbered from 1 in each index
   * (issue #10473). An index column carries the {@code pg_type} of the column it indexes, and no {@code information_schema} row:
   * that view lists the columns of tables.
   */
  private static List<Row> attributeRows(final Context context) {
    final List<Row> rows = new ArrayList<>(columnRows(context));
    for (final PostgresCatalogIndexes.Info index : context.indexes()) {
      final List<Property> properties = sortedProperties(index.type());
      for (int k = 0; k < index.columns().length; k++) {
        final int attnum = index.attnums()[k];
        if (attnum == 0)
          // a key that is no column of the type has no attribute to describe
          continue;
        final Property property = properties.get(attnum - 1);
        final PostgresType pgType = PostgresType.getTypeFromArcade(property.getType(), property.getOfType());
        rows.add(describeType(indexRow(context, index)//
            .with("pg_attribute", attributeValues(index.oid(), property.getName(), k + 1, pgType, false, false)), pgType)
            .complete());
      }
    }
    return rows;
  }

  /**
   * {@code pg_namespace} as a join reads it: the catalog the types this protocol produces live in, which {@code pg_type.typnamespace}
   * names, and the database's own schema. The other statements read the one row of the schema; a join reads the schemas a type can
   * be in, so that {@code t.typnamespace = n.oid} finds the schema of every type a driver looks up (issue #10495).
   */
  private static List<Map<String, Object>> namespaceRows(final Context context) {
    return List.of(
        completeMap("pg_namespace", "oid", PostgresTypeCatalog.PG_CATALOG_OID, "nspname", PG_CATALOG_SCHEMA, "nspowner", OWNER_OID),
        completeMap("pg_namespace", "oid", SCHEMA_OID, "nspname", context.schema(), "nspowner", OWNER_OID));
  }

  /** {@code pg_am}: the heap every type is stored in, and the families of index. */
  private static List<Map<String, Object>> accessMethodRows() {
    final List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(completeMap("pg_am", "oid", HEAP_ACCESS_METHOD_OID, "amname", "heap", "amhandler", "heap_tableam_handler", "amtype",
        "t"));
    for (final PostgresCatalogIndexes.AccessMethod method : PostgresCatalogIndexes.ACCESS_METHODS)
      rows.add(completeMap("pg_am", PostgresCatalogIndexes.accessMethodValues(method)));
    return rows;
  }

  private static List<Map<String, Object>> indexRows(final Context context) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (final PostgresCatalogIndexes.Info index : context.indexes())
      rows.add(completeMap("pg_index", PostgresCatalogIndexes.indexValues(index)));
    return rows;
  }

  /** {@code pg_indexes}: the view of the indexes that names each with the statement that creates it. */
  private static List<Map<String, Object>> indexListRows(final Context context) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (final PostgresCatalogIndexes.Info index : context.indexes())
      rows.add(completeMap("pg_indexes", "schemaname", context.schema(), "tablename", index.type().getName(), "indexname",
          index.name(), "indexdef", PostgresCatalogIndexes.indexDefinition(index, context.schema(), 0, false)));
    return rows;
  }

  private static List<Map<String, Object>> constraintRows(final Context context) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (final PostgresCatalogIndexes.Info index : context.indexes())
      if (index.hasConstraint())
        rows.add(completeMap("pg_constraint", PostgresCatalogIndexes.constraintValues(index, SCHEMA_OID)));
    return rows;
  }

  private static List<Map<String, Object>> tableConstraintRows(final Context context) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (final PostgresCatalogIndexes.Info index : context.indexes())
      if (index.hasConstraint())
        rows.add(completeMap("information_schema.table_constraints",
            PostgresCatalogIndexes.tableConstraintValues(index, context.schema())));
    return rows;
  }

  private static List<Map<String, Object>> keyColumnRows(final Context context) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (final PostgresCatalogIndexes.Info index : context.indexes())
      if (index.hasConstraint())
        for (int position = 1; position <= index.columns().length; position++)
          rows.add(completeMap("information_schema.key_column_usage",
              PostgresCatalogIndexes.keyColumnValues(index, context.schema(), position)));
    return rows;
  }

  /** {@code pg_inherits}: one row per super-type of a type, numbered in the order they are declared. */
  private static List<Map<String, Object>> inheritanceRows(final Context context) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (final DocumentType type : sortedTypes(context)) {
      int sequence = 0;
      for (final DocumentType parent : type.getSuperTypes())
        rows.add(completeMap("pg_inherits", "inhrelid", oidOf(type.getName()), "inhparent", oidOf(parent.getName()), "inhseqno",
            ++sequence, "inhdetachpending", Boolean.FALSE));
    }
    return rows;
  }

  /**
   * information_schema.columns.numeric_precision_radix: 2 for the binary/approximate numeric types PostgreSQL
   * itself reports it for, but 10 for NUMERIC specifically - it is the one numeric type PostgreSQL stores and
   * reports precision in base 10 rather than base 2 (issue #6447). NULL for a type that is not a number, where
   * every native scalar type used to answer 2 (issue #10370).
   */
  private static Integer numericPrecisionRadix(final PostgresType pgType) {
    return switch (pgType) {
      case SMALLINT, INTEGER, LONG, REAL, DOUBLE -> 2;
      case NUMERIC -> 10;
      // PostgreSQL's _pg_numeric_precision_radix is NULL for every type that is not a number (issue #10370)
      default -> null;
    };
  }

  /** information_schema.columns.numeric_precision of a column declared bare: the bits of the number, NULL for a NUMERIC. */
  private static Integer numericPrecision(final PostgresType pgType) {
    return switch (pgType) {
      case SMALLINT -> 16;
      case INTEGER -> 32;
      case LONG -> 64;
      case REAL -> 24;
      case DOUBLE -> 53;
      default -> null;
    };
  }

  /** information_schema.columns.numeric_scale: 0 for the integers, NULL for the approximate types and a bare NUMERIC. */
  private static Integer numericScale(final PostgresType pgType) {
    return switch (pgType) {
      case SMALLINT, INTEGER, LONG -> 0;
      default -> null;
    };
  }

  /** information_schema.columns.datetime_precision: a DATE has no fraction, a TIMESTAMP declared bare keeps microseconds. */
  private static Integer datetimePrecision(final PostgresType pgType) {
    return switch (pgType) {
      case DATE -> 0;
      case TIMESTAMP -> 6;
      default -> null;
    };
  }

  /** information_schema.columns.character_octet_length of a text column with no declared length: 2^30, PostgreSQL's own. */
  private static Integer characterOctetLength(final PostgresType pgType) {
    return switch (pgType) {
      case TEXT, VARCHAR, BPCHAR -> 1 << 30;
      default -> null;
    };
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

    return finish(statement, projection, surviving, from, context, columnsByRelation, toleratesUnreadableFilter, outer, false);
  }

  /**
   * The rest of a statement once its rows are chosen: window functions, grouping, the projection, DISTINCT, ORDER BY and LIMIT.
   * Shared by the statements that read one row per entity and the ones that join (issue #10473).
   *
   * @param neverHasRows whether the statement reads a relation that has no rows in this catalog, so that no row is projected
   *                     whatever the projection holds, and a column this catalog cannot compute is of no consequence
   */
  private static Answer finish(final Statement statement, final Projection projection, final List<Joined> surviving,
      final List<FromEntry> from, final Context context, final Map<String, Collection<String>> columnsByRelation,
      final boolean toleratesUnreadableFilter, final RowResolver outer, final boolean neverHasRows) {
    // Window functions are the one thing that cannot be computed a row at a time: row_number() is defined by
    // the other rows of its partition. They are computed here, over the rows that survived the filter.
    final Map<PostgresCatalogExpression.WindowCall, Object[]> windows = computeWindows(projection.items, surviving,
        from, context, outer);
    if (windows == null)
      return DECLINED;

    final List<Map<String, Object>> answered;
    if (projection.aggregated || statement.groupBy != null) {
      // a set-returning call under a grouping would have to be expanded before the rows are grouped
      if (!windows.isEmpty() || !projection.expansions.isEmpty())
        return DECLINED;
      answered = aggregate(statement, projection, surviving, from, context, columnsByRelation, outer);
      if (answered == null)
        return DECLINED;
    } else {
      answered = new ArrayList<>(surviving.size());
      for (int i = 0; i < surviving.size(); i++) {
        final Joined joined = surviving.get(i);
        final RowResolver resolver = new RowResolver(joined.row(), joined.nulled(), from, context, outer).withWindows(windows, i);

        // A set-returning call turns the row into one for each element of its array, the calls of the row in step: the row
        // is projected as many times as the longest of them says, none for an array that has no element
        int projections = 1;
        if (!projection.expansions.isEmpty()) {
          projections = 0;
          for (final PostgresCatalogExpression.ExpandArray expansion : projection.expansions) {
            final int rows = expansion.rows(resolver);
            if (rows < 0)
              return DECLINED;
            projections = Math.max(projections, rows);
          }
        }

        for (int position = 0; position < projections; position++) {
          final Map<String, Object> projected = projectRow(projection, resolver.withExpansion(position));
          if (projected == null)
            // A projected column this catalog cannot compute: answering the rest of the row would be answering
            // a different question from the one asked.
            return DECLINED;
          answered.add(projected);
        }
      }

      // With no rows to project - an empty database, or a filter that matched nothing - the projection was
      // never evaluated, so a column this catalog cannot produce would go unnoticed and the client would be
      // told the query succeeded. Evaluate it once against an all-null row of the same shape to find out.
      if (answered.isEmpty() && !neverHasRows && projectRow(projection,
          new RowResolver(probeRow(from, columnsByRelation), null, from, context, outer).withWindows(windows, 0)
              .withExpansion(0)) == null)
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
      result = sort(statement.orderBy, result, projection);
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
          // an int2vector or oidvector travels as text, see textOfVectors
          final PostgresType sampled = value instanceof PostgresCatalogIndexes.Vector ? PostgresType.VARCHAR :
              PostgresType.getTypeForValue(value);
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
    return sort(orderBy, projected, null);
  }

  /**
   * {@link #sort(List, List)} that also reads an item written as a qualified column ({@code i.indisprimary DESC}) when the
   * projection carries that column, which is how psql orders the indexes of {@code \d} (issue #10473).
   */
  private static List<Map<String, Object>> sort(final List<PostgresCatalogToken> orderBy,
      final List<Map<String, Object>> projected, final Projection projection) {
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

      int index = -1;
      if (key.size() == 3 && key.get(1).isSymbol(".") && projection != null) {
        index = projectedColumn(projection, key.get(0).text.toLowerCase(Locale.ENGLISH), key.get(2).text.toLowerCase(Locale.ENGLISH));
        if (index < 0)
          return projected;
        keys.add(index);
        descending.add(desc);
        continue;
      }
      if (key.size() != 1)
        return projected;

      final PostgresCatalogToken token = key.get(0);
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

  /** The position of the projected column that is the plain column {@code qualifier.name}, -1 when the projection has none. */
  private static int projectedColumn(final Projection projection, final String qualifier, final String name) {
    for (int i = 0; i < projection.items.size(); i++)
      if (projection.items.get(i).expression instanceof PostgresCatalogExpression.ColumnReference reference
          && name.equalsIgnoreCase(reference.name) && qualifier.equalsIgnoreCase(reference.qualifier))
        return i;
    return -1;
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

  /**
   * A parsed projection, and whether it aggregates - which turns the rows it is evaluated over into groups.
   *
   * @param expansions the set-returning calls it holds, each of which turns a row into one for every element of its array
   */
  private record Projection(List<ProjectionItem> items, boolean aggregated,
                            List<PostgresCatalogExpression.ExpandArray> expansions) {
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
    final List<PostgresCatalogExpression.ExpandArray> expansions = new ArrayList<>();
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
      expansions.addAll(parser.expansions());

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

    return new Projection(projection, aggregated, expansions);
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
    /** Which row of its set-returning calls the projection is on, -1 when it is on none, see {@link #withExpansion}. */
    private int                                                 expansion = -1;
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

    /** The row being projected is the {@code position}th (from 0) a set-returning call of the projection produces. */
    RowResolver withExpansion(final int position) {
      this.expansion = position;
      return this;
    }

    @Override
    public int expansion() {
      return expansion;
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
        final Map<String, Object> columns = row.columnsOf(from, index);
        if (columns == null || !columns.containsKey(name) || row.isAmbiguous(columns, name))
          return PostgresCatalogExpression.UNKNOWN;
        return nulled != null && nulled[index] ? null : columns.get(name);
      }

      // Unqualified: the first relation in the FROM clause that has such a column, which is how PostgreSQL
      // resolves it too when only one of them does - and then the query around a sub-select.
      for (int i = 0; i < from.size(); i++) {
        final Map<String, Object> columns = row.columnsOf(from, i);
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
        case "current_schemas" -> currentSchemas(arguments, context);
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
        case "obj_description", "col_description", "shobj_description", "pg_get_viewdef" -> null;
        // The statement that creates an index, and the constraint a unique index is (issue #10473)
        case "pg_get_indexdef" -> indexDefinition(arguments);
        case "pg_get_constraintdef" -> constraintDefinition(arguments);
        default -> PostgresCatalogExpression.UNKNOWN;
      };
    }

    /**
     * {@code current_schemas(include_implicit)}: the schemas on the search path, which is the database's own and, when the argument
     * is true, the catalog every connection searches first: the types this protocol produces live there, and a driver asks whether
     * their schema is on the path to decide if it must qualify their names (pgjdbc's {@code n.nspname = ANY(current_schemas(true))}).
     * NULL for a NULL argument, as the function is strict.
     */
    private static Object currentSchemas(final List<Object> arguments, final Context context) {
      if (arguments.size() != 1 || arguments.get(0) == PostgresCatalogExpression.UNKNOWN)
        return PostgresCatalogExpression.UNKNOWN;
      if (arguments.get(0) == null)
        return null;
      return PostgresCatalogExpression.truth(arguments.get(0)) ? List.of(PG_CATALOG_SCHEMA, context.schema()) :
          List.of(context.schema());
    }

    /**
     * {@code pg_get_indexdef(index [, column, pretty])}: NULL for an index that does not exist, as PostgreSQL answers.
     */
    private Object indexDefinition(final List<Object> arguments) {
      if (arguments.isEmpty() || arguments.size() > 3 || arguments.contains(PostgresCatalogExpression.UNKNOWN))
        return PostgresCatalogExpression.UNKNOWN;
      if (!(arguments.get(0) instanceof Number oid))
        return null;
      final PostgresCatalogIndexes.Info index = context.indexWithOid(oid.intValue());
      if (index == null)
        return null;
      final int column = arguments.size() > 1 && arguments.get(1) instanceof Number number ? number.intValue() : 0;
      final boolean pretty = arguments.size() > 2 && PostgresCatalogExpression.truth(arguments.get(2));
      return PostgresCatalogIndexes.indexDefinition(index, context.schema(), column, pretty);
    }

    /** {@code pg_get_constraintdef(constraint [, pretty])}: NULL for a constraint that does not exist. */
    private Object constraintDefinition(final List<Object> arguments) {
      if (arguments.isEmpty() || arguments.size() > 2 || arguments.contains(PostgresCatalogExpression.UNKNOWN))
        return PostgresCatalogExpression.UNKNOWN;
      if (!(arguments.get(0) instanceof Number oid))
        return null;
      final PostgresCatalogIndexes.Info index = context.constraintWithOid(oid.intValue());
      return index == null ? null : PostgresCatalogIndexes.constraintDefinition(index);
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
      return type.sqlName();
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
      return type == null ? null : type.sqlName();
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
