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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the shape-based catalog emulation of issue #6412, over the queries the PostgreSQL JDBC
 * driver actually sends (which is what every JDBC-based tool sends) plus the hand-written spellings that
 * used to be matched by string equality.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Article").createProperty("id", Type.INTEGER);
      database.getSchema().getType("Article").createProperty("title", Type.STRING).setMandatory(true);
      database.getSchema().createDocumentType("Author").createProperty("name", Type.STRING);
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- the driver's own queries

  /** {@code DatabaseMetaData.getSchemas()} as pgjdbc writes it. */
  private static final String JDBC_GET_SCHEMAS =
      "SELECT nspname AS \"TABLE_SCHEM\", current_database() AS \"TABLE_CATALOG\" FROM pg_catalog.pg_namespace "
          + " WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_' "
          + " OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_' "
          + " OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_')) "
          + " ORDER BY \"TABLE_SCHEM\"";

  /** {@code DatabaseMetaData.getTables()} as pgjdbc writes it, trimmed of the parts it only adds on demand. */
  private static final String JDBC_GET_TABLES =
      "SELECT current_database() AS \"TABLE_CAT\", n.nspname AS \"TABLE_SCHEM\", c.relname AS \"TABLE_NAME\", "
          + " CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' "
          + " WHEN true THEN CASE "
          + " WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind "
          + "  WHEN 'r' THEN 'SYSTEM TABLE' "
          + "  WHEN 'v' THEN 'SYSTEM VIEW' "
          + "  WHEN 'i' THEN 'SYSTEM INDEX' "
          + "  ELSE NULL "
          + "  END "
          + " WHEN n.nspname = 'pg_toast' THEN CASE c.relkind "
          + "  WHEN 'r' THEN 'SYSTEM TOAST TABLE' "
          + "  WHEN 'i' THEN 'SYSTEM TOAST INDEX' "
          + "  ELSE NULL "
          + "  END "
          + " ELSE CASE c.relkind "
          + "  WHEN 'r' THEN 'TEMPORARY TABLE' "
          + "  WHEN 'p' THEN 'TEMPORARY TABLE' "
          + "  WHEN 'i' THEN 'TEMPORARY INDEX' "
          + "  WHEN 'S' THEN 'TEMPORARY SEQUENCE' "
          + "  WHEN 'v' THEN 'TEMPORARY VIEW' "
          + "  ELSE NULL "
          + "  END "
          + " END "
          + " WHEN false THEN CASE c.relkind "
          + " WHEN 'r' THEN 'TABLE' "
          + " WHEN 'p' THEN 'PARTITIONED TABLE' "
          + " WHEN 'i' THEN 'INDEX' "
          + " WHEN 'P' then 'PARTITIONED INDEX' "
          + " WHEN 'S' THEN 'SEQUENCE' "
          + " WHEN 'v' THEN 'VIEW' "
          + " WHEN 'c' THEN 'TYPE' "
          + " WHEN 'f' THEN 'FOREIGN TABLE' "
          + " WHEN 'm' THEN 'MATERIALIZED VIEW' "
          + " ELSE NULL "
          + " END "
          + " ELSE NULL "
          + " END "
          + " AS \"TABLE_TYPE\", d.description AS \"REMARKS\", "
          + " '' as \"TYPE_CAT\", '' as \"TYPE_SCHEM\", '' as \"TYPE_NAME\", "
          + "'' AS \"SELF_REFERENCING_COL_NAME\", '' AS \"REF_GENERATION\" "
          + " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c "
          + " LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass) "
          + " WHERE c.relnamespace = n.oid  AND c.relname LIKE $1 AND (false  OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) ) "
          + " ORDER BY \"TABLE_TYPE\",\"TABLE_SCHEM\",\"TABLE_NAME\" ";

  /** {@code DatabaseMetaData.getColumns()} as pgjdbc writes it: a derived table with a window function in it. */
  private static final String JDBC_GET_COLUMNS =
      "SELECT * FROM (SELECT current_database() AS current_database, n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull"
          + " OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,"
          + "row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, "
          + "nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,"
          + "pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype "
          + " FROM pg_catalog.pg_namespace n "
          + " JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) "
          + " JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) "
          + " JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) "
          + " LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) "
          + " LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) "
          + " LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') "
          + " LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') "
          + " WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped  AND c.relname LIKE $1) c "
          + " WHERE true  AND attname LIKE $2 ORDER BY nspname,c.relname,attnum ";

  @Test
  void theDriversSchemaListNamesTheConnectedDatabase() {
    final PostgresCatalog.Answer answer = resolve(JDBC_GET_SCHEMAS);

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0)).containsEntry("TABLE_SCHEM", database.getName());
    assertThat(answer.rows.get(0)).containsEntry("TABLE_CATALOG", database.getName());
    // The projection order is the client's, because a DataRow is read positionally.
    assertThat(answer.columns.keySet()).containsExactly("TABLE_SCHEM", "TABLE_CATALOG");
  }

  @Test
  void theDriversTableListNamesEveryType() {
    final PostgresCatalog.Answer answer = resolve(JDBC_GET_TABLES, "%");

    assertThat(names(answer.rows, "TABLE_NAME")).containsExactly("Article", "Author");
    // Produced by the driver's own CASE over relkind, not by this catalog naming it.
    assertThat(answer.rows.get(0)).containsEntry("TABLE_TYPE", "TABLE");
    assertThat(answer.rows.get(0)).containsEntry("TABLE_SCHEM", database.getName());
    assertThat(answer.rows.get(0)).containsEntry("REMARKS", null);
  }

  @Test
  void theDriversTableListAppliesTheNamePatternItBoundAsAParameter() {
    assertThat(names(resolve(JDBC_GET_TABLES, "Article").rows, "TABLE_NAME")).containsExactly("Article");
    assertThat(names(resolve(JDBC_GET_TABLES, "A%").rows, "TABLE_NAME")).containsExactly("Article", "Author");
    assertThat(resolve(JDBC_GET_TABLES, "Missing").rows).isEmpty();
  }

  @Test
  void theDriversColumnListDescribesOneTypesProperties() {
    final PostgresCatalog.Answer answer = resolve(JDBC_GET_COLUMNS, "Article", "%");

    assertThat(names(answer.rows, "attname")).containsExactly("id", "title");
    assertThat(answer.rows.get(0)).containsEntry("relname", "Article");
    assertThat(answer.rows.get(0)).containsEntry("atttypid", PostgresType.INTEGER.code);
    // row_number() over the partition: the ordinal position within its own table, counted from 1.
    assertThat(answer.rows.get(0)).containsEntry("attnum", 1L);
    assertThat(answer.rows.get(1)).containsEntry("attnum", 2L);
    // "title" is mandatory, which is the closest thing ArcadeDB has to NOT NULL.
    assertThat(answer.rows.get(1)).containsEntry("attnotnull", Boolean.TRUE);
    assertThat(answer.rows.get(0)).containsEntry("attnotnull", Boolean.FALSE);
  }

  @Test
  void theDriversColumnListAppliesTheColumnPattern() {
    final PostgresCatalog.Answer answer = resolve(JDBC_GET_COLUMNS, "%", "id");
    assertThat(names(answer.rows, "attname")).containsExactly("id");
  }

  // ---------------------------------------------------------------- hand-written spellings

  @Test
  void theSchemaListWithCaseExpressionsIsAnswered() {
    final PostgresCatalog.Answer answer = resolve(
        "select NSPNAME as SCHEMA_NAME, case when lower(NSPNAME)='pg_catalog' then 'Y' else 'N' end as IS_PUBLIC, "
            + "case when lower(NSPNAME)='information_schema' then 'Y' else 'N' end as IS_SYSTEM, 'N' as IS_EMPTY "
            + "from PG_CATALOG.PG_NAMESPACE order by NSPNAME asc");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0)).containsEntry("schema_name", database.getName());
    assertThat(answer.rows.get(0)).containsEntry("is_public", "N");
    assertThat(answer.rows.get(0)).containsEntry("is_system", "N");
  }

  @Test
  void theUserListIsAnswered() {
    final PostgresCatalog.Answer answer = resolve(
        "select distinct GRANTEE as USER_NAME, 'N' as IS_EXPIRED, 'N' as IS_LOCKED "
            + "from INFORMATION_SCHEMA.USAGE_PRIVILEGES order by GRANTEE asc");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0)).containsEntry("user_name", USER);
  }

  @Test
  void theDistinctPrivilegeListIsAnswered() {
    // This one used to be on an ignore-list, so it came back with no rows at all for every client.
    final PostgresCatalog.Answer answer = resolve(
        "select distinct PRIVILEGE_TYPE as PRIVILEGE_NAME from INFORMATION_SCHEMA.USAGE_PRIVILEGES order by PRIVILEGE_TYPE asc");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0)).containsEntry("privilege_name", "USAGE");
  }

  @Test
  void theCharacterSetListIsAnswered() {
    final PostgresCatalog.Answer answer = resolve(
        "select CHARACTER_SET_NAME as CHARSET_NAME, -1 as MAX_LENGTH from INFORMATION_SCHEMA.CHARACTER_SETS order by CHARACTER_SET_NAME asc");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0)).containsEntry("charset_name", "UTF8");
    assertThat(answer.rows.get(0)).containsEntry("max_length", -1L);
  }

  @Test
  void theInformationSchemaTableListIsAnswered() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '" + database.getName()
            + "' ORDER BY table_name");

    assertThat(names(answer.rows, "table_name")).containsExactly("Article", "Author");
    assertThat(answer.rows.get(0)).containsEntry("table_type", "BASE TABLE");
  }

  @Test
  void theInformationSchemaColumnListIsAnswered() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            + "WHERE table_name = 'Article' ORDER BY ordinal_position");

    assertThat(names(answer.rows, "column_name")).containsExactly("id", "title");
    assertThat(answer.rows.get(0)).containsEntry("data_type", "int4");
    assertThat(answer.rows.get(0)).containsEntry("is_nullable", "YES");
    assertThat(answer.rows.get(1)).containsEntry("is_nullable", "NO");
  }

  @Test
  void aDecimalColumnReportsBase10PrecisionRadix() {
    // issue #6447: DECIMAL used to map to DOUBLE, so the flat "every native scalar type answers 2" here was
    // accidentally correct for it. Now that it maps to NUMERIC, real PostgreSQL reports 10 for that type
    // specifically - every other native scalar type this catalog emulates is still binary (base 2).
    database.transaction(() -> database.getSchema().createDocumentType("Invoice6447").createProperty("amount", Type.DECIMAL));

    final PostgresCatalog.Answer answer = resolve(
        "SELECT column_name, numeric_precision_radix FROM information_schema.columns WHERE table_name = 'Invoice6447'");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0)).containsEntry("numeric_precision_radix", 10);
  }

  @Test
  void aStarProjectionExpandsToTheRelationsColumns() {
    final PostgresCatalog.Answer answer = resolve("SELECT * FROM pg_catalog.pg_namespace");

    assertThat(answer.columns.keySet()).contains("oid", "nspname", "nspowner", "nspacl");
    assertThat(answer.rows.get(0)).containsEntry("nspname", database.getName());
  }

  // ---------------------------------------------------------------- what is not answered

  @Test
  void aQueryAboutARelationThisCatalogDoesNotModelIsDeclined() {
    assertThat(resolveRaw("SELECT indexrelid FROM pg_catalog.pg_index")).isSameAs(PostgresCatalog.DECLINED);
  }

  @Test
  void aProjectionThisCatalogCannotComputeIsDeclinedRatherThanAnsweredWithHoles() {
    // A row for every table with a made-up size in it would be worse than no answer, because the client
    // believes what a system catalog tells it.
    assertThat(resolveRaw("SELECT relname, pg_total_relation_size(c.oid) FROM pg_class c")).isSameAs(
        PostgresCatalog.DECLINED);
  }

  @Test
  void anOrdinaryQueryIsNotACatalogQuery() {
    assertThat(resolveRaw("SELECT name FROM Author")).isNull();
    assertThat(resolveRaw("SELECT 1")).isNull();
  }

  @Test
  void aUserTypeNamedLikeACatalogRelationIsNotACatalogQuery() {
    database.transaction(() -> database.getSchema().createDocumentType("pg_notes"));
    assertThat(resolveRaw("SELECT note FROM pg_notes")).isNull();
  }

  @Test
  void anUnreadablePredicateDoesNotEmptyTheAnswer() {
    // has_table_privilege() is not modelled. Read strictly, the whole table list would come back empty; the
    // rows it could exclude are ones this catalog never produces.
    final PostgresCatalog.Answer answer = resolve(
        "SELECT relname FROM pg_class c WHERE has_table_privilege(c.oid, 'SELECT') AND relname LIKE 'A%'");

    assertThat(names(answer.rows, "relname")).containsExactly("Article", "Author");
  }

  @Test
  void aReadablePredicateStillFiltersWhenAnUnreadableOneIsBesideIt() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT relname FROM pg_class c WHERE has_table_privilege(c.oid, 'SELECT') AND relname = 'Author'");

    assertThat(names(answer.rows, "relname")).containsExactly("Author");
  }

  @Test
  void aWindowNumbersItsPartitionInTheDirectionTheClientAskedFor() {
    // The same shape the JDBC driver's column list uses, but ordered the other way round: the numbering has
    // to follow, or the client is handed numbers that contradict its own ORDER BY.
    final PostgresCatalog.Answer ascending = resolve(
        "SELECT a.attname, row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS n "
            + "FROM pg_class c, pg_attribute a WHERE c.relname = 'Article'");
    assertThat(names(ascending.rows, "attname")).containsExactly("id", "title");
    assertThat(names(ascending.rows, "n")).containsExactly(1L, 2L);

    final PostgresCatalog.Answer descending = resolve(
        "SELECT a.attname, row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum DESC) AS n "
            + "FROM pg_class c, pg_attribute a WHERE c.relname = 'Article'");
    assertThat(names(descending.rows, "attname")).containsExactly("id", "title");
    assertThat(names(descending.rows, "n")).containsExactly(2L, 1L);
  }

  @Test
  void anUnAliasedWindowColumnIsNamedAfterItsFunction() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT row_number() OVER (ORDER BY relname) FROM pg_class");

    assertThat(answer.columns.keySet()).containsExactly("row_number");
  }

  // ---------------------------------------------------------------- the remaining families and clauses

  @Test
  void theDatabaseListNamesOnlyTheConnectedDatabase() {
    // Enumerating the server's databases is not something an emulated catalog should hand out, and a
    // PostgreSQL connection is bound to one database anyway.
    final PostgresCatalog.Answer answer = resolve("SELECT datname, encoding FROM pg_database");

    assertThat(names(answer.rows, "datname")).containsExactly(database.getName());
    assertThat(answer.rows.get(0)).containsEntry("encoding", 6);
  }

  @Test
  void theRoleListNamesOnlyTheConnectedUser() {
    assertThat(names(resolve("SELECT rolname FROM pg_roles").rows, "rolname")).containsExactly(USER);
    assertThat(names(resolve("SELECT usename FROM pg_user").rows, "usename")).containsExactly(USER);
  }

  @Test
  void theCollationListIsAnswered() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT COLLATION_SCHEMA, COLLATION_NAME FROM INFORMATION_SCHEMA.COLLATIONS");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0)).containsEntry("collation_name", "default");
  }

  @Test
  void theViewListIsEmptyBecauseThereAreNoViewsToReport() {
    // An empty answer is a real answer, and it still describes its own columns.
    final PostgresCatalog.Answer answer = resolve("SELECT viewname FROM pg_views");

    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns.keySet()).containsExactly("viewname");
  }

  @Test
  void aQualifiedStarExpandsToThatRelationsColumns() {
    final PostgresCatalog.Answer answer = resolve("SELECT n.* FROM pg_catalog.pg_namespace n");

    assertThat(answer.columns.keySet()).containsExactly("oid", "nspname", "nspowner", "nspacl");
  }

  @Test
  void orderByFollowsTheClientsDirectionAndOrdinal() {
    assertThat(names(resolve("SELECT relname FROM pg_class ORDER BY relname DESC").rows, "relname"))
        .containsExactly("Author", "Article");
    assertThat(names(resolve("SELECT relname FROM pg_class ORDER BY 1 DESC").rows, "relname"))
        .containsExactly("Author", "Article");
    // An ORDER BY this catalog cannot resolve leaves the rows in the order it built them, by name, rather
    // than declining a query whose data is perfectly answerable.
    assertThat(names(resolve("SELECT relname FROM pg_class ORDER BY pg_relation_size(oid)").rows, "relname"))
        .containsExactly("Article", "Author");
  }

  @Test
  void limitTruncatesTheAnswer() {
    assertThat(resolve("SELECT relname FROM pg_class ORDER BY relname LIMIT 1").rows).hasSize(1);
  }

  @Test
  void distinctCollapsesRowsThatProjectTheSameValues() {
    // One row per type, but the projection keeps only the schema they share.
    final PostgresCatalog.Answer answer = resolve("SELECT DISTINCT nspname FROM pg_namespace n, pg_class c");

    assertThat(answer.rows).hasSize(1);
  }

  @Test
  void aSetOperationOrAGroupByIsDeclined() {
    // Both change what the rows are, and answering the half this catalog understands would be answering a
    // different question.
    assertThat(resolveRaw("SELECT relname FROM pg_class UNION SELECT nspname FROM pg_namespace")).isSameAs(
        PostgresCatalog.DECLINED);
    assertThat(resolveRaw("SELECT relkind FROM pg_class GROUP BY relkind")).isSameAs(PostgresCatalog.DECLINED);
    assertThat(resolveRaw("SELECT relname FROM pg_class LIMIT ALL")).isSameAs(PostgresCatalog.DECLINED);
  }

  @Test
  void aStatementThatIsNotASelectIsNotAnsweredButIsNotSwallowedEither() {
    // A DDL statement naming an emulated relation is still not this class's business, but one naming a user
    // type - even a type called pg_something - must reach the SQL engine untouched.
    assertThat(resolveRaw("INSERT INTO pg_class SET relname = 'x'")).isSameAs(PostgresCatalog.DECLINED);
    assertThat(resolveRaw("CREATE DOCUMENT TYPE pg_notes")).isNull();
  }

  @Test
  void aBoundParameterWithNoLiteralFormLeavesItsPredicateUnread() {
    // A null parameter cannot be written inline, so the predicate around it is not read - and an unreadable
    // predicate does not remove rows.
    final PostgresCatalog.Answer answer = resolveRaw("SELECT relname FROM pg_class WHERE relname LIKE $1",
        (Object) null);

    assertThat(names(answer.rows, "relname")).containsExactly("Article", "Author");
  }

  @Test
  void theColumnsOfAnAnswerAreTypedFromItsValues() {
    final PostgresCatalog.Answer answer = resolve("SELECT relname, oid, relhasindex FROM pg_class");

    assertThat(answer.columns.get("relname")).isEqualTo(PostgresType.VARCHAR);
    assertThat(answer.columns.get("oid")).isEqualTo(PostgresType.INTEGER);
    assertThat(answer.columns.get("relhasindex")).isEqualTo(PostgresType.BOOLEAN);
  }

  @Test
  void anExpressionWithNoAliasIsAnnouncedTheWayPostgresWouldNameIt() {
    final PostgresCatalog.Answer answer = resolve("SELECT relname, current_database(), 1 + 1 FROM pg_class");

    assertThat(answer.columns.keySet()).containsExactly("relname", "current_database", "?column?");
  }

  // ---------------------------------------------------------------- pg_type as a relation of its own

  /**
   * The Apache Arrow ADBC driver's type-resolver bootstrap, verbatim from arrow-adbc
   * {@code c/driver/postgresql/database.cc}. Its WHERE clause is a conjunction of three inequalities, one of
   * them behind a cast and one of them parenthesised - which is why answering it belongs to the expression
   * evaluator rather than to {@link PostgresTypeCatalog}'s single-equality patterns (issue #7178).
   */
  private static final String ADBC_TYPE_RESOLVER_QUERY =
      "SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM "
          + "pg_catalog.pg_type WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r' AND "
          + "typreceive::TEXT != 'array_recv'";

  @Test
  void theArrowAdbcTypeResolverQueryIsAnswered() {
    final PostgresCatalog.Answer answer = resolve(ADBC_TYPE_RESOLVER_QUERY);

    assertThat(answer.columns.keySet())
        .as("the driver validates the column count before it looks at a single row")
        .containsExactly("oid", "typname", "typreceive", "typbasetype", "typrelid", "typarray");
    assertThat(answer.rows).isNotEmpty();

    assertThat(names(answer.rows, "typname")).contains("int4", "text", "bool", "timestamp", "numeric")
        .as("typreceive::TEXT != 'array_recv' filters the array types out").doesNotContain("_int4", "_text");

    final Map<String, Object> int4 = answer.rows.stream().filter(row -> "int4".equals(row.get("typname")))
        .findFirst().orElseThrow();
    assertThat(int4.get("oid")).isEqualTo(PostgresType.INTEGER.code);
    assertThat(int4.get("typreceive")).isEqualTo("int4recv");
    assertThat(int4.get("typarray")).isEqualTo(PostgresType.ARRAY_INT.code);
    assertThat(int4.get("typbasetype")).isEqualTo(0);
    assertThat(int4.get("typrelid")).isEqualTo(0);
  }

  @Test
  void aPlainEnumerationOfPgTypeListsEveryTypeInOidOrder() {
    final PostgresCatalog.Answer answer = resolve("SELECT oid, typname FROM pg_catalog.pg_type ORDER BY oid");

    assertThat(answer.rows).hasSize(PostgresType.values().length);
    assertThat(names(answer.rows, "oid")).isSorted();
  }

  @Test
  void aFilteredLookupInPgTypeSelectsOneRow() {
    final PostgresCatalog.Answer answer = resolve("SELECT typname, typlen FROM pg_type WHERE typcategory = 'N' AND typlen = 8");

    assertThat(names(answer.rows, "typname")).containsExactlyInAnyOrder("int8", "float8");
  }

  @Test
  void aColumnListJoiningPgTypeStillAnswersOneRowPerColumn() {
    // pg_type carrying a family of its own must not turn the driver's column list - which joins pg_attribute
    // to pg_type to name each column's type - into an enumeration of the type catalog.
    final PostgresCatalog.Answer answer = resolve(JDBC_GET_COLUMNS, "Article", "%");

    assertThat(names(answer.rows, "attname")).containsExactly("id", "title");
    assertThat(names(answer.rows, "typtype")).containsOnly("b");
  }

  @Test
  void aPgTypeFilterThisCatalogCannotReadDeclinesRatherThanAnsweringEveryType() {
    // The permissive-WHERE rule - an unreadable predicate does not get to remove rows - is right for every
    // other relation here, because their rows are the user's own types and such a predicate can only be
    // excluding rows this catalog never produced. pg_type is a fixed set of built-in types where a filter
    // selects a real subset, so keeping every row answers "all 23 types" to "the one named hstore", and
    // psycopg fails outright with "found 23 different types named hstore" rather than concluding it is
    // absent. An unanswerable filter over pg_type has to decline.
    assertThat(resolveRaw("SELECT typname, oid FROM pg_type t WHERE t.typname = pg_catalog.quote_ident('hstore')"))
        .isSameAs(PostgresCatalog.DECLINED);
  }

  @Test
  void aPgTypeFilterThatWillNotEvenParseDeclinesToo() {
    // Parsing and evaluating are two ways to fail to read the same predicate, and only the second used to
    // decline: a WHERE written with a construct the expression parser does not implement - a scalar
    // sub-query here - left the parse result null, which the caller read as "no filter" and answered every
    // type with. The two have to be treated alike (issue #7180).
    assertThat(resolveRaw("SELECT typname FROM pg_type t WHERE t.oid = (SELECT 1 FROM pg_type LIMIT 1)"))
        .isSameAs(PostgresCatalog.DECLINED);

    // The same shape over a relation whose rows are the user's own types is still answered permissively.
    assertThat(names(resolve("SELECT relname FROM pg_class WHERE oid = (SELECT 1 FROM pg_class LIMIT 1)").rows,
        "relname")).containsExactly("Article", "Author");
  }

  /**
   * psycopg's {@code TypeInfo.fetch}, verbatim from {@code psycopg/_typeinfo.py}, with its {@code %(name)s}
   * parameter bound.
   */
  private static final String PSYCOPG_TYPE_LOOKUP =
      "SELECT typname AS name, oid, typarray AS array_oid, oid::regtype::text AS regtype, typdelim AS delimiter "
          + "FROM pg_type t WHERE t.oid = to_regtype(%s) ORDER BY t.oid";

  @Test
  void psycopgsTypeLookupAnswersWhetherThisProtocolHasThatType() {
    // to_regtype() is the whole predicate of psycopg's lookup, so until it was read the query could only be
    // declined (issue #7180). Now it is answered the way PostgreSQL answers it: NULL for a type the server
    // does not have, which makes the equality false and the lookup correctly conclude "absent".
    assertThat(resolve(PSYCOPG_TYPE_LOOKUP.formatted("'hstore'::text")).rows).isEmpty();

    final PostgresCatalog.Answer present = resolve(PSYCOPG_TYPE_LOOKUP.formatted("'int4'::text"));
    assertThat(names(present.rows, "name")).containsExactly("int4");
    assertThat(present.rows.get(0)).containsEntry("regtype", "int4")
        .containsEntry("array_oid", PostgresType.ARRAY_INT.code);
  }

  @Test
  void wrappingPgTypeInASubSelectDoesNotGetThePermissiveFilterBack() {
    // The strictness has to travel with the rows, not with the statement that built them: the outer WHERE
    // reads the same closed set of types, so a sub-select must not become the way back to "an unreadable
    // predicate keeps every row" - which is the hole that answered psycopg with all 23 types.
    assertThat(resolveRaw("SELECT * FROM (SELECT oid, typname FROM pg_type) t "
        + "WHERE t.typname = pg_catalog.quote_ident('hstore')")).isSameAs(PostgresCatalog.DECLINED);

    // A readable outer filter over the same sub-select still answers, so the strictness has not become a
    // blanket refusal of derived tables over pg_type.
    assertThat(names(resolve("SELECT * FROM (SELECT oid, typname FROM pg_type) t WHERE t.typname = 'int4'").rows,
        "typname")).containsExactly("int4");
  }

  @Test
  void aPgTypeFilterThisCatalogCanReadStillSelectsASubset() {
    // Declining an unreadable filter must not become declining every filter: the readable ones still work,
    // and a name this protocol does not produce correctly answers no rows rather than being declined.
    assertThat(names(resolve("SELECT oid, typname FROM pg_type WHERE typname = 'hstore'").rows, "typname"))
        .as("a type this protocol cannot produce is absent, not unanswerable").isEmpty();
    assertThat(names(resolve("SELECT oid, typname FROM pg_type WHERE typname = 'int4'").rows, "typname"))
        .containsExactly("int4");
  }

  @Test
  void readingOneRelationThroughTwoAliasesIsDeclined() {
    // Every Row carries one map per relation, so two aliases of the same relation read the same map. The
    // second would answer with the first's values, and a projection naming a column through both announces
    // one column where the client asked for two - a miscount that makes a strict client abort. There is no
    // honest answer, so the query is declined rather than guessed at.
    assertThat(resolveRaw("SELECT t.typname, e.typname FROM pg_type t, pg_type e WHERE t.typelem = e.oid"))
        .isSameAs(PostgresCatalog.DECLINED);
    assertThat(resolveRaw("SELECT a.relname, b.relname FROM pg_class a, pg_class b WHERE a.oid = b.oid"))
        .isSameAs(PostgresCatalog.DECLINED);
  }

  @Test
  void theDriverSelfJoinIsStillAnsweredByTheTypeCatalog() {
    // The one self-join that matters is recognised by PostgresTypeCatalog, which runs before this class, so
    // declining the shape here does not take it away: the projected columns describe the element type of the
    // array the filter names, which is what the client's join asks for.
    final List<Map<String, Object>> rows = PostgresTypeCatalog
        .resolve("SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = 1009 AND t.typelem = e.oid");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsEntry("typname", "text").containsEntry("typdelim", ",");
  }

  @Test
  void aSecondAliasUsedOnlyInAJoinConditionDoesNotDeclineTheQuery() {
    // The driver's getColumns() joins pg_class and pg_namespace a second time purely to look up a comment it
    // never projects. Those aliases live only in ON conditions, which are skipped rather than evaluated, so
    // they must not count as reading the relation twice - declining here would take out the column list of
    // every JDBC client.
    final PostgresCatalog.Answer answer = resolve(JDBC_GET_COLUMNS, "Article", "%");

    assertThat(names(answer.rows, "attname")).containsExactly("id", "title");
  }

  // ---------------------------------------------------------------- the ::regclass cast (issue #7180)

  /**
   * {@code PostgresConnection::GetTableSchema} as the Apache Arrow native ADBC driver writes it
   * (arrow-adbc {@code c/driver/postgresql/connection.cc}, tag {@code apache-arrow-adbc-24}). It binds one
   * text parameter: the table name after {@code PQescapeIdentifier}, so the double quotes are part of it.
   */
  private static final String ADBC_GET_TABLE_SCHEMA =
      "SELECT attname, atttypid FROM pg_catalog.pg_class AS cls "
          + " INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
          + " INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
          + " WHERE attr.attnum >= 0 AND cls.oid = $1::regclass::oid ORDER BY attr.attnum";

  @Test
  void theDriversTableSchemaQueryDescribesTheTypeItsRegclassCastNames() {
    final PostgresCatalog.Answer answer = resolve(ADBC_GET_TABLE_SCHEMA, "\"Article\"");

    assertThat(names(answer.rows, "attname")).containsExactly("id", "title");
    assertThat(answer.rows.get(0).get("atttypid")).isEqualTo(PostgresType.INTEGER.code);
    assertThat(answer.rows.get(1).get("atttypid")).isEqualTo(PostgresType.VARCHAR.code);
  }

  @Test
  void anUnquotedRegclassNameStillFindsTheTypeThatIsSpelledWithCapitals() {
    // PostgreSQL folds an unquoted name to lower case; ArcadeDB type names are case-sensitive, so a name
    // that no type carries verbatim is matched ignoring case rather than answered with nothing.
    assertThat(names(resolve(ADBC_GET_TABLE_SCHEMA, "article").rows, "attname")).containsExactly("id", "title");
  }

  @Test
  void aSchemaQualifiedRegclassNameNamesTheSameType() {
    assertThat(names(resolve(ADBC_GET_TABLE_SCHEMA, "public.\"Article\"").rows, "attname"))
        .containsExactly("id", "title");
  }

  @Test
  void aRegclassNameNoTypeCarriesSelectsNoRowRatherThanEveryRow() {
    // The permissive-WHERE rule must not turn "describe the table that does not exist" into "describe every
    // column of every table": the cast is read, so the equality is false rather than unreadable.
    assertThat(resolve(ADBC_GET_TABLE_SCHEMA, "\"Missing\"").rows).isEmpty();
  }

  @Test
  void regclassOnAnOidRendersTheRelationNameTheWayPostgreSqlPrintsIt() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT DISTINCT attrelid::regclass FROM pg_catalog.pg_attribute ORDER BY 1");

    assertThat(answer.columns.keySet()).containsExactly("regclass");
    assertThat(names(answer.rows, "regclass")).containsExactly("Article", "Author");
  }

  @Test
  void regtypeNamesTheTypeOfAColumnAndResolvesATypeNameBackToItsOid() {
    final PostgresCatalog.Answer named = resolve(
        "SELECT attname, atttypid::regtype AS t FROM pg_catalog.pg_attribute WHERE attrelid = 'Author'::regclass");

    assertThat(names(named.rows, "t")).containsExactly("varchar");

    final PostgresCatalog.Answer filtered = resolve(
        "SELECT attname FROM pg_catalog.pg_attribute WHERE atttypid = 'int4'::regtype");

    assertThat(names(filtered.rows, "attname")).containsExactly("id");
  }

  @Test
  void aRegclassNameTwoTypesAnswerToOnlyByCaseIsNotGuessedAt() {
    database.transaction(() -> database.getSchema().createDocumentType("ARTICLE").createProperty("code", Type.STRING));

    // "Article" and "ARTICLE" both fold to "article", and picking either would describe the wrong table. The
    // exact spelling still resolves, because a quoted identifier means itself.
    assertThat(resolve(ADBC_GET_TABLE_SCHEMA, "article").rows).isEmpty();
    assertThat(names(resolve(ADBC_GET_TABLE_SCHEMA, "\"ARTICLE\"").rows, "attname")).containsExactly("code");
    assertThat(names(resolve(ADBC_GET_TABLE_SCHEMA, "\"Article\"").rows, "attname")).containsExactly("id", "title");
  }

  @Test
  void aRegclassCastOfANumericOidStringIsThatOid() {
    // PostgreSQL's regclass input accepts an OID spelled as digits, and the driver's parameter arrives as
    // text either way.
    final PostgresCatalog.Answer byName = resolve(ADBC_GET_TABLE_SCHEMA, "\"Author\"");
    final Object oid = resolve("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'Author'").rows.get(0).get("oid");

    assertThat(names(resolve(ADBC_GET_TABLE_SCHEMA, String.valueOf(oid)).rows, "attname"))
        .isEqualTo(names(byName.rows, "attname"));
  }

  // ---------------------------------------------------------------- helpers

  private PostgresCatalog.Answer resolve(final String query, final Object... parameters) {
    final PostgresCatalog.Answer answer = resolveRaw(query, parameters);
    assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
    assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
    return answer;
  }

  private PostgresCatalog.Answer resolveRaw(final String query, final Object... parameters) {
    return PostgresCatalog.resolve(query, database, USER, parameters);
  }

  private static List<Object> names(final List<Map<String, Object>> rows, final String column) {
    return rows.stream().map(row -> row.get(column)).toList();
  }
}
