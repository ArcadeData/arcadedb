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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10473: the catalog did not model {@code pg_index}, so {@code relhasindex} answered false for every type and the index
 * listings of psql ({@code \d <table>}, {@code \di}) and psqlodbc ({@code SQLPrimaryKeys}, {@code SQLStatistics}) were refused.
 * Modelling it needs a statement to bind two rows of one relation - psql reads {@code pg_class} as the table and again as each
 * of its indexes - which the catalog's one-row-per-entity model could not do.
 * <p>
 * Every psql statement below is the exact text psql 17 sent to this server, captured with {@code psql -E}; the expected answers
 * are what PostgreSQL 17 answers for the same objects (an index of ArcadeDB is a PostgreSQL index of the same name, and a unique
 * one is also the unique constraint of that name).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10473IndexCatalogTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10473IndexCatalogTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      schema.createDocumentType("Big").createProperty("id", Type.INTEGER);
      schema.getType("Big").createProperty("name", Type.STRING);
      schema.getType("Big").createProperty("v", Type.DOUBLE);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Big", "id");
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Big", "name", "v");
      schema.createDocumentType("Plain").createProperty("a", Type.STRING);
      schema.createDocumentType("Sub").addSuperType("Big");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- psql

  /** psql 17's index section of {@code \d "Big"}: the table read as {@code c}, each index as {@code c2}, the constraint it is. */
  @Test
  void psqlDescribesTheIndexesOfATable() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
          pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
        FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
          LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
        WHERE c.oid = '%d' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
        ORDER BY i.indisprimary DESC, c2.relname;
        """.formatted(oidOf("Big")));

    assertThat(column(answer, "relname")).containsExactly("Big[id]", "Big[name,v]");
    assertThat(column(answer, "indisprimary")).containsExactly(false, false);
    assertThat(column(answer, "indisunique")).containsExactly(true, false);
    assertThat(column(answer, "indisclustered")).containsExactly(false, false);
    assertThat(column(answer, "indisvalid")).containsExactly(true, true);
    assertThat(column(answer, "pg_get_indexdef")).containsExactly("CREATE UNIQUE INDEX \"Big[id]\" ON \"Big\" USING btree (id)",
        "CREATE INDEX \"Big[name,v]\" ON \"Big\" USING btree (name, v)");
    // a unique index is a unique constraint: psql prints "UNIQUE CONSTRAINT" for it
    assertThat(column(answer, "pg_get_constraintdef")).containsExactly("UNIQUE (id)", null);
    assertThat(column(answer, "contype")).containsExactly("u", null);
    assertThat(column(answer, "condeferrable")).containsExactly(false, null);
    assertThat(column(answer, "condeferred")).containsExactly(false, null);
    assertThat(column(answer, "indisreplident")).containsExactly(false, false);
    assertThat(column(answer, "reltablespace")).containsExactly(0, 0);
  }

  /** psql 17's {@code \di}. */
  @Test
  void psqlListsTheIndexes() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT n.nspname as "Schema",
          c.relname as "Name",
          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
          pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
          c2.relname as "Table"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
             LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
             LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
        WHERE c.relkind IN ('i','I','')
              AND n.nspname <> 'pg_catalog'
              AND n.nspname !~ '^pg_toast'
              AND n.nspname <> 'information_schema'
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;
        """);

    assertThat(column(answer, "Schema")).containsExactly(database.getName(), database.getName());
    assertThat(column(answer, "Name")).containsExactly("Big[id]", "Big[name,v]");
    assertThat(column(answer, "Type")).containsExactly("index", "index");
    assertThat(column(answer, "Owner")).containsExactly(USER, USER);
    assertThat(column(answer, "Table")).containsExactly("Big", "Big");
  }

  /** The first statement of {@code \d "Big[id]"}: an index is found by its name, as any relation is. */
  @Test
  void psqlFindsAnIndexByName() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT c.oid,
          n.nspname,
          c.relname
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname OPERATOR(pg_catalog.~) E'^(Big\\\\[id\\\\])$' COLLATE pg_catalog.default
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 2, 3;
        """);

    assertThat(column(answer, "relname")).containsExactly("Big[id]");
  }

  /** The three statements of {@code \d "Big[id]"} after it found the index. */
  @Test
  void psqlDescribesAnIndex() {
    final int index = oidOf("Big[id]");

    final PostgresCatalog.Answer info = resolve("""
        SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
        FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
        LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
        WHERE c.oid = '%d';
        """.formatted(index));
    assertThat(column(info, "relkind")).containsExactly("i");
    assertThat(column(info, "relhasindex")).containsExactly(false);
    assertThat(column(info, "amname")).containsExactly("btree");

    final PostgresCatalog.Answer columns = resolve("""
        SELECT a.attname,
          pg_catalog.format_type(a.atttypid, a.atttypmod),
          CASE WHEN a.attnum <= (SELECT i.indnkeyatts FROM pg_catalog.pg_index i WHERE i.indexrelid = '%1$d') THEN 'yes' ELSE 'no' END AS is_key,
          pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE) AS indexdef
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = '%1$d' AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;
        """.formatted(index));
    assertThat(column(columns, "attname")).containsExactly("id");
    assertThat(column(columns, "format_type")).containsExactly("integer");
    assertThat(column(columns, "is_key")).containsExactly("yes");
    assertThat(column(columns, "indexdef")).containsExactly("id");

    final PostgresCatalog.Answer detail = resolve("""
        SELECT i.indisunique, i.indisprimary, i.indisclustered, i.indisvalid,
          (NOT i.indimmediate) AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x') AND condeferrable) AS condeferrable,
          (NOT i.indimmediate) AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x') AND condeferred) AS condeferred,
        i.indisreplident,
        false AS indnullsnotdistinct,
          a.amname, c2.relname, pg_catalog.pg_get_expr(i.indpred, i.indrelid, true)
        FROM pg_catalog.pg_index i, pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_am a
        WHERE i.indexrelid = c.oid AND c.oid = '%d' AND c.relam = a.oid
        AND i.indrelid = c2.oid;
        """.formatted(index));
    assertThat(column(detail, "indisunique")).containsExactly(true);
    assertThat(column(detail, "amname")).containsExactly("btree");
    assertThat(column(detail, "relname")).containsExactly("Big");
  }

  /**
   * The statements psql 17 sends for every table besides the ones above: row-level security policies, extended statistics,
   * publications. ArcadeDB has none of them, which is the answer; they used to be refused (issue #10371) and {@code \d} with them.
   */
  @Test
  void psqlGetsNoPolicyStatisticsOrPublicationOfATable() {
    final int table = oidOf("Big");

    assertThat(resolve("""
        SELECT pol.polname, pol.polpermissive,
          CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
          pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
          pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
          CASE pol.polcmd
            WHEN 'r' THEN 'SELECT'
            WHEN 'a' THEN 'INSERT'
            WHEN 'w' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            END AS cmd
        FROM pg_catalog.pg_policy pol
        WHERE pol.polrelid = '%d' ORDER BY 1;
        """.formatted(table)).rows).isEmpty();

    assertThat(resolve("""
        SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, stxname,
          (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ')
           FROM pg_catalog.unnest(stxkeys) s(attnum)
           JOIN pg_catalog.pg_attribute a ON (stxrelid = a.attrelid AND
                a.attnum = s.attnum AND NOT attisdropped)) AS columns,
          'd' = any(stxkind) AS ndist_enabled,
          'f' = any(stxkind) AS deps_enabled,
          'm' = any(stxkind) AS mcv_enabled,
          -1 AS stxstattarget
        FROM pg_catalog.pg_statistic_ext
        WHERE stxrelid = '%d'
        ORDER BY 1;
        """.formatted(table)).rows).isEmpty();

    assertThat(resolve("""
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
        WHERE pr.prrelid = '%1$d'
        UNION ALL
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('%1$d')
        ORDER BY 1;
        """.formatted(table)).rows).isEmpty();
  }

  /** The inheritance of {@code \d "Sub"} (its parents) and of {@code \d "Big"} (its children). */
  @Test
  void psqlReadsTheInheritanceOfATable() {
    final PostgresCatalog.Answer parents = resolve("""
        SELECT c.oid::pg_catalog.regclass
        FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
        WHERE c.oid = i.inhparent AND i.inhrelid = '%d'
          AND c.relkind != 'p' AND c.relkind != 'I'
        ORDER BY inhseqno;
        """.formatted(oidOf("Sub")));
    assertThat(parents.rows.stream().map(row -> row.values().iterator().next())).containsExactly("Big");

    final PostgresCatalog.Answer children = resolve("""
        SELECT c.oid::pg_catalog.regclass, c.relkind, false AS inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
        FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
        WHERE c.oid = i.inhrelid AND i.inhparent = '%d'
        ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;
        """.formatted(oidOf("Big")));
    assertThat(children.rows).hasSize(1);
    assertThat(children.rows.get(0).values().iterator().next()).isEqualTo("Sub");
    assertThat(column(children, "relkind")).containsExactly("r");

    assertThat(resolve("select inhrelid from pg_inherits where inhparent = " + oidOf("Plain")).rows).isEmpty();
  }

  /**
   * {@code \d} on a type with no index: psql reads {@code relhasindex}, finds it false and sends no index statement, and the ones
   * it does send for every table (policies, statistics, publications, inheritance) answer instead of being refused.
   */
  @Test
  void psqlDescribesATypeWithNoIndex() {
    final int table = oidOf("Plain");

    final PostgresCatalog.Answer info = resolve("""
        SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
        FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
        LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
        WHERE c.oid = '%d';
        """.formatted(table));
    assertThat(column(info, "relhasindex")).containsExactly(false);
    assertThat(column(info, "amname")).containsExactly("heap");

    // the index statement, were it sent, finds none
    assertThat(resolve("""
        SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
          pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
        FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
          LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
        WHERE c.oid = '%d' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
        ORDER BY i.indisprimary DESC, c2.relname;
        """.formatted(table)).rows).isEmpty();

    assertThat(resolve("select inhparent from pg_inherits where inhrelid = " + table).rows).isEmpty();
    assertThat(resolve("select polname from pg_policy where polrelid = " + table).rows).isEmpty();
    assertThat(resolve("select stxname from pg_statistic_ext where stxrelid = " + table).rows).isEmpty();
  }

  /** psql reads {@code relhasindex} before it lists the indexes of a table. */
  @Test
  void psqlReadsRelhasindexOfEveryTable() {
    final PostgresCatalog.Answer answer = resolve("select relname, relhasindex from pg_catalog.pg_class where relkind = 'r' order by 1");

    assertThat(column(answer, "relname")).containsExactly("Big", "Plain", "Sub");
    assertThat(column(answer, "relhasindex")).containsExactly(true, false, false);
  }

  // ---------------------------------------------------------------- psqlodbc

  /** psqlodbc's {@code SQLPrimaryKeys}: two aliases of {@code pg_attribute} and of {@code pg_class}, and {@code indkey[...]}. */
  @Test
  void odbcPrimaryKeysOfATableWithoutOneAreNone() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT ta.attname, ia.attnum, ic.relname, n.nspname, NULL
         FROM pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, pg_catalog.pg_namespace n, pg_catalog.pg_class ic
         WHERE tc.relname = 'Big' AND n.nspname = '%s' AND tc.oid = i.indrelid AND n.oid = tc.relnamespace AND i.indisprimary = 't' AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) AND ic.oid = i.indexrelid ORDER BY ia.attnum
        """.formatted(database.getName()));

    // ArcadeDB has no primary key: a unique index is not one, and the flag-as-text test must not be read as true
    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns).hasSize(5);
  }

  /** The key columns of a unique index through the same shape: the join of an index with its own and its table's columns. */
  @Test
  void theKeyColumnsOfAnIndexAreFoundThroughTheTwoAliasesOfPgAttribute() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT ta.attname, ia.attnum, ic.relname
         FROM pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, pg_catalog.pg_class ic
         WHERE tc.relname = 'Big' AND tc.oid = i.indrelid AND i.indisunique = 't' AND i.indnkeyatts = 1 AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) AND ic.oid = i.indexrelid ORDER BY ia.attnum
        """);

    assertThat(column(answer, "attname")).containsExactly("id");
    assertThat(column(answer, "attnum")).containsExactly(1);
    assertThat(column(answer, "relname")).containsExactly("Big[id]");
  }

  /** The shape of psqlodbc's {@code SQLStatistics}: the indexes of a table with the key columns of each as a text of numbers. */
  @Test
  void odbcStatisticsOfATable() {
    final PostgresCatalog.Answer answer = resolve("select c.relname, i.indkey, i.indisunique, i.indisclustered, a.amname, c.relhasrules, n.nspname"
        + " from pg_catalog.pg_index i, pg_catalog.pg_class c, pg_catalog.pg_class d, pg_catalog.pg_am a, pg_catalog.pg_namespace n"
        + " where d.relname = 'Big' and n.nspname = '" + database.getName() + "' and n.oid = d.relnamespace and d.oid = i.indrelid"
        + " and i.indexrelid = c.oid and c.relam = a.oid order by i.indisunique desc, a.amname, c.relname");

    assertThat(column(answer, "relname")).containsExactly("Big[id]", "Big[name,v]");
    // an int2vector is read as text, its values separated by a space; the keys are the numbers of the columns in the table
    assertThat(column(answer, "indkey")).containsExactly("1", "2 3");
    assertThat(column(answer, "indisunique")).containsExactly(true, false);
    assertThat(column(answer, "amname")).containsExactly("btree", "btree");
  }

  // ---------------------------------------------------------------- information_schema and pg_catalog views

  @Test
  void informationSchemaListsTheUniqueConstraintsAndTheirColumns() {
    final PostgresCatalog.Answer constraints = resolve(
        "select constraint_name, table_name, constraint_type, is_deferrable, initially_deferred from information_schema.table_constraints");
    assertThat(column(constraints, "constraint_name")).containsExactly("Big[id]");
    assertThat(column(constraints, "table_name")).containsExactly("Big");
    assertThat(column(constraints, "constraint_type")).containsExactly("UNIQUE");
    assertThat(column(constraints, "is_deferrable")).containsExactly("NO");

    final PostgresCatalog.Answer keys = resolve(
        "select constraint_name, column_name, ordinal_position from information_schema.key_column_usage order by 1, 3");
    assertThat(column(keys, "column_name")).containsExactly("id");
    assertThat(column(keys, "ordinal_position")).containsExactly(1);
  }

  @Test
  void aCompositeUniqueIndexIsOneConstraintOverItsColumnsInOrder() {
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Plain", "a");
    database.getSchema().getType("Plain").createProperty("b", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Plain", "b", "a");

    final PostgresCatalog.Answer keys = resolve(
        "select constraint_name, column_name, ordinal_position from information_schema.key_column_usage where table_name = 'Plain' order by 1, 3");
    assertThat(column(keys, "constraint_name")).containsExactly("Plain[a]", "Plain[b,a]", "Plain[b,a]");
    assertThat(column(keys, "column_name")).containsExactly("a", "b", "a");
    assertThat(column(keys, "ordinal_position")).containsExactly(1, 1, 2);

    final PostgresCatalog.Answer definitions = resolve("""
        select pg_get_constraintdef(con.oid) as def from pg_constraint con join pg_class c on c.oid = con.conrelid
        where c.relname = 'Plain' order by con.conname""");
    assertThat(column(definitions, "def")).containsExactly("UNIQUE (a)", "UNIQUE (b, a)");
  }

  @Test
  void pgIndexesNamesEachIndexWithTheStatementThatCreatesIt() {
    final PostgresCatalog.Answer answer = resolve("select schemaname, tablename, indexname, indexdef from pg_indexes order by indexname");

    assertThat(column(answer, "schemaname")).containsExactly(database.getName(), database.getName());
    assertThat(column(answer, "tablename")).containsExactly("Big", "Big");
    assertThat(column(answer, "indexname")).containsExactly("Big[id]", "Big[name,v]");
    // PostgreSQL's own spelling: the schema in front of the table, each quoted when it has capitals
    final String schema = PostgresCatalogIndexes.quoteIdentifier(database.getName());
    assertThat(column(answer, "indexdef")).containsExactly("CREATE UNIQUE INDEX \"Big[id]\" ON " + schema + ".\"Big\" USING btree (id)",
        "CREATE INDEX \"Big[name,v]\" ON " + schema + ".\"Big\" USING btree (name, v)");
  }

  // ---------------------------------------------------------------- the model

  @Test
  void anIndexIsAClassOfKindIWithItsOwnOidAndTheAccessMethodOfItsFamily() {
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.HASH, false, "Plain", "a");

    final PostgresCatalog.Answer answer = resolve(
        "select c.relname, c.relkind, am.amname, c.relam, c.relnatts from pg_class c join pg_am am on am.oid = c.relam where c.relkind = 'i' order by 1");

    assertThat(column(answer, "relname")).containsExactly("Big[id]", "Big[name,v]", "Plain[a]");
    assertThat(column(answer, "relkind")).containsExactly("i", "i", "i");
    assertThat(column(answer, "amname")).containsExactly("btree", "btree", "hash");
    assertThat(column(answer, "relam")).containsExactly(403, 403, 405);
    assertThat(column(answer, "relnatts")).containsExactly(1, 2, 1);

    final List<Object> oids = column(resolve("select oid from pg_class"), "oid");
    assertThat(oids).as("every class has its own oid").doesNotHaveDuplicates().hasSize(6);
  }

  @Test
  void theOidsOfAnIndexAreTheSameFromOneStatementToTheNext() {
    final Object first = resolve("select indexrelid from pg_index where indisunique").rows.get(0).get("indexrelid");
    final Object second = resolve("select indexrelid from pg_index where indisunique").rows.get(0).get("indexrelid");

    assertThat(first).isEqualTo(second).isEqualTo(oidOf("Big[id]"));
  }

  @Test
  void anIndexIsListedOnTheTypeThatDeclaresItNotOnItsSubTypes() {
    final PostgresCatalog.Answer answer = resolve("select c.relname, i.indisunique from pg_class c, pg_index i where c.oid = i.indrelid order by 1, 2");

    assertThat(column(answer, "relname")).containsExactly("Big", "Big");
  }

  @Test
  void theKeyColumnsOfAnIndexAreDescribedAsColumnsOfIt() {
    final PostgresCatalog.Answer answer = resolve(
        "select a.attname, a.attnum, a.atttypid, t.typname from pg_attribute a join pg_type t on t.oid = a.atttypid where a.attrelid = "
            + oidOf("Big[name,v]") + " order by a.attnum");

    assertThat(column(answer, "attname")).containsExactly("name", "v");
    assertThat(column(answer, "attnum")).containsExactly(1, 2);
    assertThat(column(answer, "typname")).containsExactly("varchar", "float8");
  }

  @Test
  void theColumnsOfATableDoNotListTheIndexColumns() {
    // information_schema lists the columns of tables only; the key columns of an index are pg_attribute's alone
    assertThat(column(resolve("select column_name from information_schema.columns where table_name = 'Big[id]' order by ordinal_position"),
        "column_name")).isEmpty();
    assertThat(column(resolve("select column_name from information_schema.columns where table_name = 'Big' order by ordinal_position"),
        "column_name")).containsExactly("id", "name", "v");
    assertThat(resolve("select attname from pg_attribute a join pg_class c on c.oid = a.attrelid where c.relkind = 'r' and c.relname = 'Big'").rows)
        .hasSize(3);
  }

  @Test
  void indkeyIsReadAsTextAndSubscriptedFromZero() {
    final PostgresCatalog.Answer answer = resolve(
        "select indkey, indkey[0] as first, indkey[1] as second, indnatts from pg_index where indnatts = 2");

    assertThat(column(answer, "indkey")).containsExactly("2 3");
    assertThat(column(answer, "first")).containsExactly(2);
    assertThat(column(answer, "second")).containsExactly(3);
  }

  @Test
  void aFlagTestedAgainstTextIsReadAsABoolean() {
    assertThat(column(resolve("select indexrelid from pg_index where indisunique = 't'"), "indexrelid")).hasSize(1);
    assertThat(column(resolve("select indexrelid from pg_index where indisunique = 'f'"), "indexrelid")).hasSize(1);
    assertThat(resolve("select indexrelid from pg_index where indisprimary = 't'").rows).isEmpty();
    assertThat(resolve("select indexrelid from pg_index where indisunique = 'true'").rows).hasSize(1);
  }

  // ---------------------------------------------------------------- definitions

  @Test
  void anIdentifierIsQuotedWhenPostgresqlWouldQuoteIt() {
    assertThat(PostgresCatalogIndexes.quoteIdentifier("id")).isEqualTo("id");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("name")).isEqualTo("name");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("user_id2")).isEqualTo("user_id2");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("Big")).isEqualTo("\"Big\"");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("Big[id]")).isEqualTo("\"Big[id]\"");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("2fast")).isEqualTo("\"2fast\"");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("has space")).isEqualTo("\"has space\"");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("say \"hi\"")).isEqualTo("\"say \"\"hi\"\"\"");
    // a keyword that cannot be a name on its own
    assertThat(PostgresCatalogIndexes.quoteIdentifier("order")).isEqualTo("\"order\"");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("user")).isEqualTo("\"user\"");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("timestamp")).isEqualTo("\"timestamp\"");
    assertThat(PostgresCatalogIndexes.quoteIdentifier("")).isEqualTo("\"\"");
  }

  @Test
  void pgGetIndexdefAnswersTheStatementOrAKeyColumn() {
    final int index = oidOf("Big[name,v]");

    final PostgresCatalog.Answer answer = resolve("select pg_get_indexdef(" + index + ") as qualified, pg_get_indexdef(" + index
        + ", 0, true) as pretty, pg_get_indexdef(" + index + ", 1, false) as one, pg_get_indexdef(" + index + ", 2, true) as two, "
        + "pg_get_indexdef(" + index + ", 3, true) as three, pg_get_indexdef(1) as absent from pg_class where relname = 'Big'");

    final String schema = PostgresCatalogIndexes.quoteIdentifier(database.getName());
    assertThat(answer.rows.get(0)).containsEntry("qualified",
            "CREATE INDEX \"Big[name,v]\" ON " + schema + ".\"Big\" USING btree (name, v)")
        .containsEntry("pretty", "CREATE INDEX \"Big[name,v]\" ON \"Big\" USING btree (name, v)").containsEntry("one", "name")
        .containsEntry("two", "v").containsEntry("three", null).containsEntry("absent", null);
  }

  @Test
  void aColumnNamedLikeAKeywordIsQuotedInTheStatement() {
    database.getSchema().getType("Plain").createProperty("order", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Plain", "order");

    final PostgresCatalog.Answer answer = resolve("select pg_get_indexdef(indexrelid, 0, true) as def from pg_index i join pg_class c on c.oid = i.indrelid where c.relname = 'Plain'");

    assertThat(column(answer, "def")).containsExactly("CREATE INDEX \"Plain[order]\" ON \"Plain\" USING btree (\"order\")");
  }

  // ---------------------------------------------------------------- the JDBC driver

  /** The set-returning function the JDBC driver lists an index's key columns with: a row per element, the calls in step. */
  @Test
  void expandarrayTurnsAnIndexIntoARowPerKeyColumn() {
    final PostgresCatalog.Answer answer = resolve("""
        select ci.relname as idx, (information_schema._pg_expandarray(i.indkey)).n as position,
               (information_schema._pg_expandarray(i.indkey)).x as attnum
        from pg_class ci join pg_index i on ci.oid = i.indexrelid order by 1, 2""");

    assertThat(column(answer, "idx")).containsExactly("Big[id]", "Big[name,v]", "Big[name,v]");
    assertThat(column(answer, "position")).containsExactly(1, 1, 2);
    assertThat(column(answer, "attnum")).containsExactly(1, 2, 3);
  }

  /** The driver's {@code getPrimaryKeys} wraps the expansion in a derived table and reads {@code (result.KEYS).x} outside it. */
  @Test
  void aMemberOfAnExpandedPairIsReadFromADerivedTable() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.IDX FROM
         (SELECT ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
                 ci.relname AS IDX, information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM
          FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
               JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid) JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
          WHERE true AND ct.relname = 'Big') result
        where result.A_ATTNUM = (result.KEYS).x ORDER BY result.table_name, result.idx, result.key_seq""");

    assertThat(column(answer, "column_name")).containsExactly("id", "name", "v");
    assertThat(column(answer, "key_seq")).containsExactly(1, 1, 2);
    assertThat(column(answer, "idx")).containsExactly("Big[id]", "Big[name,v]", "Big[name,v]");
  }

  @Test
  void anArrayWithNoElementProducesNoRow() {
    final PostgresCatalog.Answer answer = resolve(
        "select (information_schema._pg_expandarray(i.indexprs)).n as position from pg_index i");

    assertThat(answer.rows).isEmpty();
  }

  /** The driver reads whether an index key is descending from the bit of {@code indoption}, a vector subscripted from 0. */
  @Test
  void theBitOfAnIndexOptionIsReadWithTheBitwiseOperator() {
    final PostgresCatalog.Answer answer = resolve("""
        select CASE i.indoption[0] & 1::smallint WHEN 1 THEN 'D' ELSE 'A' END as direction, 6 & 3 as bits, 5 & 1 as odd
        from pg_index i where i.indnatts = 1""");

    assertThat(column(answer, "direction")).containsExactly("A");
    assertThat(column(answer, "bits")).containsExactly(2L);
    assertThat(column(answer, "odd")).containsExactly(1L);
  }

  /** SQL's TRIM, with the side and the characters, which the driver strips the quotes of an index column with. */
  @Test
  void trimTakesTheCharactersNamedOffTheSideNamed() {
    final PostgresCatalog.Answer answer = resolve("""
        select trim(both '"' from '"Name"') as both_sides, trim(leading '"' from '"Name"') as leading_only,
               trim(trailing '"' from '"Name"') as trailing_only, trim(both from '  pad ') as spaces, trim('  pad ') as plain,
               btrim('xxpadxx', 'x') as characters
        from pg_class where relname = 'Big'""");

    assertThat(answer.rows.get(0)).containsEntry("both_sides", "Name").containsEntry("leading_only", "Name\"")
        .containsEntry("trailing_only", "\"Name").containsEntry("spaces", "pad").containsEntry("plain", "pad")
        .containsEntry("characters", "pad");
  }

  // ---------------------------------------------------------------- the join

  @Test
  void aRelationReadTwiceBindsTwoRows() {
    final PostgresCatalog.Answer answer = resolve("""
        select t.relname as tbl, ix.relname as idx from pg_class t, pg_class ix, pg_index i
        where i.indrelid = t.oid and i.indexrelid = ix.oid order by 1, 2""");

    assertThat(column(answer, "tbl")).containsExactly("Big", "Big");
    assertThat(column(answer, "idx")).containsExactly("Big[id]", "Big[name,v]");
  }

  @Test
  void aLeftJoinKeepsTheRowsNothingMatches() {
    final PostgresCatalog.Answer answer = resolve("""
        select c.relname, i.indexrelid from pg_class c left join pg_index i on i.indrelid = c.oid
        where c.relkind = 'r' order by 1""");

    assertThat(column(answer, "relname")).containsExactly("Big", "Big", "Plain", "Sub");
    assertThat(column(answer, "indexrelid")).hasSize(4).containsNull();
  }

  @Test
  void aConditionOnTheOuterSideIsAppliedAfterTheJoin() {
    final PostgresCatalog.Answer answer = resolve("""
        select c.relname from pg_class c left join pg_index i on i.indrelid = c.oid
        where c.relkind = 'r' and i.indexrelid is null order by 1""");

    assertThat(column(answer, "relname")).containsExactly("Plain", "Sub");
  }

  @Test
  void anIndexesTableCanBeCountedAndGrouped() {
    final PostgresCatalog.Answer answer = resolve("""
        select c.relname, count(*) as indexes from pg_class c join pg_index i on i.indrelid = c.oid
        where c.relkind = 'r' group by c.relname order by 1""");

    assertThat(column(answer, "relname")).containsExactly("Big");
    assertThat(column(answer, "indexes")).containsExactly(2L);
    assertThat(resolve("select count(*) as n from pg_index").rows.get(0)).containsEntry("n", 2L);
  }

  @Test
  void aStatementReadingNoRowOfTheDatabaseStillAnswersItsOneRow() {
    assertThat(resolve("select count(*) as n from pg_policy").rows.get(0)).containsEntry("n", 0L);
    assertThat(resolve("select count(*) as n from pg_publication_rel").rows.get(0)).containsEntry("n", 0L);
  }

  @Test
  void aDatabaseWithNoIndexListsNone() {
    database.getSchema().dropIndex("Big[id]");
    database.getSchema().dropIndex("Big[name,v]");

    assertThat(resolve("select * from pg_index").rows).isEmpty();
    assertThat(resolve("select relhasindex from pg_class where relname = 'Big'").rows.get(0)).containsEntry("relhasindex", false);
    assertThat(resolve("select * from information_schema.table_constraints").rows).isEmpty();
  }

  @Test
  void aTypeCalledLikeACatalogRelationIsTheUsersAndNotTheCatalogs() {
    database.getSchema().createDocumentType("pg_policy");

    assertThat(PostgresCatalog.resolve("select * from pg_policy", database, USER)).isNull();
  }

  /**
   * psql's index statement over a database of many types and indexes: the rows are found by the equalities of the statement and not
   * by trying every pair, so the statement does not spend the work a catalog query may.
   */
  @Test
  void theIndexesOfATableAreFoundWithoutReadingEveryPair() {
    database.transaction(() -> {
      for (int t = 0; t < 400; t++) {
        final String name = "Many" + t;
        database.getSchema().createDocumentType(name).createProperty("k", Type.INTEGER);
        database.getSchema().getType(name).createProperty("m", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, name, "k");
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, name, "m");
      }
    });

    final PostgresCatalog.Answer answer = resolve("""
        SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
          pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
        FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
          LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
        WHERE c.oid = '%d' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
        ORDER BY i.indisprimary DESC, c2.relname;
        """.formatted(oidOf("Many123")));

    assertThat(column(answer, "relname")).containsExactly("Many123[k]", "Many123[m]");
  }

  /**
   * A cross join of large relations that nothing narrows is refused with the statement's work limit instead of being built: each
   * binding it keeps is charged to the budget, so it stops long before it holds a number of rows that could fill the heap.
   */
  @Test
  @Timeout(120)
  void aCrossJoinOfLargeRelationsRunsOutOfBudgetInsteadOfMemory() {
    database.transaction(() -> {
      for (int t = 0; t < 300; t++) {
        final String name = "Cross" + t;
        database.getSchema().createDocumentType(name).createProperty("k", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, name, "k");
      }
    });

    // 302 indexes: three entries make 27 million bindings
    assertThatThrownBy(() -> PostgresCatalog.resolve("select count(*) from pg_index a, pg_index b, pg_index c", database, USER))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  // ---------------------------------------------------------------- helpers

  private int oidOf(final String relation) {
    final PostgresCatalog.Answer answer = resolve("select oid from pg_class where relname = '" + relation.replace("'", "''") + "'");
    assertThat(answer.rows).as("one class named %s", relation).hasSize(1);
    return ((Number) answer.rows.get(0).get("oid")).intValue();
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("answered: %s", query).isNotNull();
    assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
    return answer;
  }

  private static List<Object> column(final PostgresCatalog.Answer answer, final String name) {
    final List<Object> values = new ArrayList<>();
    for (final Map<String, Object> row : answer.rows)
      values.add(row.get(name));
    return values;
  }
}
