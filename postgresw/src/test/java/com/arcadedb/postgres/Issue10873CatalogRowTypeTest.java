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
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10873: {@code pg_class.reltype} was 0 for every relation, so the join a client writes to reach a table's row type
 * matched nothing, and {@code pg_attribute.attcollation} was 0 for every column while {@code pg_type.typcollation} said the
 * column's type was collatable. Every relation now has its row type in {@code pg_type} ({@code typtype = 'c'}, {@code typrelid}
 * leading back), {@code reltype} names it, and the collation of a column is its type's.
 * <p>
 * The census generalises #10711's rule from the references of {@code pg_type} to every OID-valued column of every relation the
 * catalog models: each names a row of the relation it points into, and holds 0 only where PostgreSQL does. The columns are
 * enumerated from the catalog itself, and one that looks like a reference and is declared neither way fails the census, so a
 * relation or a column added later has to say what it points at.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10873CatalogRowTypeTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10873CatalogRowTypeTest";
  private static final String USER          = "root";

  /** The name shapes PostgreSQL gives a column that holds another catalog row's OID. */
  private static final Pattern LOOKS_LIKE_A_REFERENCE = Pattern.compile(
      ".+(oid|relid|typid|namespace|nspid|owner|collation|reltype|oftype|elem|array|relam|indid|parentid|dba|pubid|basetype|rewrite"
          + "|tablespace|classid|ev_class|foid|roleid|member|grantor|rettype|lang|support|variadic|server|subtype|subopc|canonical"
          + "|subdiff|constraint|aclrole)$");

  /** A column that holds a reference: the relation it points into, and when it may hold 0 instead. */
  private record Reference(String target, Predicate<Map<String, Object>> zeroAllowed) {
  }

  private static final Predicate<Map<String, Object>> NEVER  = row -> false;
  private static final Predicate<Map<String, Object>> ALWAYS = row -> true;

  private static final Map<String, Reference> REFERENCES = new HashMap<>();

  /** Columns whose name looks like a reference and that are not one, or point into nothing this catalog models, with why. */
  private static final Map<String, String> NOT_CHECKED = new HashMap<>();

  static {
    references("pg_class.relnamespace", "pg_namespace", NEVER);
    // the row type of the relation, 0 for an index alone (issue #10873)
    references("pg_class.reltype", "pg_type", row -> "i".equals(row.get("relkind")));
    references("pg_class.reloftype", "pg_type", ALWAYS);
    references("pg_class.relowner", "pg_roles", NEVER);
    references("pg_class.relam", "pg_am", ALWAYS);
    references("pg_class.reltoastrelid", "pg_class", ALWAYS);
    references("pg_class.relrewrite", "pg_class", ALWAYS);
    // 0 is the database's default tablespace in PostgreSQL
    references("pg_class.reltablespace", "pg_tablespace", ALWAYS);
    references("pg_attribute.attrelid", "pg_class", NEVER);
    references("pg_attribute.atttypid", "pg_type", NEVER);
    references("pg_attribute.attcollation", "pg_collation", ALWAYS);
    references("pg_type.typnamespace", "pg_namespace", NEVER);
    references("pg_type.typowner", "pg_roles", NEVER);
    references("pg_type.typrelid", "pg_class", ALWAYS);
    references("pg_type.typelem", "pg_type", ALWAYS);
    references("pg_type.typarray", "pg_type", ALWAYS);
    references("pg_type.typbasetype", "pg_type", ALWAYS);
    references("pg_type.typcollation", "pg_collation", ALWAYS);
    references("pg_namespace.nspowner", "pg_roles", NEVER);
    references("pg_database.datdba", "pg_roles", NEVER);
    references("pg_database.dattablespace", "pg_tablespace", ALWAYS);
    references("pg_index.indexrelid", "pg_class", NEVER);
    references("pg_index.indrelid", "pg_class", NEVER);
    references("pg_index.indcollation", "pg_collation", ALWAYS);
    references("pg_constraint.connamespace", "pg_namespace", NEVER);
    references("pg_constraint.conrelid", "pg_class", ALWAYS);
    references("pg_constraint.contypid", "pg_type", ALWAYS);
    references("pg_constraint.conindid", "pg_class", ALWAYS);
    references("pg_constraint.conparentid", "pg_constraint", ALWAYS);
    references("pg_constraint.confrelid", "pg_class", ALWAYS);
    references("pg_attrdef.adrelid", "pg_class", NEVER);
    references("pg_inherits.inhrelid", "pg_class", NEVER);
    references("pg_inherits.inhparent", "pg_class", NEVER);
    references("pg_collation.collnamespace", "pg_namespace", NEVER);
    references("pg_collation.collowner", "pg_roles", NEVER);
    references("pg_tablespace.spcowner", "pg_roles", NEVER);
    references("pg_policy.polrelid", "pg_class", NEVER);
    references("pg_statistic_ext.stxrelid", "pg_class", NEVER);
    references("pg_statistic_ext.stxnamespace", "pg_namespace", NEVER);
    references("pg_statistic_ext.stxowner", "pg_roles", NEVER);
    references("pg_publication.pubowner", "pg_roles", NEVER);
    references("pg_publication_rel.prpubid", "pg_publication", NEVER);
    references("pg_publication_rel.prrelid", "pg_class", NEVER);
    references("pg_publication_namespace.pnpubid", "pg_publication", NEVER);
    references("pg_publication_namespace.pnnspid", "pg_namespace", NEVER);
    references("pg_range.rngtypid", "pg_type", NEVER);
    references("pg_range.rngsubtype", "pg_type", NEVER);
    references("pg_range.rngmultitypid", "pg_type", NEVER);
    references("pg_range.rngcollation", "pg_collation", ALWAYS);
    references("pg_enum.enumtypid", "pg_type", NEVER);
    references("pg_proc.pronamespace", "pg_namespace", NEVER);
    references("pg_proc.proowner", "pg_roles", NEVER);
    references("pg_proc.prorettype", "pg_type", NEVER);
    references("pg_proc.provariadic", "pg_type", ALWAYS);
    references("pg_trigger.tgrelid", "pg_class", NEVER);
    references("pg_trigger.tgparentid", "pg_trigger", ALWAYS);
    references("pg_trigger.tgfoid", "pg_proc", NEVER);
    references("pg_trigger.tgconstrrelid", "pg_class", ALWAYS);
    references("pg_trigger.tgconstrindid", "pg_class", ALWAYS);
    references("pg_trigger.tgconstraint", "pg_constraint", ALWAYS);
    references("pg_auth_members.roleid", "pg_roles", NEVER);
    references("pg_auth_members.member", "pg_roles", NEVER);
    references("pg_auth_members.grantor", "pg_roles", NEVER);
    references("pg_rewrite.ev_class", "pg_class", NEVER);
    references("pg_sequence.seqrelid", "pg_class", NEVER);
    references("pg_sequence.seqtypid", "pg_type", NEVER);
    references("pg_default_acl.defaclrole", "pg_roles", NEVER);
    references("pg_default_acl.defaclnamespace", "pg_namespace", ALWAYS);
    references("pg_foreign_table.ftrelid", "pg_class", NEVER);

    notChecked("pg_proc.prolang", "pg_language is not modelled, and pg_proc has no row");
    notChecked("pg_proc.prosupport", "a regproc, read as a name");
    notChecked("pg_foreign_table.ftserver", "pg_foreign_server is not modelled, and pg_foreign_table has no row");
    notChecked("pg_range.rngsubopc", "pg_opclass is not modelled, and pg_range has no row");
    notChecked("pg_range.rngcanonical", "a regproc, read as a name");
    notChecked("pg_range.rngsubdiff", "a regproc, read as a name");
    notChecked("pg_index.indclass", "pg_opclass is not modelled: the operator classes have no equivalent here");
    notChecked("pg_description.classoid", "the catalog of the object described, and pg_description has no row");
    notChecked("pg_shdescription.classoid", "the catalog of the object described, and pg_shdescription has no row");
    notChecked("pg_depend.classid", "the OID of a system catalog (pg_class, pg_constraint, pg_rewrite), which pg_class does not list as a row");
    notChecked("pg_depend.refclassid", "the OID of a system catalog (pg_class, pg_constraint, pg_rewrite), which pg_class does not list as a row");
    notChecked("pg_shdescription.objoid", "an object of any shared catalog, and pg_shdescription has no row");
    notChecked("pg_tables.tableowner", "the owner's name, not its OID");
    notChecked("pg_matviews.matviewowner", "the owner's name, not its OID");
    notChecked("pg_views.viewowner", "the owner's name, not its OID");
  }

  private static void references(final String column, final String target, final Predicate<Map<String, Object>> zeroAllowed) {
    REFERENCES.put(column, new Reference(target, zeroAllowed));
  }

  private static void notChecked(final String column, final String why) {
    NOT_CHECKED.put(column, why);
  }

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    // a type with a super type, a text column, an integer one with a default, a list of text, a unique index on the text
    // column, and a materialized view
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final VertexType person = schema.createVertexType("Person");
      person.createProperty("name", Type.STRING);
      person.createProperty("age", Type.INTEGER).setDefaultValue("0");
      person.createProperty("tags", Type.LIST, "STRING");
      schema.createVertexType("Employee").addSuperType("Person").createProperty("salary", Type.DOUBLE);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "name");
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a', age = 1"));
    database.command("sql", "CREATE MATERIALIZED VIEW PersonView AS SELECT name FROM Person");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** The report's repro: the join from a relation to its row type answers one row per table and materialized view. */
  @Test
  void theRowTypeOfEveryRelationIsARowOfPgType() {
    final List<Map<String, Object>> relations = resolve("SELECT c.oid, c.relname, c.relkind, c.reltype FROM pg_class c ORDER BY 2").rows;
    final Set<String> withRowType = new TreeSet<>();
    for (final Map<String, Object> relation : relations)
      if ("i".equals(relation.get("relkind")))
        assertThat(number(relation, "reltype")).as("an index has no row type: %s", relation.get("relname")).isZero();
      else {
        assertThat(number(relation, "reltype")).as("the row type of %s", relation.get("relname")).isNotZero();
        withRowType.add((String) relation.get("relname"));
      }
    assertThat(withRowType).containsExactly("Employee", "Person", "PersonView");

    final List<Map<String, Object>> joined = resolve(
        "SELECT c.oid, c.relname, t.typname, t.typtype, t.typrelid, t.typcategory FROM pg_class c JOIN pg_type t ON t.oid = c.reltype "
            + "ORDER BY 2").rows;
    assertThat(joined).hasSize(3);
    for (final Map<String, Object> row : joined) {
      assertThat(row.get("typname")).isEqualTo(row.get("relname"));
      assertThat(row.get("typtype")).isEqualTo("c");
      assertThat(row.get("typcategory")).isEqualTo("C");
      assertThat(number(row, "typrelid")).as("typrelid leads back to the relation").isEqualTo(number(row, "oid"));
    }

    // the other direction, from the row type to its relation
    assertThat(column(resolve("SELECT c.relname FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid WHERE t.typname = 'Person'"),
        "relname")).containsExactly("Person");
  }

  /** pg_type lists the row types in both of the ways it is answered: by this catalog and by the recogniser that runs first. */
  @Test
  void pgTypeListsTheRowTypesInBothOfItsAnswers() {
    assertThat(column(resolve("SELECT typname FROM pg_type WHERE typtype = 'c' ORDER BY typname"), "typname"))
        .containsExactly("Employee", "Person", "PersonView");

    final int rowType = number(resolve("SELECT reltype FROM pg_class WHERE relname = 'Person'").rows.getFirst(), "reltype");
    final List<Map<String, Object>> recognised = PostgresTypeCatalog.resolve("SELECT oid, typname, typtype, typrelid FROM pg_type",
        () -> PostgresCatalog.rowTypes(database));
    assertThat(recognised).filteredOn(row -> "c".equals(row.get("typtype"))).extracting(row -> row.get("typname"))
        .containsExactly("Employee", "Person", "PersonView");
    assertThat(PostgresTypeCatalog.resolve("SELECT typname, typrelid FROM pg_type WHERE oid = " + rowType,
        () -> PostgresCatalog.rowTypes(database))).extracting(row -> row.get("typname")).containsExactly("Person");
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE typname = 'Person'", () -> PostgresCatalog.rowTypes(database)))
        .extracting(row -> row.get("oid")).containsExactly(rowType);
    // a name is compared as written
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE typname = 'person'", () -> PostgresCatalog.rowTypes(database)))
        .isEmpty();
    // a row type has no element type
    assertThat(PostgresTypeCatalog.resolve("SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = " + rowType
        + " AND t.typelem = e.oid", () -> PostgresCatalog.rowTypes(database))).isEmpty();
  }

  /** {@code format_type} and {@code regtype} name a row type as PostgreSQL prints it: the relation's name, quoted as needed. */
  @Test
  void aRowTypeIsNamedAfterItsRelation() {
    assertThat(column(resolve("SELECT format_type(c.reltype, NULL) AS t FROM pg_class c WHERE c.relname = 'Person'"), "t"))
        .containsExactly("\"Person\"");
    assertThat(column(resolve("SELECT c.reltype::regtype AS t FROM pg_class c WHERE c.relname = 'Person'"), "t"))
        .containsExactly("\"Person\"");
    final Object rowType = resolve("SELECT reltype FROM pg_class WHERE relname = 'Person'").rows.getFirst().get("reltype");
    assertThat(column(resolve("SELECT oid FROM pg_type WHERE oid = 'Person'::regtype"), "oid")).containsExactly(rowType);
  }

  /** The {@code attcollation} half: a column's collation is its type's, and an index key's is its column's. */
  @Test
  void theCollationOfAColumnIsTheCollationOfItsType() {
    final List<Map<String, Object>> rows = resolve("SELECT a.attname, a.attcollation, t.typname, t.typcollation FROM pg_attribute a "
        + "JOIN pg_type t ON t.oid = a.atttypid WHERE a.attrelid IN (SELECT oid FROM pg_class WHERE relname = 'Person')").rows;
    assertThat(rows).extracting(row -> row.get("attname")).contains("name", "age", "tags");
    for (final Map<String, Object> row : rows)
      assertThat(row.get("attcollation")).as("the collation of %s (%s)", row.get("attname"), row.get("typname"))
          .isEqualTo(row.get("typcollation"));
    assertThat(rows).filteredOn(row -> "name".equals(row.get("attname"))).extracting(row -> row.get("attcollation")).containsExactly(100);
    assertThat(rows).filteredOn(row -> "age".equals(row.get("attname"))).extracting(row -> row.get("attcollation")).containsExactly(0);

    assertThat(column(resolve("SELECT i.indcollation FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'Person[name]'"),
        "indcollation")).extracting(Object::toString).containsExactly("100");
  }

  /**
   * The guarantee: every reference column of every relation the catalog models names a row of the relation it points into, or
   * holds 0 where PostgreSQL does. The columns come from the catalog, so a new one shaped like a reference must be declared.
   */
  @Test
  void everyReferenceOfEveryRelationIsARowOfItsTarget() {
    final Set<String> undeclared = new TreeSet<>();
    final Map<String, Set<Long>> oidsOf = new HashMap<>();
    final List<String> violations = new ArrayList<>();
    final Set<String> checked = new TreeSet<>();

    for (final String relation : new TreeSet<>(PostgresCatalog.modelledRelations())) {
      if (relation.startsWith("information_schema."))
        continue;
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select * from " + relation, database, USER);
      if (answer == null || answer.rows == null)
        continue;

      for (final String columnName : answer.columns.keySet()) {
        final String column = relation + "." + columnName.toLowerCase(Locale.ENGLISH);
        final Reference reference = REFERENCES.get(column);
        if (reference == null) {
          if (LOOKS_LIKE_A_REFERENCE.matcher(columnName.toLowerCase(Locale.ENGLISH)).matches() && !NOT_CHECKED.containsKey(column))
            undeclared.add(column);
          continue;
        }

        final Set<Long> targets = oidsOf.computeIfAbsent(reference.target(), this::oidsOf);
        for (final Map<String, Object> row : answer.rows)
          for (final long oid : oids(row.get(columnName))) {
            if (oid == 0) {
              if (!reference.zeroAllowed().test(row))
                violations.add(column + " is 0 in " + row);
            } else if (!targets.contains(oid))
              violations.add(column + " = " + oid + " names no row of " + reference.target());
            checked.add(column);
          }
      }
    }

    assertThat(undeclared).as("columns shaped like a reference that the census does not declare").isEmpty();
    assertThat(violations).isEmpty();
    // the census reached the columns of the report, and the ones #10711 covered
    assertThat(checked).contains("pg_class.reltype", "pg_class.relnamespace", "pg_class.relam", "pg_attribute.attcollation",
        "pg_attribute.atttypid", "pg_type.typrelid", "pg_type.typnamespace", "pg_type.typarray", "pg_index.indcollation");
  }

  // ---------------------------------------------------------------- helpers

  private Set<Long> oidsOf(final String relation) {
    final Set<Long> oids = new HashSet<>();
    for (final Map<String, Object> row : resolve("select oid from " + relation).rows)
      oids.addAll(oids(row.get("oid")));
    return oids;
  }

  /** The OIDs a value holds: none for a NULL, one for a number, each element of a vector, which a client reads as text. */
  private static List<Long> oids(final Object value) {
    final List<Long> oids = new ArrayList<>();
    if (value instanceof Number number)
      oids.add(number.longValue());
    else if (value != null && !value.toString().isBlank())
      for (final String element : value.toString().trim().split("\\s+"))
        oids.add(Long.parseLong(element));
    return oids;
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("answered: %s", query).isNotNull();
    assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
    return answer;
  }

  private static int number(final Map<String, Object> row, final String column) {
    return ((Number) row.get(column)).intValue();
  }

  private static List<Object> column(final PostgresCatalog.Answer answer, final String name) {
    final List<Object> values = new ArrayList<>();
    for (final Map<String, Object> row : answer.rows)
      values.add(row.get(name));
    return values;
  }
}
