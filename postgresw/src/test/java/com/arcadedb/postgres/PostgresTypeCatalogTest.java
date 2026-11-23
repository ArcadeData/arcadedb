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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * The pg_type answers a PostgreSQL client uses to find out what the OIDs it was handed in RowDescription
 * mean (issue #5290).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresTypeCatalogTest {

  @Test
  void enumerationReturnsOneRowPerTypeThisProtocolCanProduce() {
    // This query used to be on an ignore-list and answered with zero rows, so a client that builds its
    // whole OID-to-name map up front built an empty one.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT oid, typname FROM pg_type");

    assertThat(rows).hasSize(PostgresType.values().length);
    assertThat(rows).allSatisfy(row -> assertThat(row.keySet()).containsExactly("oid", "typname"));
    assertThat(rows).anySatisfy(row -> {
      assertThat(row.get("oid")).isEqualTo(PostgresType.TEXT.code);
      assertThat(row.get("typname")).isEqualTo("text");
    });
    assertThat(rows).anySatisfy(row -> {
      assertThat(row.get("oid")).isEqualTo(PostgresType.ARRAY_TEXT.code);
      assertThat(row.get("typname")).isEqualTo("_text");
    });
  }

  @Test
  void enumerationIsOrderedByOidSoACachedAnswerNeverReshuffles() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT oid FROM pg_type");

    int previous = Integer.MIN_VALUE;
    for (final Map<String, Object> row : rows) {
      final int oid = (Integer) row.get("oid");
      assertThat(oid).isGreaterThan(previous);
      previous = oid;
    }
  }

  @Test
  void projectionOrderIsTheClientsOrderBecauseADataRowIsReadPositionally() {
    assertThat(PostgresTypeCatalog.resolve("SELECT typname, oid FROM pg_type").get(0).keySet())
        .containsExactly("typname", "oid");
    assertThat(PostgresTypeCatalog.resolve("SELECT oid, typname FROM pg_type").get(0).keySet())
        .containsExactly("oid", "typname");
  }

  @Test
  void projectionAliasNamesTheColumn() {
    assertThat(PostgresTypeCatalog.resolve("SELECT t.oid AS type_oid, t.typname FROM pg_type t").get(0).keySet())
        .containsExactly("type_oid", "typname");
  }

  @Test
  void starProjectsEveryColumnTheCatalogKnows() {
    final Map<String, Object> row = PostgresTypeCatalog.resolve("SELECT * FROM pg_type").get(0);
    assertThat(row.keySet()).contains("oid", "typname", "typelem", "typarray", "typdelim", "typtype", "typcategory",
        "typlen", "typinput", "typnotnull", "typbasetype", "typnamespace", "typrelid");
  }

  @Test
  void arrayAndScalarRowsDescribeEachOther() {
    final Map<String, Object> byOid = PostgresTypeCatalog.resolve("SELECT oid, typelem, typarray, typcategory FROM pg_type")
        .stream().filter(row -> row.get("oid").equals(PostgresType.ARRAY_TEXT.code)).findFirst().orElseThrow();
    assertThat(byOid.get("typelem")).isEqualTo(PostgresType.TEXT.code);
    assertThat(byOid.get("typarray")).isEqualTo(0);   // an array type is not itself the element of one
    assertThat(byOid.get("typcategory")).isEqualTo("A");

    final Map<String, Object> scalar = PostgresTypeCatalog.resolve("SELECT oid, typelem, typarray, typcategory FROM pg_type")
        .stream().filter(row -> row.get("oid").equals(PostgresType.TEXT.code)).findFirst().orElseThrow();
    assertThat(scalar.get("typelem")).isEqualTo(0);
    assertThat(scalar.get("typarray")).isEqualTo(PostgresType.ARRAY_TEXT.code);
    assertThat(scalar.get("typcategory")).isEqualTo("S");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // A filter this catalog does not understand is not guessed at.
      "SELECT oid FROM pg_type WHERE typtype = 'b' AND typlen > 4",
      "SELECT oid FROM pg_type ORDER BY oid",
      // A column this catalog cannot produce is declined whole rather than answered with a hole in it.
      "SELECT oid, typacl FROM pg_type",
      // Not pg_type at all.
      "SELECT oid FROM pg_class" })
  void shapesThisCatalogCannotAnswerAreDeclined(final String query) {
    assertThat(PostgresTypeCatalog.resolve(query)).isNull();
  }

  @Test
  void aFilterOnOidSelectsThatTypesOwnRow() {
    // Every column the client projected comes back, the OID it filtered on included. The fixed handful of
    // columns this used to answer with never carried "oid", so a client that asked for it got a row missing
    // the very column it selected by.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT oid, typname, typelem FROM pg_type WHERE oid = 1007");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly(entry("oid", 1007), entry("typname", "_int4"),
        entry("typelem", PostgresType.INTEGER.code));
  }

  @Test
  void aFilterOnTypeNameSelectsThatTypesOwnRow() {
    for (final PostgresType type : PostgresType.values()) {
      final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
          "SELECT oid, typname FROM pg_type WHERE typname = '" + type.typeName + "'");
      assertThat(rows).as(type.name()).hasSize(1);
      assertThat(rows.get(0)).as(type.name()).containsEntry("oid", type.code).containsEntry("typname", type.typeName);
    }
  }

  @Test
  void aFilterNamingATypeThisProtocolCannotProduceMeansNoRowRatherThanNoAnswer() {
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE typname = 'hstore'")).isEmpty();
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE oid = 987654")).isEmpty();
  }

  @Test
  void theDriverSelfJoinDescribesTheElementOfTheArray() {
    // The shape pgjdbc sends. Here - and only here - the projected columns describe a different row from
    // the one the filter selects, which is what the "t.typelem = e.oid" correlation says.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT e.typdelim, e.typname FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly(entry("typdelim", ","), entry("typname", "int4"));
  }

  @Test
  void theDriverSelfJoinAnswersOnlyWhatItProjected() {
    // Its WHERE clause names both oid and typelem; reading the columns off the whole query rather than off
    // the projection would answer with fields nobody asked for.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1009 AND t.typelem = e.oid");

    assertThat(rows.get(0)).containsExactly(entry("typdelim", ","));
  }

  @Test
  void theDriverSelfJoinOnAnUnknownOidStillFallsBackToTextSoADriverGetsAnAnswerItCanUse() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 987654 AND t.typelem = e.oid");

    assertThat(rows.get(0)).containsEntry("typname", "text");
  }

  @Test
  void theDriverSelfJoinOnAScalarSelectsNothingBecauseAScalarHasNoElement() {
    assertThat(PostgresTypeCatalog.resolve(
        "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 23 AND t.typelem = e.oid")).isEmpty();
  }

  @Test
  void inputFunctionsAreSpelledTheWayPostgresSpellsThem() {
    // Synthesising <name> + "in" is right for most types but not for the temporal ones, json or numeric, which
    // PostgreSQL spells with an underscore.
    final Map<Integer, Object> byOid = new HashMap<>();
    for (final Map<String, Object> row : PostgresTypeCatalog.resolve("SELECT oid, typinput FROM pg_type"))
      byOid.put((Integer) row.get("oid"), row.get("typinput"));

    assertThat(byOid).containsEntry(PostgresType.INTEGER.code, "int4in");
    assertThat(byOid).containsEntry(PostgresType.BOOLEAN.code, "boolin");
    assertThat(byOid).containsEntry(PostgresType.DATE.code, "date_in");
    assertThat(byOid).containsEntry(PostgresType.TIMESTAMP.code, "timestamp_in");
    assertThat(byOid).containsEntry(PostgresType.JSON.code, "json_in");
    assertThat(byOid).containsEntry(PostgresType.NUMERIC.code, "numeric_in");
    assertThat(byOid).containsEntry(PostgresType.ARRAY_TEXT.code, "array_in");
  }

  @Test
  void binaryIoFunctionsAreSpelledTheWayPostgresSpellsThem() {
    // issue #7178: the Apache Arrow ADBC driver picks a column's decoder by typreceive's name, so a plausible
    // name is no better than none - the type is simply left out of its resolver.
    final Map<Integer, Map<String, Object>> byOid = new HashMap<>();
    for (final Map<String, Object> row : PostgresTypeCatalog.resolve("SELECT oid, typreceive, typsend, typoutput FROM pg_type"))
      byOid.put((Integer) row.get("oid"), row);

    assertThat(byOid.get(PostgresType.INTEGER.code)).containsEntry("typreceive", "int4recv")
        .containsEntry("typsend", "int4send").containsEntry("typoutput", "int4out");
    assertThat(byOid.get(PostgresType.BOOLEAN.code)).containsEntry("typreceive", "boolrecv");
    assertThat(byOid.get(PostgresType.BYTEA.code)).containsEntry("typreceive", "bytearecv");
    assertThat(byOid.get(PostgresType.VARCHAR.code)).containsEntry("typreceive", "varcharrecv");
    assertThat(byOid.get(PostgresType.TIMESTAMP.code)).containsEntry("typreceive", "timestamp_recv")
        .containsEntry("typsend", "timestamp_send");
    assertThat(byOid.get(PostgresType.NUMERIC.code)).containsEntry("typreceive", "numeric_recv");
    assertThat(byOid.get(PostgresType.JSON.code)).containsEntry("typreceive", "json_recv");
    assertThat(byOid.get(PostgresType.ARRAY_TEXT.code))
        .as("the driver excludes arrays with typreceive != 'array_recv' and rebuilds them from typarray")
        .containsEntry("typreceive", "array_recv").containsEntry("typsend", "array_send");
  }

  @Test
  void storageAttributesFollowEachTypesWidth() {
    final Map<Integer, Map<String, Object>> byOid = new HashMap<>();
    for (final Map<String, Object> row : PostgresTypeCatalog
        .resolve("SELECT oid, typbyval, typalign, typstorage, typcollation FROM pg_type"))
      byOid.put((Integer) row.get("oid"), row);

    assertThat(byOid.get(PostgresType.SMALLINT.code)).containsEntry("typbyval", Boolean.TRUE)
        .containsEntry("typalign", "s").containsEntry("typstorage", "p").containsEntry("typcollation", 0);
    assertThat(byOid.get(PostgresType.LONG.code)).containsEntry("typalign", "d");
    assertThat(byOid.get(PostgresType.BOOLEAN.code)).containsEntry("typalign", "c");
    assertThat(byOid.get(PostgresType.TEXT.code)).containsEntry("typbyval", Boolean.FALSE)
        .containsEntry("typstorage", "x")
        .as("a collation-sensitive type carries the default collation's OID").containsEntry("typcollation", 100);
    assertThat(byOid.get(PostgresType.NUMERIC.code))
        .as("numeric is the one varlena type PostgreSQL keeps in the main table").containsEntry("typstorage", "m");
  }

  @Test
  void anArrayTakesItsStorageAttributesFromItsElement() {
    // PostgreSQL auto-generates each array type from its element (Catalog.pm, GenerateArrayTypes). Only the
    // columns with a BKI_ARRAY_DEFAULT get a fixed array value; the rest are copied from the element, and
    // typalign has a rule of its own: INT unless the element needs DOUBLE.
    final Map<Integer, Map<String, Object>> byOid = new HashMap<>();
    for (final Map<String, Object> row : PostgresTypeCatalog
        .resolve("SELECT oid, typalign, typcollation, typstorage, typbyval, typcategory FROM pg_type"))
      byOid.put((Integer) row.get("oid"), row);

    assertThat(byOid.get(PostgresType.ARRAY_LONG.code))
        .as("_int8's element is DOUBLE-aligned, so the array is too").containsEntry("typalign", "d");
    assertThat(byOid.get(PostgresType.ARRAY_DOUBLE.code)).containsEntry("typalign", "d");
    assertThat(byOid.get(PostgresType.ARRAY_INT.code))
        .as("_int4's element is INT-aligned").containsEntry("typalign", "i");
    assertThat(byOid.get(PostgresType.ARRAY_CHAR.code))
        .as("an array never narrows to its element's CHAR alignment").containsEntry("typalign", "i");

    assertThat(byOid.get(PostgresType.ARRAY_TEXT.code))
        .as("typcollation has no BKI_ARRAY_DEFAULT, so _text copies text's collation rather than dropping it")
        .containsEntry("typcollation", 100);
    assertThat(byOid.get(PostgresType.ARRAY_INT.code))
        .as("an array of a non-collatable element stays non-collatable").containsEntry("typcollation", 0);

    assertThat(byOid.get(PostgresType.ARRAY_TEXT.code)).containsEntry("typstorage", "x")
        .containsEntry("typbyval", Boolean.FALSE).containsEntry("typcategory", "A");
  }

  @Test
  void noArrayTypeHasAnArrayForAnElement() {
    // typalign and typcollation are derived by reading the element's, which is a single step only because
    // this protocol has no array of arrays. The assumption is invisible in that code and would misbehave
    // quietly rather than fail if one were ever added, so it is asserted here instead.
    for (final PostgresType type : PostgresType.values()) {
      if (!type.isArrayType())
        continue;
      final PostgresType element = PostgresType.byCode(type.elementCode);
      assertThat(element).as("%s declares element OID %d, which is not a type this protocol produces",
          type.typeName, type.elementCode).isNotNull();
      assertThat(element.isArrayType()).as("%s's element %s must not itself be an array",
          type.typeName, element.typeName).isFalse();
    }
  }

  @Test
  void numericIsCategorisedAsNumberLikeEveryOtherNumericType() {
    // issue #6447: NUMERIC used to fall through category()'s default "U" (user-defined) arm, the same bucket
    // real PostgreSQL uses for json - wrong for a type PostgreSQL itself files under "N" alongside int4/float8.
    final Map<String, Object> row = PostgresTypeCatalog.resolve("SELECT oid, typcategory FROM pg_type")
        .stream().filter(r -> r.get("oid").equals(PostgresType.NUMERIC.code)).findFirst().orElseThrow();
    assertThat(row.get("typcategory")).isEqualTo("N");
  }

  @Test
  void anExplicitlyEmptyQuotedAliasIsAColumnNamedEmpty() {
    assertThat(PostgresTypeCatalog.resolve("SELECT oid AS \"\" FROM pg_type").get(0).keySet()).containsExactly("");
  }

  @Test
  void everyTypeCarriesTheNamePostgresGivesItsOid() {
    // The names are what a client resolves the announced OID against, so they have to be PostgreSQL's own.
    assertThat(PostgresType.ARRAY_TEXT.typeName).isEqualTo("_text");
    assertThat(PostgresType.ARRAY_INT.typeName).isEqualTo("_int4");
    assertThat(PostgresType.JSON.typeName).isEqualTo("json");
    // 1002 is "char"[]; 1003, which this used to claim, is name[] - a type ArcadeDB never produces.
    assertThat(PostgresType.ARRAY_CHAR.code).isEqualTo(1002);
    assertThat(PostgresType.ARRAY_CHAR.typeName).isEqualTo("_char");
    assertThat(PostgresType.ARRAY_CHAR.elementCode).isEqualTo(PostgresType.CHAR.code);
  }

  // ---------------------------------------------------------------- the element join is read off the tokens (issue #10882)

  @ParameterizedTest
  @ValueSource(strings = {
      // the whole join in a string literal, the FROM naming a user's type
      "SELECT typname FROM Note WHERE typname = 'pg_ typelem = e.oid and t.oid = 1007'",
      "SELECT e.typname FROM Note e WHERE e.typname = 'pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid'",
      // in a comment
      "SELECT e.typname FROM Note e /* pg_type t WHERE t.oid = 1007 AND t.typelem = e.oid */",
      "SELECT e.typname FROM Note e -- pg_type t WHERE t.oid = 1007 AND t.typelem = e.oid",
      // the correlation spelled out, but over relations that are not pg_type
      "SELECT e.typname FROM Note t, Note e WHERE t.oid = 1007 AND t.typelem = e.oid",
      "SELECT e.typname FROM pg_type t, Note e WHERE t.oid = 1007 AND t.typelem = e.oid",
      // the OID filter only inside a literal
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.typelem = e.oid AND t.typname = 'oid = 1007'",
      // the correlation only inside a literal
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typname = 'typelem = e.oid'" })
  void aJoinThatIsNotInTheStatementIsNotAnElementLookup(final String query) {
    assertThat(PostgresTypeCatalog.resolve(query)).as(query).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // a column of the array, not of its element
      "SELECT t.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid",
      "SELECT e.typdelim, t.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid",
      // a column no alias owns, which PostgreSQL refuses as ambiguous
      "SELECT typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid",
      // the filter on the element, which asks for the array whose element is 1007
      "SELECT e.typname FROM pg_type t, pg_type e WHERE e.oid = 1007 AND t.typelem = e.oid",
      // a correlation the other way round, and one naming the same alias twice
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND e.typelem = t.oid",
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = t.oid",
      // another predicate, which this recogniser would ignore rather than evaluate
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid AND e.typname = 'text'",
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 OR t.typelem = e.oid",
      // an outer join, a third relation, an unbound placeholder, an OID that is not an integer
      "SELECT e.typname FROM pg_type t LEFT JOIN pg_type e ON t.typelem = e.oid WHERE t.oid = 1007",
      "SELECT e.typname FROM pg_type t, pg_type e, pg_namespace n WHERE t.oid = 1007 AND t.typelem = e.oid",
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = $1 AND t.typelem = e.oid",
      "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007.5 AND t.typelem = e.oid" })
  void aStatementThisRecogniserWouldMisreadIsLeftToTheGeneralCatalog(final String query) {
    assertThat(PostgresTypeCatalog.resolve(query)).as(query).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT e.typdelim, e.typname FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid",
      "select e.typdelim, e.typname from pg_type t, pg_type e where t.typelem = e.oid and t.oid = 1007",
      "SELECT e.typdelim, e.typname FROM pg_type AS t, pg_type AS e WHERE 1007 = t.oid AND e.oid = t.typelem;",
      "SELECT e.typdelim, e.typname FROM pg_type t JOIN pg_type e ON t.typelem = e.oid WHERE t.oid = 1007",
      "SELECT e.typdelim, e.typname FROM pg_type t INNER JOIN pg_type e ON (t.typelem = e.oid) WHERE (t.oid = 1007)",
      "SELECT e.typdelim, e.typname FROM pg_type t JOIN pg_type e ON t.typelem = e.oid AND t.oid = 1007",
      "SELECT e.typdelim, e.typname FROM pg_type, pg_type e WHERE pg_type.oid = 1007 AND pg_type.typelem = e.oid",
      "SELECT e.typdelim, e.typname FROM pg_type t, pg_type e /* the driver's lookup */ WHERE t.oid = 1007 AND t.typelem = e.oid" })
  void everySpellingOfTheDriverSelfJoinIsStillAnswered(final String query) {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(query);

    assertThat(rows).as(query).hasSize(1);
    assertThat(rows.get(0)).as(query).containsExactly(entry("typdelim", ","), entry("typname", "int4"));
  }

  @Test
  void theElementJoinKeepsTheAliasesTheClientProjected() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT e.typname AS element, e.typdelim \"Delim\" FROM pg_type t, pg_type e WHERE t.oid = 1009 AND t.typelem = e.oid");

    assertThat(rows.get(0)).containsExactly(entry("element", "text"), entry("Delim", ","));
  }

  /*
   * Issue #11027: the {@code pg_type} rows for {@code name}, {@code oid} and {@code int2vector}, which clients look up by name
   * during metadata discovery although this protocol never announces a column under them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11027
  @Test
  void nameHasARowByNameAndByOid() {
    for (final String query : new String[] { "SELECT oid, typname, typlen, typnamespace FROM pg_catalog.pg_type WHERE typname = 'name'",
        "SELECT oid, typname, typlen, typnamespace FROM pg_catalog.pg_type WHERE oid = 19" }) {
      final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(query);
      assertThat(rows).as(query).hasSize(1);
      final Map<String, Object> row = rows.get(0);
      assertThat(row.get("oid")).isEqualTo(19);
      assertThat(row.get("typname")).isEqualTo("name");
      assertThat(row.get("typlen")).isEqualTo(64);
      assertThat(row.get("typnamespace")).isEqualTo(PostgresTypeCatalog.PG_CATALOG_OID);
    }
  }

  // Regression: #11027
  @Test
  void oidAndInt2vectorHaveTheirRealOidsAndLengths() {
    final Map<String, Object> oid = PostgresTypeCatalog.resolve("SELECT oid, typlen FROM pg_catalog.pg_type WHERE typname = 'oid'")
        .get(0);
    assertThat(oid.get("oid")).isEqualTo(26);
    assertThat(oid.get("typlen")).isEqualTo(4);

    final Map<String, Object> vector = PostgresTypeCatalog.resolve(
        "SELECT oid, typlen FROM pg_catalog.pg_type WHERE typname = 'int2vector'").get(0);
    assertThat(vector.get("oid")).isEqualTo(22);
    assertThat(vector.get("typlen")).isEqualTo(-1);
  }

  // Regression: #11027
  @Test
  void everyTypeArrayReferenceStillResolves() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT oid, typelem, typarray FROM pg_catalog.pg_type");
    final Set<Object> oids = new HashSet<>();
    for (final Map<String, Object> row : rows)
      oids.add(row.get("oid"));
    for (final Map<String, Object> row : rows) {
      if ((Integer) row.get("typelem") != 0)
        assertThat(oids).as("typelem of %s", row.get("oid")).contains(row.get("typelem"));
      if ((Integer) row.get("typarray") != 0)
        assertThat(oids).as("typarray of %s", row.get("oid")).contains(row.get("typarray"));
    }
  }

  // Regression: #11027
  @Test
  void nameIsNotAnnouncedForAColumnSoIdentifierColumnsStayVarchar() {
    assertThat(PostgresType.getTypeForValue("relname")).isEqualTo(PostgresType.VARCHAR);
  }

  /*
   * The pg_type attributes of the date/time and jsonb types added by #11960 were derived from generic rules
   * that filed time, timetz and timestamptz under the user-defined category, interval too, preferred none of
   * them, gave timetz and interval INT alignment and spelled their I/O functions without the underscore
   * PostgreSQL uses (issue #12313). Comparing every row found two more in the same rules: {@code char} filed as a
   * string type instead of PostgreSQL's internal-use category Z, and {@code oid} not preferred within N.
   * <p>
   * The expected rows below were read from a stock PostgreSQL 16 server with
   * {@code SELECT oid, typname, typlen, typbyval, typcategory, typispreferred, typalign, typstorage, typinput,
   * typoutput, typreceive, typsend FROM pg_type}.
   */
  private static final String COLUMNS = "oid, typname, typlen, typbyval, typcategory, typispreferred, typalign, typstorage, "
      + "typinput, typoutput, typreceive, typsend";

  /** PostgreSQL 16, one line per type this protocol produces, columns in {@link #COLUMNS} order. */
  private static final List<String> POSTGRESQL_16 = List.of(//
      "16|bool|1|t|B|t|c|p|boolin|boolout|boolrecv|boolsend",
      "17|bytea|-1|f|U|f|i|x|byteain|byteaout|bytearecv|byteasend",
      "18|char|1|t|Z|f|c|p|charin|charout|charrecv|charsend",
      "19|name|64|f|S|f|c|p|namein|nameout|namerecv|namesend",
      "20|int8|8|t|N|f|d|p|int8in|int8out|int8recv|int8send",
      "21|int2|2|t|N|f|s|p|int2in|int2out|int2recv|int2send",
      "22|int2vector|-1|f|A|f|i|p|int2vectorin|int2vectorout|int2vectorrecv|int2vectorsend",
      "23|int4|4|t|N|f|i|p|int4in|int4out|int4recv|int4send",
      "25|text|-1|f|S|t|i|x|textin|textout|textrecv|textsend",
      "26|oid|4|t|N|t|i|p|oidin|oidout|oidrecv|oidsend",
      "114|json|-1|f|U|f|i|x|json_in|json_out|json_recv|json_send",
      "199|_json|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "700|float4|4|t|N|f|i|p|float4in|float4out|float4recv|float4send",
      "701|float8|8|t|N|t|d|p|float8in|float8out|float8recv|float8send",
      "1000|_bool|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1001|_bytea|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1002|_char|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1003|_name|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1005|_int2|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1006|_int2vector|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1007|_int4|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1009|_text|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1014|_bpchar|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1015|_varchar|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1016|_int8|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1021|_float4|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1022|_float8|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1028|_oid|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1042|bpchar|-1|f|S|f|i|x|bpcharin|bpcharout|bpcharrecv|bpcharsend",
      "1043|varchar|-1|f|S|f|i|x|varcharin|varcharout|varcharrecv|varcharsend",
      "1082|date|4|t|D|f|i|p|date_in|date_out|date_recv|date_send",
      "1083|time|8|t|D|f|d|p|time_in|time_out|time_recv|time_send",
      "1114|timestamp|8|t|D|f|d|p|timestamp_in|timestamp_out|timestamp_recv|timestamp_send",
      "1115|_timestamp|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1182|_date|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1183|_time|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1184|timestamptz|8|t|D|t|d|p|timestamptz_in|timestamptz_out|timestamptz_recv|timestamptz_send",
      "1185|_timestamptz|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1186|interval|16|f|T|t|d|p|interval_in|interval_out|interval_recv|interval_send",
      "1187|_interval|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1231|_numeric|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1266|timetz|12|f|D|f|d|p|timetz_in|timetz_out|timetz_recv|timetz_send",
      "1270|_timetz|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1700|numeric|-1|f|N|f|i|m|numeric_in|numeric_out|numeric_recv|numeric_send",
      "3802|jsonb|-1|f|U|f|i|x|jsonb_in|jsonb_out|jsonb_recv|jsonb_send",
      "3807|_jsonb|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send");

  // Regression: #12313
  @Test
  void theRowsTheIssueNamesAnswerWhatPostgreSQL16Answers() {
    // The query the issue gives, answered by the catalog a wire session reaches.
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/PostgresTypeCatalogTest-temporalAttributes");
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();
    final List<Map<String, Object>> rows;
    try {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
          "SELECT oid, typname, typlen, typbyval, typcategory, typispreferred, typalign, typstorage, typinput FROM pg_type "
              + "WHERE oid IN (1083, 1114, 1184, 1186, 1266, 3802)", database, "root");
      assertThat(answer).isNotNull();
      rows = answer.rows;
    } finally {
      database.drop();
    }

    assertThat(rows).extracting(PostgresTypeCatalogTest::line).containsExactlyInAnyOrder(//
        "1083|time|8|t|D|f|d|p|time_in",
        "1114|timestamp|8|t|D|f|d|p|timestamp_in",
        "1184|timestamptz|8|t|D|t|d|p|timestamptz_in",
        "1186|interval|16|f|T|t|d|p|interval_in",
        "1266|timetz|12|f|D|f|d|p|timetz_in",
        "3802|jsonb|-1|f|U|f|i|x|jsonb_in");
  }

  // Regression: #12313
  @Test
  void theDateTimeCategoryHasOnePreferredTypeAndItIsTimestamptz() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT typname, typcategory, typispreferred FROM pg_type");

    assertThat(rows).filteredOn(row -> "D".equals(row.get("typcategory")) && Boolean.TRUE.equals(row.get("typispreferred")))
        .extracting(row -> row.get("typname")).containsExactly("timestamptz");
    assertThat(rows).filteredOn(row -> "T".equals(row.get("typcategory")) && Boolean.TRUE.equals(row.get("typispreferred")))
        .extracting(row -> row.get("typname")).containsExactly("interval");
  }

  // Regression: #12313
  @Test
  void everyTypeThisProtocolProducesIsDescribedAsPostgreSQL16DescribesIt() {
    // Reaches the authority: every PostgresType must have its PostgreSQL 16 row here, or this census has a
    // hole a new enum constant could slip through.
    final Map<Integer, String> expected = POSTGRESQL_16.stream()
        .collect(Collectors.toMap(line -> Integer.parseInt(line.substring(0, line.indexOf('|'))), line -> line, (a, b) -> a,
            LinkedHashMap::new));
    final List<Integer> produced = Arrays.stream(PostgresType.values()).map(type -> type.code).toList();
    assertThat(expected.keySet()).as("the PostgreSQL 16 table must hold a row for every PostgresType").containsAll(produced);

    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT " + COLUMNS + " FROM pg_type");
    assertThat(rows).hasSize(PostgresType.values().length);

    final List<String> actual = rows.stream().map(PostgresTypeCatalogTest::line).toList();
    assertThat(actual).containsExactlyInAnyOrderElementsOf(
        expected.entrySet().stream().filter(e -> produced.contains(e.getKey())).map(Map.Entry::getValue).toList());
  }

  private static String line(final Map<String, Object> row) {
    return row.values().stream().map(value -> value instanceof Boolean b ? (b ? "t" : "f") : String.valueOf(value))
        .collect(Collectors.joining("|"));
  }

  /**
   * Regression test for issue #7224: {@code pg_type} joined to {@code pg_namespace} was answered as a question about
   * SCHEMAS, because SCHEMAS outranked TYPES in the family ranking. The single fabricated schema row then had every
   * {@code pg_type} column filled with NULL by {@code Row.complete()}, so the projection validated and the client got
   * one nameless type instead of the type list - silently, with no error.
   * <p>
   * The join is a qualification, not a constraint: {@code pg_namespace} is named purely to attach a schema to each
   * type, exactly as PostgreSQL's own {@code DatabaseMetaData.getTypeInfo()} writes it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PgTypeNamespaceJoin {
    private static final String DATABASE_PATH = "./target/databases/PostgresTypeCatalogTest-PgTypeNamespaceJoin";
    private static final String USER          = "root";

    /** {@code DatabaseMetaData.getTypeInfo()} exactly as pgjdbc writes it. */
    private static final String JDBC_GET_TYPE_INFO =
        "SELECT t.typname,t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname  != 'pg_toast' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c "
            + "WHERE c.oid = t.typrelid))";

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      database.transaction(() -> database.getSchema().createDocumentType("Article").createProperty("id", Type.INTEGER));
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // Regression: #7224
    @Test
    void getTypeInfoAnswersTheTypeListRatherThanOneNullSchemaRow() {
      final PostgresCatalog.Answer answer = resolve(JDBC_GET_TYPE_INFO);

      assertThat(answer.rows).as("one fabricated all-NULL row instead of the type list").hasSizeGreaterThan(1);
      assertThat(answer.rows).allSatisfy(row -> {
        assertThat(row.get("typname")).isNotNull();
        assertThat(row.get("oid")).isNotNull();
      });

      // The same set the dedicated pg_type recogniser answers with, so the two surfaces cannot disagree.
      assertThat(names(answer.rows, "typname")).containsExactlyInAnyOrderElementsOf(
          PostgresTypeCatalog.types().stream().map(t -> PostgresTypeCatalog.columnValue(t, "typname")).toList());
    }

    // Regression: #7224
    @Test
    void aPlainJoinOfTypeAndNamespaceIsAboutTypes() {
      final PostgresCatalog.Answer answer = resolve(
          "SELECT t.typname, n.nspname, n.oid, t.typnamespace FROM pg_type t JOIN pg_namespace n ON (t.typnamespace = n.oid)");

      // the built-in types and the row type of the one relation (issue #10873)
      assertThat(answer.rows).hasSize(PostgresTypeCatalog.types().size() + 1);
      assertThat(names(answer.rows, "typname")).doesNotContainNull();
      // The schema a built-in type lives in is pg_catalog, which is also what pg_type.typnamespace already says -
      // so the column the query joins ON and the row it joins TO have to agree, or the join it wrote is a lie. A row type is
      // in the schema of its relation.
      assertThat(answer.rows).filteredOn(row -> !"Article".equals(row.get("typname"))).extracting(row -> row.get("nspname"))
          .containsOnly("pg_catalog");
      assertThat(answer.rows).filteredOn(row -> "Article".equals(row.get("typname"))).extracting(row -> row.get("nspname"))
          .containsExactly(database.getName());
      assertThat(answer.rows).allSatisfy(row -> assertThat(row.get("oid")).isEqualTo(row.get("typnamespace")));
    }

    // Regression: #7224
    /** The FROM order must not decide it: a client is free to name pg_namespace first. */
    @Test
    void theFromOrderDoesNotChangeTheAnswer() {
      final PostgresCatalog.Answer answer = resolve("SELECT t.typname FROM pg_namespace n, pg_type t");

      assertThat(answer.rows).hasSize(PostgresTypeCatalog.types().size() + 1);
      assertThat(names(answer.rows, "typname")).doesNotContainNull();
    }

    // Regression: #7224
    /**
     * The other half of the ranking must not move: {@code pg_type} joined to {@code pg_class} or {@code pg_attribute}
     * CONSTRAINS the row set - one row per table or per column - and answering it with one row per type would change
     * what every JDBC {@code getColumns()} means.
     */
    @Test
    void pgClassAndPgAttributeStillOutrankPgType() {
      assertThat(names(resolve("SELECT c.relname, t.typname FROM pg_class c, pg_type t").rows, "relname")).containsExactly(
          "Article");

      final PostgresCatalog.Answer columns = resolve("SELECT a.attname, t.typname FROM pg_attribute a, pg_type t");
      assertThat(names(columns.rows, "attname")).contains("id");
    }

    // Regression: #7224
    /**
     * The families that used to TIE with TYPES at rank 0 - ROLES, DATABASES, PRIVILEGES, CHARACTER_SETS, COLLATIONS -
     * now lose to it, so the FROM order no longer settles them. That is a side effect of the ranking this issue
     * changed, and it is the same reading: pg_roles joined to pg_type reads an owner for each type, so it qualifies.
     * The old javadoc admitted the tie was accidental rather than considered, which is precisely why it needs pinning
     * now that it is neither.
     */
    @Test
    void typesWinsOverTheFamiliesItUsedToTieWith() {
      final PostgresCatalog.Answer typeFirst = resolve("SELECT t.typname, r.rolname FROM pg_type t, pg_roles r");
      final PostgresCatalog.Answer rolesFirst = resolve("SELECT t.typname, r.rolname FROM pg_roles r, pg_type t");

      assertThat(typeFirst.rows).hasSize(PostgresTypeCatalog.types().size() + 1);
      assertThat(names(typeFirst.rows, "typname")).doesNotContainNull();
      // The two orders have to agree on the ROWS, not merely on how many there are: a ranking regression could keep
      // the count and the non-null names while answering about a different relation.
      assertThat(rolesFirst.rows).containsExactlyElementsOf(typeFirst.rows);

      // And pg_roles on its own is still a question about roles: one row, not one per type.
      assertThat(resolve("SELECT rolname FROM pg_roles").rows).hasSize(1);
    }

    // Regression: #7224
    /** And pg_namespace alone is still a question about schemas: pg_catalog, which typnamespace names, and the database's (issue #10745). */
    @Test
    void pgNamespaceOnItsOwnIsStillASchemaQuery() {
      final PostgresCatalog.Answer answer = resolve("SELECT nspname FROM pg_catalog.pg_namespace");

      assertThat(answer.rows).hasSize(2);
      assertThat(answer.rows.get(0).get("nspname")).isEqualTo("pg_catalog");
      assertThat(answer.rows.get(1).get("nspname")).isNotNull();
    }

    // Regression: #7224
    /** A three-way join keeps naming the most specific subject: pg_class still wins over both. */
    @Test
    void tablesStillWinWhenNamespaceAndTypeAreBothJoinedToPgClass() {
      final PostgresCatalog.Answer answer = resolve("SELECT c.relname FROM pg_class c, pg_namespace n, pg_type t");

      assertThat(names(answer.rows, "relname")).containsExactly("Article");
    }

    // Regression: #7224
    /**
     * A scalar sub-select in the predicate is an operand this catalog cannot compute, not a reason to abandon the
     * whole predicate: the readable half still constrains, and three-valued logic settles the rest. What it must
     * NOT do is weaken the pg_type strictness - a predicate whose value is still UNKNOWN once the subquery has
     * answered UNKNOWN is declined exactly as an unparseable one was.
     */
    @Test
    void aScalarSubqueryIsAnUnknownOperandRatherThanAnUnreadablePredicate() {
      // typrelid is 0 for every type this protocol produces, so `= 0 OR <unknown>` is TRUE for all of them.
      assertThat(resolve("SELECT typname FROM pg_type t WHERE t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_class c "
          + "WHERE c.oid = t.typrelid)").rows).hasSize(PostgresTypeCatalog.types().size());

      // A sub-select this catalog can answer is evaluated rather than left UNKNOWN (issue #10228): no table has the
      // OID 0 a type's typrelid holds, so the sub-select is NULL for every type and selects none of them.
      assertThat(resolve("SELECT typname FROM pg_type t WHERE (SELECT c.relkind = 'c' FROM pg_class c "
          + "WHERE c.oid = t.typrelid)").rows).isEmpty();

      // With nothing readable left to decide it - a sub-select over a relation this catalog does not model - the
      // answer is still a decline rather than "every type".
      assertThat(PostgresCatalog.resolve("SELECT typname FROM pg_type t WHERE (SELECT e.extrelocatable FROM pg_extension e "
          + "WHERE e.oid = t.typreceive)", database, USER)).isSameAs(PostgresCatalog.DECLINED);
    }

    private PostgresCatalog.Answer resolve(final String query, final Object... parameters) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER, parameters);
      assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
      assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
      return answer;
    }

    private static List<Object> names(final List<Map<String, Object>> rows, final String column) {
      return rows.stream().map(row -> row.get(column)).toList();
    }
  }

  /**
   * Issue #10495: the seven lookups pgjdbc's {@code TypeInfoCache} sends when it meets a type it does not know were declined by the
   * catalog evaluator and kept on an empty-answer shim ({@code PostgresCatalog.isTypeResolutionProbe}), so a driver was told "no such
   * type" for a type this server has. {@code pg_type} is the closed set of types {@link PostgresType} knows, so each lookup has a real
   * answer for a name or an OID it holds, and only the others are empty.
   * <p>
   * The evaluator now reads what the lookups are written with - a table function in FROM ({@code generate_series}, {@code unnest}),
   * {@code USING (column)}, {@code = ANY(array)}, a derived table joined to other entries, and a second alias of {@code pg_type} - and
   * each is pinned on its own as well, so the answer does not depend on how one driver version spells its lookup.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TypeLookup {
    private static final String DATABASE_PATH = "./target/databases/PostgresTypeCatalogTest-TypeLookup";
    private static final String USER          = "root";

    /** pgjdbc: the OID of a type named without a schema. */
    private static final String OID_OF_NAME =
        "SELECT pg_type.oid, typname FROM pg_catalog.pg_type LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r from pg_namespace as ns "
            + "join (select s.r, (current_schemas(false))[s.r] as nspname from generate_series(1, array_upper(current_schemas(false), 1)) "
            + "as s(r)) as r using (nspname)) as sp ON sp.nspoid = typnamespace WHERE typname = $1 ORDER BY sp.r, pg_type.oid DESC LIMIT 1;";

    /** pgjdbc: the array type of an element type. */
    private static final String ARRAY_OF_TYPE =
        "SELECT t.typarray, arr.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid "
            + "JOIN pg_catalog.pg_type arr ON arr.oid = t.typarray WHERE t.typname = $1 AND (n.nspname = $2 OR $3 AND n.nspname = ANY "
            + "(current_schemas(true))) ORDER BY t.oid DESC LIMIT 1";

    /** pgjdbc: the array type that has a given element, found through a scalar sub-select. */
    private static final String ARRAY_OF_ELEMENT =
        "SELECT t.oid, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.typelem = "
            + "(SELECT oid FROM pg_catalog.pg_type WHERE typname = $1) AND substring(t.typname, 1, 1) = '_' AND t.typlen = -1 ORDER BY "
            + "t.typelem DESC LIMIT 1";

    /** pgjdbc: the name of a type, and whether its schema is on the search path. */
    private static final String NAME_OF_OID =
        "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n "
            + "ON t.typnamespace = n.oid WHERE t.oid = $1";

    /** pgjdbc: the delimiter of an array type. */
    private static final String DELIMITER_OF_ARRAY =
        "SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = $1 and t.typelem = e.oid";

    /** pgjdbc: the element of an array type. */
    private static final String ELEMENT_OF_ARRAY =
        "SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_type e "
            + "ON t.typelem = e.oid JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $1";

    /** A driver that loads every type up front, composites, ranges and enums included. */
    private static final String EVERY_TYPE =
        "SELECT ns.nspname, t.oid, t.typname FROM pg_type AS t JOIN pg_namespace AS ns ON (ns.oid = typnamespace) "
            + "LEFT JOIN pg_range r ON r.rngtypid = t.oid LEFT JOIN pg_enum e ON e.enumtypid = t.oid";

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      database.transaction(() -> database.getSchema().createDocumentType("Person").createProperty("name", Type.STRING));
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private PostgresCatalog.Answer resolve(final String query, final Object... parameters) {
      return PostgresCatalog.resolve(query, database, USER, parameters);
    }

    /** The values of each row, in the order the statement projected them. */
    private List<List<Object>> rowsOf(final String query, final Object... parameters) {
      final PostgresCatalog.Answer answer = resolve(query, parameters);
      assertThat(answer).as(query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
      assertThat(answer.rows).as("rows of %s", query).isNotNull();
      final List<List<Object>> rows = new ArrayList<>(answer.rows.size());
      for (final Map<String, Object> row : answer.rows)
        rows.add(new ArrayList<>(row.values()));
      return rows;
    }

    // Regression: #10495
    @Test
    void theOidOfATypeNameIsTheOidOfTheTypeThatHasIt() {
      assertThat(rowsOf(OID_OF_NAME, "int4")).containsExactly(List.of(23, "int4"));
      assertThat(rowsOf(OID_OF_NAME, "_int4")).containsExactly(List.of(1007, "_int4"));
      assertThat(rowsOf(OID_OF_NAME, "text")).containsExactly(List.of(25, "text"));
      assertThat(rowsOf(OID_OF_NAME, "bool")).containsExactly(List.of(16, "bool"));
    }

    // Regression: #10495
    @Test
    void aTypeNameNoTypeHasIsAnEmptyAnswerAndNotARefusal() {
      assertThat(rowsOf(OID_OF_NAME, "hstore")).isEmpty();
      assertThat(rowsOf(OID_OF_NAME, "")).isEmpty();
    }

    // Regression: #10495
    @Test
    void theArrayTypeOfAnElementTypeIsFoundOnTheSearchPathOrInANamedSchema() {
      // pgjdbc passes the schema it was given and whether to look on the search path when it was given none
      assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "", true)).containsExactly(List.of(1007, "_int4"));
      assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "pg_catalog", false)).containsExactly(List.of(1007, "_int4"));
      assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "other", false)).isEmpty();
      assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "", false)).isEmpty();
      assertThat(rowsOf(ARRAY_OF_TYPE, "hstore", "", true)).isEmpty();
      // a type with no array type of its own has nothing to answer
      assertThat(rowsOf(ARRAY_OF_TYPE, "_int4", "", true)).isEmpty();
    }

    // Regression: #10495
    @Test
    void theArrayTypeThatHasAnElementIsFoundThroughAScalarSubSelect() {
      assertThat(rowsOf(ARRAY_OF_ELEMENT, "int4")).containsExactly(List.of(1007, "_int4"));
      assertThat(rowsOf(ARRAY_OF_ELEMENT, "text")).containsExactly(List.of(1009, "_text"));
      assertThat(rowsOf(ARRAY_OF_ELEMENT, "hstore")).isEmpty();
    }

    // Regression: #10495
    @Test
    void theNameOfAnOidSaysWhetherItsSchemaIsOnTheSearchPath() {
      assertThat(rowsOf(NAME_OF_OID, 23)).containsExactly(List.of(true, "pg_catalog", "int4"));
      assertThat(rowsOf(NAME_OF_OID, 1007)).containsExactly(List.of(true, "pg_catalog", "_int4"));
      assertThat(rowsOf(NAME_OF_OID, 999_999)).isEmpty();
    }

    // Regression: #10495
    @Test
    void theDelimiterOfAnArrayIsTheOneOfItsElement() {
      assertThat(rowsOf(DELIMITER_OF_ARRAY, 1007)).containsExactly(List.of(","));
      // a type that is no array has no element, so the join selects nothing
      assertThat(rowsOf(DELIMITER_OF_ARRAY, 23)).isEmpty();
      assertThat(rowsOf(DELIMITER_OF_ARRAY, 999_999)).isEmpty();
    }

    // Regression: #10495
    @Test
    void theElementOfAnArrayIsNamedWithItsSchema() {
      assertThat(rowsOf(ELEMENT_OF_ARRAY, 1007)).containsExactly(List.of(23, true, "pg_catalog", "int4"));
      assertThat(rowsOf(ELEMENT_OF_ARRAY, 1009)).containsExactly(List.of(25, true, "pg_catalog", "text"));
      assertThat(rowsOf(ELEMENT_OF_ARRAY, 23)).isEmpty();
    }

    // Regression: #10495
    @Test
    void everyTypeIsListedWithItsSchemaAndNoRangeOrEnum() {
      final List<List<Object>> rows = rowsOf(EVERY_TYPE);
      // the built-in types and the row type of the one relation (issue #10873), which is in the relation's schema
      assertThat(rows).hasSize(PostgresTypeCatalog.types().size() + 1);
      for (final PostgresType type : PostgresTypeCatalog.types())
        assertThat(rows).contains(List.of("pg_catalog", type.code, type.typeName));
      assertThat(rows).filteredOn(row -> "Person".equals(row.get(2))).extracting(row -> row.get(0)).containsExactly(database.getName());
    }

    // Regression: #10495
    @Test
    void anOidAnArrayAndItsElementAgreeWithPostgresType() {
      for (final PostgresType type : PostgresTypeCatalog.types()) {
        assertThat(rowsOf(OID_OF_NAME, type.typeName)).as(type.typeName).containsExactly(List.of(type.code, type.typeName));
        assertThat(rowsOf(NAME_OF_OID, type.code)).as("oid %d", type.code).containsExactly(List.of(true, "pg_catalog", type.typeName));
        if (type.isArrayType())
          assertThat(rowsOf(ELEMENT_OF_ARRAY, type.code)).as("element of %s", type.typeName)
              .hasSize(1).first().satisfies(row -> assertThat(row.get(0)).isEqualTo(type.elementCode));
      }
    }

    // ---------------------------------------------------------------- what the evaluator reads, each on its own

    // Regression: #10495
    @Test
    void aTableFunctionInFromIsARelationOfItsOwn() {
      assertThat(rowsOf("select s.r from pg_database d, generate_series(1, 4) as s(r)")).containsExactly(List.of(1L), List.of(2L),
          List.of(3L), List.of(4L));
      assertThat(rowsOf("select s.r from pg_database d, generate_series(1, 9, 4) as s(r)")).containsExactly(List.of(1L), List.of(5L),
          List.of(9L));
      assertThat(rowsOf("select s.r from pg_database d, generate_series(3, 1, -1) as s(r)")).containsExactly(List.of(3L), List.of(2L),
          List.of(1L));
      assertThat(rowsOf("select s.r from pg_database d, generate_series(2, 1) as s(r)")).isEmpty();
      assertThat(rowsOf("select u.x from pg_database d, unnest(current_schemas(true)) as u(x)")).containsExactly(
          List.of("pg_catalog"), List.of(database.getName()));
      // without an alias the column is named after the function, as PostgreSQL names it
      assertThat(rowsOf("select generate_series from pg_database d, generate_series(1, 2)")).containsExactly(List.of(1L), List.of(2L));
    }

    // Regression: #10495
    @Test
    void aTableFunctionWithAnArgumentItCannotReadIsDeclinedAndNeverAnsweredEmpty() {
      assertThat(resolve("select s.r from pg_database d, generate_series(1, nope) as s(r)")).isSameAs(PostgresCatalog.DECLINED);
      assertThat(resolve("select s.r from pg_database d, generate_series(1, 2, 0) as s(r)")).isSameAs(PostgresCatalog.DECLINED);
      assertThat(resolve("select s.r from pg_database d, generate_subscripts(1, 2) as s(r)")).isSameAs(PostgresCatalog.DECLINED);
    }

    // Regression: #10495
    /** A series is charged to the statement's budget before it is built, so a client cannot ask the catalog for a billion rows. */
    @Test
    void aSeriesTooLongForTheBudgetIsRefusedBeforeItIsBuilt() {
      assertThatThrownBy(() -> resolve("select s.r from pg_database d, generate_series(1, 9223372036854775807) as s(r)"))
          .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
      assertThatThrownBy(() -> resolve("select s.r from pg_database d, generate_series(-9223372036854775807, 9223372036854775807) as s(r)"))
          .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    }

    // Regression: #10495
    @Test
    void aTableFunctionIsJoinedToARelation() {
      final List<List<Object>> rows = rowsOf("select t.typname, s.r from pg_type t, generate_series(1, 2) as s(r) where t.oid = 23");
      assertThat(rows).containsExactly(List.of("int4", 1L), List.of("int4", 2L));
    }

    // Regression: #10495
    @Test
    void usingJoinsOnTheColumnsBothSidesShare() {
      final List<List<Object>> rows = rowsOf("select ns.nspname, u.x from pg_namespace ns join (select x as nspname, 7 as x from "
          + "unnest(current_schemas(false)) as v(x)) as u using (nspname)");
      assertThat(rows).containsExactly(List.of(database.getName(), 7L));
    }

    // Regression: #10495
    @Test
    void anyComparesWithEveryElementOfAnArray() {
      assertThat(rowsOf("select 'a' = any(current_schemas(true)) from pg_database")).containsExactly(List.of(false));
      assertThat(rowsOf("select 'pg_catalog' = any(current_schemas(true)) from pg_database")).containsExactly(List.of(true));
      assertThat(rowsOf("select 'pg_catalog' = some(current_schemas(false)) from pg_database")).containsExactly(List.of(false));
      assertThat(rowsOf("select 'x' <> all(current_schemas(false)) from pg_database")).containsExactly(List.of(true));
      assertThat(rowsOf("select 'pg_catalog' <> all(current_schemas(true)) from pg_database")).containsExactly(List.of(false));
      // an operator other than = and <>, and a NULL operand or element: NULL, as SQL says
      assertThat(rowsOf("select 'b' > any(current_schemas(true)) from pg_database")).containsExactly(List.of(true));
      assertThat(rowsOf("select null = any(current_schemas(true)) from pg_database")).containsExactly(
          Collections.singletonList(null));
      // the elements of an array sub-select
      assertThat(rowsOf("select 23 = any(array(select oid from pg_type where typname = 'int4')) from pg_database")).containsExactly(
          List.of(true));
      assertThat(rowsOf("select nspname from pg_catalog.pg_namespace n, pg_catalog.pg_type t where t.oid = 23 and t.typnamespace = "
          + "n.oid and n.nspname = any(current_schemas(true))")).containsExactly(List.of("pg_catalog"));
    }

    // Regression: #10495
    @Test
    void aSecondAliasOfPgTypeReadsItsOwnRow() {
      final List<List<Object>> rows = rowsOf(
          "select t.typname, e.typname from pg_type t join pg_type e on t.typelem = e.oid where t.typname = '_int4'");
      assertThat(rows).containsExactly(List.of("_int4", "int4"));
    }

    // Regression: #10495
    @Test
    void theSchemasOnTheSearchPathAreTheCatalogAndTheDatabase() {
      assertThat(rowsOf("select current_schemas(false) from pg_database")).containsExactly(List.of(List.of(database.getName())));
      assertThat(rowsOf("select array_upper(current_schemas(false), 1), array_upper(current_schemas(true), 1) from pg_database"))
          .containsExactly(List.of(1L, 2L));
      assertThat(rowsOf("select (current_schemas(true))[1], (current_schemas(true))[2] from pg_database")).containsExactly(
          List.of("pg_catalog", database.getName()));
      // no dimension of an array has an upper bound but the first
      assertThat(rowsOf("select array_upper(current_schemas(true), 2) from pg_database")).containsExactly(
          Collections.singletonList(null));
    }

    // Regression: #10495
    @Test
    void aCountOverTheRelationsOfALookupIsStillItsOneRow() {
      assertThat(rowsOf("select count(*) from pg_catalog.pg_range")).containsExactly(List.of(0L));
      assertThat(rowsOf("select count(*) from pg_catalog.pg_enum")).containsExactly(List.of(0L));
    }
  }
}
