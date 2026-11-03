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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11027: the {@code pg_type} rows for {@code name}, {@code oid} and {@code int2vector}, which clients look up by name
 * during metadata discovery although this protocol never announces a column under them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11027NameTypeTest {

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

  @Test
  void nameIsNotAnnouncedForAColumnSoIdentifierColumnsStayVarchar() {
    assertThat(PostgresType.getTypeForValue("relname")).isEqualTo(PostgresType.VARCHAR);
  }
}
