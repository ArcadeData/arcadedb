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
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10495 through the PostgreSQL JDBC driver itself: the lookups its {@code TypeInfoCache} sends to name a type it has not met
 * (the OID of a schema-qualified name, the array of a type, the element of an array) are answered from {@link PostgresType} with the
 * values the driver bound, where they were declined and kept on an empty "no such type" answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10495TypeLookupIT extends PostgresWireProtocolTestBase {

  @Test
  void theDriverFindsTheOidOfAQualifiedTypeNameThisServerHas() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      // a name the driver has no entry for is looked up in the catalog, with the schema it was given
      assertThat(types.getPGType(types.getPGType("pg_catalog.int4"))).isEqualTo("int4");
      assertThat(types.getPGType("pg_catalog.int4")).isEqualTo(PostgresType.INTEGER.code);
      assertThat(types.getPGType("pg_catalog.text")).isEqualTo(PostgresType.TEXT.code);
    }
  }

  @Test
  void theDriverFindsTheArrayTypeOfAnElementType() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      assertThat(types.getPGType("pg_catalog.int4[]")).isEqualTo(PostgresType.ARRAY_INT.code);
      assertThat(types.getPGType("pg_catalog.float8[]")).isEqualTo(PostgresType.ARRAY_DOUBLE.code);
    }
  }

  @Test
  void aTypeNoServerTypeHasIsNotFoundAndNothingFails() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      // "no such type" is the driver's answer for a name the catalog does not have, an unspecified OID, and not an error
      assertThat(types.getPGType("pg_catalog.hstore")).isZero();
      assertThat(types.getPGType("hstore")).isZero();
      assertThat(types.getPGType("nowhere.int4")).isZero();
    }
  }

  @Test
  void theDriverNamesATypeFromItsOidAndFindsTheElementOfAnArray() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      for (final PostgresType type : PostgresTypeCatalog.types()) {
        assertThat(types.getPGType(type.code)).as("name of oid %d", type.code).isEqualTo(type.typeName);
        if (type.isArrayType())
          assertThat(types.getPGArrayElement(type.code)).as("element of %s", type.typeName).isEqualTo(type.elementCode);
      }
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
