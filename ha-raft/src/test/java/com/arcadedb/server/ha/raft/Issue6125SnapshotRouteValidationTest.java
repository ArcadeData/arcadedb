/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6125 item 3: {@code SnapshotHttpHandler} used to take its {@code /checksums} branch BEFORE the block that
 * rejects {@code ..}, separators, NUL and non-ASCII in the database name. It was not exploitable - the checksums
 * path gates on {@code existsDatabase}, an exact-match lookup over the already-open databases, so a traversal string
 * simply 404'd and the database was never resolved - but the guarantee lived two calls away from the handler that
 * needed it.
 * <p>
 * These tests pin the ORDER: the sub-path is stripped, the name is validated, and only then is a branch chosen, so
 * both routes refuse a malformed name identically instead of one of them relying on a downstream lookup.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6125SnapshotRouteValidationTest {

  @Test
  void aPlainDatabaseNameRoutesToTheSnapshotDownload() {
    final SnapshotHttpHandler.Route route = SnapshotHttpHandler.resolveRoute("/mydb");
    assertThat(route.error()).isNull();
    assertThat(route.databaseName()).isEqualTo("mydb");
    assertThat(route.checksums()).isFalse();
  }

  @Test
  void theChecksumsSuffixSelectsTheChecksumsViewOfTheSameName() {
    final SnapshotHttpHandler.Route route = SnapshotHttpHandler.resolveRoute("/mydb/checksums");
    assertThat(route.error()).isNull();
    assertThat(route.databaseName()).isEqualTo("mydb");
    assertThat(route.checksums()).isTrue();
  }

  @Test
  void aLeadingSlashIsOptional() {
    assertThat(SnapshotHttpHandler.resolveRoute("mydb").databaseName()).isEqualTo("mydb");
    assertThat(SnapshotHttpHandler.resolveRoute("mydb/checksums").databaseName()).isEqualTo("mydb");
  }

  /**
   * The regression itself: every one of these was refused on the download route and waved through the validation on
   * the checksums route.
   */
  @ParameterizedTest
  @ValueSource(strings = { "../../etc/passwd", "..", "a/b", "a\\b", "sub/dir/db", "db\u0000x", "caf\u00e9",
      "bell\u0007" })
  void aMalformedNameIsRefusedOnTheChecksumsRouteToo(final String malformed) {
    assertThat(SnapshotHttpHandler.resolveRoute("/" + malformed + "/checksums").error())
        .as("checksums route must refuse '%s'", malformed).isEqualTo("Invalid database name");
    assertThat(SnapshotHttpHandler.resolveRoute("/" + malformed).error())
        .as("download route must refuse '%s' as it always did", malformed).isEqualTo("Invalid database name");
  }

  @Test
  void anEmptyNameIsRefusedOnBothRoutes() {
    assertThat(SnapshotHttpHandler.resolveRoute("/").error()).isEqualTo("Missing database name in path");
    // "/checksums" IS A DATABASE CALLED "checksums", NOT AN EMPTY NAME ON THE CHECKSUMS ROUTE - THE SUFFIX ONLY
    // MATCHES WITH ITS SEPARATOR, WHICH IS WHY THE EMPTY CASE NEEDS THE DOUBLE SLASH
    assertThat(SnapshotHttpHandler.resolveRoute("//checksums").error()).isEqualTo("Missing database name in path");
    assertThat(SnapshotHttpHandler.resolveRoute("/checksums").databaseName()).isEqualTo("checksums");
  }

  /**
   * A refused route carries no database name at all, so no caller can accidentally use one that failed validation.
   */
  @Test
  void aRefusedRouteExposesNoDatabaseName() {
    assertThat(SnapshotHttpHandler.resolveRoute("/../secret/checksums").databaseName()).isNull();
  }
}
