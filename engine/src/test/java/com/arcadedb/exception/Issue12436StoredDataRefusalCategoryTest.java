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
package com.arcadedb.exception;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #12436, on every protocol: the DDL refusals of a caller's mistake that the HTTP API now answers 400 are classified by
 * {@link ErrorCategory}, which Bolt, Postgres, MongoDB and gRPC read too. They were plain {@link CommandExecutionException}s,
 * which no rule classifies, so every protocol answered them as a server fault ({@link ErrorCategory#SERVER}). They are
 * {@link ErrorCategory#VALIDATION} now, and still a {@link CommandExecutionException}, so an embedded caller that catches one
 * keeps catching it. A refusal of an existence constraint over stored data carries the {@link ValidationException.Kind} a protocol
 * with a code for a violated constraint answers with (Postgres {@code 23502}, Bolt {@code ConstraintValidationFailed}), the same
 * way the refusal of a MIN or a MAX over stored data already did.
 */
class Issue12436StoredDataRefusalCategoryTest extends TestHelper {

  @Test
  void mandatoryOverStoredRecordsIsAValidationErrorOfKindMandatory() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET x = 'a'"));

    final Throwable refused = catchThrowable(() -> database.command("sql", "CREATE PROPERTY T.m STRING (mandatory true)"));

    assertRefusal(refused, ValidationException.Kind.MANDATORY);
    assertThat(refused).hasMessageContaining("violates it").hasMessageContaining("Fix or delete the non-conforming records first");
    assertThat(CauseChain.find(refused, ValidationException.class)).as("a protocol reports the message of the ValidationException")
        .hasMessage(refused.getMessage());
    assertThat(database.getSchema().getType("T").existsProperty("m")).isFalse();
  }

  @Test
  void notNullOverAStoredNullIsAValidationErrorOfKindNotNull() {
    database.command("sql", "CREATE DOCUMENT TYPE N");
    database.command("sql", "CREATE PROPERTY N.v INTEGER");
    database.transaction(() -> database.newDocument("N").set("v", (Object) null).save());

    final Throwable refused = catchThrowable(() -> database.command("sql", "ALTER PROPERTY N.v NOTNULL true"));

    assertRefusal(refused, ValidationException.Kind.NOT_NULL);
    assertThat(database.getSchema().getType("N").getProperty("v").isNotNull()).isFalse();
  }

  /** The openCypher path shares the check: a constraint the stored nodes break is the same validation error. */
  @Test
  void cypherExistenceConstraintOverStoredNodesIsAValidationError() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.transaction(() -> database.command("sql", "CREATE VERTEX Person SET age = 3"));

    final Throwable refused = catchThrowable(
        () -> database.command("cypher", "CREATE CONSTRAINT person_name FOR (n:Person) REQUIRE n.name IS NOT NULL"));

    assertRefusal(refused, ValidationException.Kind.MANDATORY);
  }

  @Test
  void dropOfABucketATypeOwnsIsAValidationError() {
    database.command("sql", "CREATE DOCUMENT TYPE B");

    final Throwable refused = catchThrowable(() -> database.command("sql", "DROP BUCKET B_0"));

    assertThat(refused).isInstanceOf(InvalidArgumentException.class).hasMessageContaining("because used by type 'B'");
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.VALIDATION);
    assertThat(database.getSchema().existsBucket("B_0")).isTrue();
  }

  /** Control: a value the stored samples of a TIMESERIES type break names no kind, as on the write path of the same check. */
  @Test
  void aMaxTheStoredSamplesBreakIsAValidationErrorWithNoKind() {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (h STRING) FIELDS (v LONG) SHARDS 1");
    database.command("sql", "INSERT INTO M SET ts = 1000, h = 'a', v = 10");

    final Throwable refused = catchThrowable(() -> database.command("sql", "ALTER PROPERTY M.v MAX 5"));

    assertRefusal(refused, null);
    assertThat(refused).hasMessageContaining("violates it");
  }

  @Test
  void mandatoryTheStoredSamplesLackIsAValidationErrorOfKindMandatory() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v LONG) SHARDS 1");
    database.command("sql", "INSERT INTO S SET ts = 1000, h = 'a'");

    final Throwable refused = catchThrowable(() -> database.command("sql", "ALTER PROPERTY S.v MANDATORY true"));

    assertRefusal(refused, ValidationException.Kind.MANDATORY);
    assertThat(refused).hasMessageContaining("violates it");
  }

  private static void assertRefusal(final Throwable refused, final ValidationException.Kind kind) {
    assertThat(refused).as("still the CommandExecutionException an embedded caller catches").isInstanceOf(CommandExecutionException.class);
    assertThat(ErrorCategory.of(refused)).as("the caller's mistake on every protocol, not a server fault")
        .isEqualTo(ErrorCategory.VALIDATION);
    final ValidationException validation = CauseChain.find(refused, ValidationException.class);
    assertThat(validation).isNotNull();
    assertThat(validation.getKind()).isEqualTo(kind);
  }
}
