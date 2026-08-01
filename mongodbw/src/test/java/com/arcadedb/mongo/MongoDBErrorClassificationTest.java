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
package com.arcadedb.mongo;

import com.arcadedb.database.RID;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every failure used to leave the MongoDB wrapper as a bare {@link MongoServerException}, and the wrapper can only
 * report {@code code}/{@code codeName} to the client for a {@link MongoServerError} - so a caller's own mistake
 * arrived uncoded and indistinguishable from the server having broken. See issue #5628.
 */
class MongoDBErrorClassificationTest {

  @Test
  void anArithmeticErrorIsReportedAsABadValue() {
    final MongoServerException wrapped = MongoDBDatabaseWrapper.wireException("failed",
        new ArithmeticErrorException("/ by zero"));

    assertThat(wrapped).isInstanceOf(MongoServerError.class);
    assertThat(((MongoServerError) wrapped).getCode()).isEqualTo(2);
    assertThat(((MongoServerError) wrapped).getCodeName()).isEqualTo("BadValue");
    assertThat(wrapped.getCause()).isInstanceOf(ArithmeticErrorException.class);
  }

  @Test
  void aWrappedArithmeticErrorIsStillABadValue() {
    // A command reaching the wrapper through the auto-commit path arrives wrapped; inspecting only the outermost
    // throwable is what left these uncoded.
    final MongoServerException wrapped = MongoDBDatabaseWrapper.wireException("failed",
        new TransactionException("Error on transaction commit", new ArithmeticErrorException("long overflow")));

    assertThat(wrapped).isInstanceOf(MongoServerError.class);
    assertThat(((MongoServerError) wrapped).getCode()).isEqualTo(2);
  }

  @Test
  void aConflictIsReportedAsAWriteConflict() {
    final MongoServerException wrapped = MongoDBDatabaseWrapper.wireException("failed",
        new ConcurrentModificationException("page version changed"));

    assertThat(wrapped).isInstanceOf(MongoServerError.class);
    assertThat(((MongoServerError) wrapped).getCode()).isEqualTo(112);
    assertThat(((MongoServerError) wrapped).getCodeName()).isEqualTo("WriteConflict");
  }

  @Test
  void theRemainingCodedCategoriesGetTheirMongoCodes() {
    assertThat(codeOf(new DuplicatedKeyException("idx", "k", new RID(1, 1)))).isEqualTo(11000);
    assertThat(codeOf(new ValidationException("mandatory property"))).isEqualTo(2);
    assertThat(codeOf(new CommandParsingException("bad syntax"))).isEqualTo(9);
    assertThat(codeOf(new SchemaException("Type with name 'Nope' was not found"))).isEqualTo(26);
    assertThat(codeOf(new SecurityException("denied"))).isEqualTo(13);
    assertThat(codeOf(new TimeoutException("too slow"))).isEqualTo(50);
  }

  @Test
  void aCodeTheBackendAlreadyAssignedIsNotThrownAway() {
    // A MongoServerError from the bundled backend carries a code more precise than classification could infer,
    // and it is not an ArcadeDBException - so it would classify as SERVER and be re-wrapped uncoded, losing the
    // very thing the client needs.
    final MongoServerError fromBackend = new MongoServerError(16459, "attempt to insert in system namespace");

    final MongoServerException wrapped = MongoDBDatabaseWrapper.wireException("failed", fromBackend);

    assertThat(wrapped).isSameAs(fromBackend);
    assertThat(((MongoServerError) wrapped).getCode()).isEqualTo(16459);
  }

  @Test
  void aServerFaultStaysUncoded() {
    // MongoDB has no code that means "the server broke", so these keep the uncoded exception they already had
    // rather than being given a client-error code that would misdirect the caller.
    assertThat(MongoDBDatabaseWrapper.wireException("failed", new IOException("disk gone")))
        .isExactlyInstanceOf(MongoServerException.class);
    assertThat(MongoDBDatabaseWrapper.wireException("failed", new CommandExecutionException("something broke")))
        .isExactlyInstanceOf(MongoServerException.class);
    assertThat(MongoDBDatabaseWrapper.wireException("failed", new RecordNotFoundException("gone", new RID(1, 1))))
        .isExactlyInstanceOf(MongoServerException.class);
  }

  private static int codeOf(final Exception e) {
    return ((MongoServerError) MongoDBDatabaseWrapper.wireException("failed", e)).getCode();
  }
}
