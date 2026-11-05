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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11314 on the MongoDB wire: a value a declared constraint refuses is {@code 121 DocumentValidationFailure}, what MongoDB
 * answers a document its collection's validator refuses, where it was the {@code BadValue} of every bad argument; and a call of
 * an unknown SQL function is a {@code FailedToParse}, the caller's mistake, where it was an uncoded server error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314MongoConstraintCodeTest {

  @Test
  void aViolatedDeclaredConstraintIsDocumentValidationFailure() {
    for (final ValidationException.Kind kind : ValidationException.Kind.values()) {
      final MongoServerError mapped = map(new CommandExecutionException("Error", new ValidationException(kind, "refused")));
      if (kind.isDeclaredConstraint()) {
        assertThat(mapped.getCode()).as("%s", kind).isEqualTo(121);
        assertThat(mapped.getCodeName()).as("%s", kind).isEqualTo("DocumentValidationFailure");
      } else
        assertThat(mapped.getCodeName()).as("%s", kind).isEqualTo("BadValue");
    }
  }

  @Test
  void aRefusalOfNoDeclaredConstraintStaysABadValue() {
    assertThat(map(new ValidationException("refused")).getCodeName()).isEqualTo("BadValue");
  }

  @Test
  void anUnknownSqlFunctionFailsToParse() {
    assertThat(map(new UnknownFunctionException("foo", "Unknown function name 'foo'")).getCodeName()).isEqualTo("FailedToParse");
  }

  private static MongoServerError map(final Exception error) {
    final MongoServerException mapped = MongoDBDatabaseWrapper.wireException("failed", error);
    assertThat(mapped).isInstanceOf(MongoServerError.class);
    return (MongoServerError) mapped;
  }
}
