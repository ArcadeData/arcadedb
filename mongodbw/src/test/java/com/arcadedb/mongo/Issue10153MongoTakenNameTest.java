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

import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10153 on the MongoDB wire: a taken collection name was answered {@code NamespaceNotFound}, the opposite of the refusal,
 * because the type raised extends {@code SchemaException}. It is what MongoDB answers a createCollection on an existing collection
 * with, {@code NamespaceExists}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10153MongoTakenNameTest {

  @Test
  void aTakenNameIsNamespaceExists() {
    final MongoServerException mapped = MongoDBDatabaseWrapper.wireException("failed", new TypeAlreadyExistsException("orders"));

    assertThat(mapped).isInstanceOf(MongoServerError.class);
    assertThat(((MongoServerError) mapped).getCode()).isEqualTo(48);
    assertThat(((MongoServerError) mapped).getCodeName()).isEqualTo("NamespaceExists");
  }

  @Test
  void aMissingNameIsStillNamespaceNotFound() {
    final MongoServerError mapped = (MongoServerError) MongoDBDatabaseWrapper.wireException("failed",
        new SchemaException("Type 'orders' was not found"));

    assertThat(mapped.getCode()).isEqualTo(26);
  }
}
