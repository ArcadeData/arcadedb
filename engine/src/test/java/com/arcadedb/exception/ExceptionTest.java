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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionTest {

  @Test
  void arcadeDBExceptionWithMessage() {
    final ArcadeDBException ex = new ArcadeDBException("Test message");
    assertThat(ex.getMessage()).isEqualTo("Test message");
    assertThat(ex.getCause()).isNull();
  }

  @Test
  void arcadeDBExceptionWithMessageAndCause() {
    final RuntimeException cause = new RuntimeException("Root cause");
    final ArcadeDBException ex = new ArcadeDBException("Test message", cause);

    assertThat(ex.getMessage()).isEqualTo("Test message");
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void arcadeDBExceptionWithCause() {
    final RuntimeException cause = new RuntimeException("Root cause");
    final ArcadeDBException ex = new ArcadeDBException(cause);

    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void databaseIsClosedException() {
    final DatabaseIsClosedException ex = new DatabaseIsClosedException("Database closed");
    assertThat(ex.getMessage()).isEqualTo("Database closed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void databaseIsReadOnlyException() {
    final DatabaseIsReadOnlyException ex = new DatabaseIsReadOnlyException("Read only");
    assertThat(ex.getMessage()).isEqualTo("Read only");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void invalidDatabaseInstanceException() {
    final InvalidDatabaseInstanceException ex = new InvalidDatabaseInstanceException("Invalid instance");
    assertThat(ex.getMessage()).isEqualTo("Invalid instance");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void databaseMetadataException() {
    final DatabaseMetadataException ex = new DatabaseMetadataException("Metadata error");
    assertThat(ex.getMessage()).isEqualTo("Metadata error");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void commandParsingException() {
    final CommandParsingException ex = new CommandParsingException("Parsing failed");
    assertThat(ex.getMessage()).isEqualTo("Parsing failed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void commandSQLParsingException() {
    final CommandSQLParsingException ex = new CommandSQLParsingException("SQL parsing failed");
    assertThat(ex.getMessage()).isEqualTo("SQL parsing failed");
    assertThat(ex).isInstanceOf(CommandParsingException.class);
  }

  @Test
  void commandSQLParsingExceptionWithCommand() {
    final CommandSQLParsingException ex = new CommandSQLParsingException("Error at position 10");
    ex.setCommand("SELECT * FROM test");

    final String str = ex.toString();
    assertThat(str).contains("SELECT * FROM test");
  }

  @Test
  void commandExecutionException() {
    final CommandExecutionException ex = new CommandExecutionException("Execution failed");
    assertThat(ex.getMessage()).isEqualTo("Execution failed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void commandExecutionExceptionWithCause() {
    final RuntimeException cause = new RuntimeException("Root cause");
    final CommandExecutionException ex = new CommandExecutionException("Execution failed", cause);

    assertThat(ex.getMessage()).isEqualTo("Execution failed");
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void needRetryException() {
    final NeedRetryException ex = new NeedRetryException("Retry needed");
    assertThat(ex.getMessage()).isEqualTo("Retry needed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void concurrentModificationException() {
    final ConcurrentModificationException ex = new ConcurrentModificationException("Concurrent modification");
    assertThat(ex.getMessage()).isEqualTo("Concurrent modification");
    assertThat(ex).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void transactionException() {
    final TransactionException ex = new TransactionException("Transaction failed");
    assertThat(ex.getMessage()).isEqualTo("Transaction failed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void duplicatedKeyException() {
    final RID rid = new RID(null, 1, 100);
    // DuplicatedKeyException takes String keys, not Object[]
    final DuplicatedKeyException ex = new DuplicatedKeyException("testIndex", "[key1, key2]", rid);

    assertThat(ex.getIndexName()).isEqualTo("testIndex");
    assertThat(ex.getKeys()).isEqualTo("[key1, key2]");
    assertThat(ex.getCurrentIndexedRID()).isEqualTo(rid);
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void duplicatedKeyExceptionMessage() {
    final RID rid = new RID(null, 1, 100);
    final DuplicatedKeyException ex = new DuplicatedKeyException("myIndex", "myKey", rid);

    assertThat(ex.getMessage()).contains("myIndex");
    assertThat(ex.getMessage()).contains("myKey");
    assertThat(ex.getMessage()).contains("Duplicated key");
  }

  @Test
  void recordNotFoundException() {
    final RID rid = new RID(null, 1, 100);
    final RecordNotFoundException ex = new RecordNotFoundException("Record not found", rid);

    assertThat(ex.getMessage()).isEqualTo("Record not found");
    assertThat(ex.getRID()).isEqualTo(rid);
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void recordNotFoundExceptionWithCause() {
    final RID rid = new RID(null, 1, 100);
    final RuntimeException cause = new RuntimeException("Root cause");
    final RecordNotFoundException ex = new RecordNotFoundException("Record not found", rid, cause);

    assertThat(ex.getRID()).isEqualTo(rid);
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void validationException() {
    final ValidationException ex = new ValidationException("Validation failed");
    assertThat(ex.getMessage()).isEqualTo("Validation failed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void schemaException() {
    final SchemaException ex = new SchemaException("Schema error");
    assertThat(ex.getMessage()).isEqualTo("Schema error");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void timeoutException() {
    final TimeoutException ex = new TimeoutException("Operation timed out");
    assertThat(ex.getMessage()).isEqualTo("Operation timed out");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void encryptionException() {
    final EncryptionException ex = new EncryptionException("Encryption failed");
    assertThat(ex.getMessage()).isEqualTo("Encryption failed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void encryptionExceptionWithCause() {
    final RuntimeException cause = new RuntimeException("Key error");
    final EncryptionException ex = new EncryptionException("Encryption failed", cause);

    assertThat(ex.getMessage()).isEqualTo("Encryption failed");
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void serializationException() {
    final SerializationException ex = new SerializationException("Serialization failed");
    assertThat(ex.getMessage()).isEqualTo("Serialization failed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void configurationException() {
    final ConfigurationException ex = new ConfigurationException("Configuration invalid");
    assertThat(ex.getMessage()).isEqualTo("Configuration invalid");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void databaseOperationException() {
    final DatabaseOperationException ex = new DatabaseOperationException("Operation failed");
    assertThat(ex.getMessage()).isEqualTo("Operation failed");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void databaseOperationExceptionWithCause() {
    final RuntimeException cause = new RuntimeException("IO error");
    final DatabaseOperationException ex = new DatabaseOperationException("Operation failed", cause);

    assertThat(ex.getMessage()).isEqualTo("Operation failed");
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void exceptionsAreRuntimeExceptions() {
    // All ArcadeDB exceptions should be runtime exceptions
    assertThat(new ArcadeDBException("test")).isInstanceOf(RuntimeException.class);
    assertThat(new DatabaseIsClosedException("test")).isInstanceOf(RuntimeException.class);
    assertThat(new TransactionException("test")).isInstanceOf(RuntimeException.class);
    assertThat(new SchemaException("test")).isInstanceOf(RuntimeException.class);
  }

  @Test
  void exceptionChaining() {
    final Exception level1 = new RuntimeException("Level 1");
    final Exception level2 = new ArcadeDBException("Level 2", level1);
    final Exception level3 = new TransactionException("Level 3", level2);

    assertThat(level3.getCause()).isSameAs(level2);
    assertThat(level3.getCause().getCause()).isSameAs(level1);
  }
}
