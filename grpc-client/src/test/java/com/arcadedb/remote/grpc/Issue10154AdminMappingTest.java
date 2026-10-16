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
package com.arcadedb.remote.grpc;

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.remote.RemoteAlreadyExistsException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10154: the taken-name check of the gRPC admin calls (#10075) was repeated at two of the three places
 * {@link RemoteGrpcServer} turns a {@link StatusException} into an exception, and the third - the streaming calls,
 * {@code importDatabase} among them - read the server's trailer-less {@code ALREADY_EXISTS} as a {@link DuplicatedKeyException}
 * naming an index that does not exist. The admin mapping is one method now; this pins it for every shape it serves, and pins that
 * {@link RemoteGrpcServer} has no catch that maps past it.
 * <p>
 * Also the client half of #10153: a taken schema object name, named by the server as a {@code SchemaException} with an
 * {@code ALREADY_EXISTS} status, is rebuilt as an {@link AlreadyExistsException} with its kind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10154AdminMappingTest {
  /** Exactly what the admin service answers for a DatabaseAlreadyExistsException: no trailer at all. */
  private static final StatusException TAKEN = Status.ALREADY_EXISTS.withDescription("Database 'taken' already exists").asException();

  @Test
  void aTrailerlessAlreadyExistsIsATakenNameOnEveryAdminPath() {
    for (final boolean write : new boolean[] { true, false }) {
      final RuntimeException mapped = GrpcClientErrorMapper.toAdminException(TAKEN, "import database", write);
      assertThat(mapped).as("write=%s", write).isInstanceOf(RemoteAlreadyExistsException.class)
          .hasMessage("Database 'taken' already exists");
    }
  }

  @Test
  void onTheDataPlaneATrailerlessAlreadyExistsStaysADuplicateKey() {
    // a server that predates the class-name trailer (#5043) answers a duplicate key exactly this way
    assertThat(GrpcClientErrorMapper.toException(TAKEN)).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aDuplicateKeyNamedByItsTrailerIsStillADuplicateKeyOnAnAdminPath() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, DuplicatedKeyException.class.getName());
    final StatusException wire = Status.ALREADY_EXISTS.withDescription("dup").asException(trailers);

    assertThat(GrpcClientErrorMapper.toAdminException(wire, "create user", true)).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aTakenSchemaObjectNameIsRebuiltWithItsKind() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, SchemaException.class.getName());
    trailers.put(GrpcClientErrorMapper.ALREADY_EXISTS_KIND_KEY, "INDEX");
    final StatusException wire = Status.ALREADY_EXISTS.withDescription("Index 'V[p]' already exists").asException(trailers);

    for (final RuntimeException mapped : new RuntimeException[] { GrpcClientErrorMapper.toException(wire),
        GrpcClientErrorMapper.toAutoCommitWriteException(wire, "command"),
        GrpcClientErrorMapper.toStreamingWriteException(wire, "InsertStream") }) {
      assertThat(mapped).isInstanceOf(AlreadyExistsException.class).hasMessage("Index 'V[p]' already exists");
      assertThat(((AlreadyExistsException) mapped).getKind()).isEqualTo(AlreadyExistsException.Kind.INDEX);
    }
  }

  @Test
  void aSchemaExceptionWithAnyOtherStatusStaysASchemaException() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, SchemaException.class.getName());
    final RuntimeException mapped = GrpcClientErrorMapper.toException(
        Status.NOT_FOUND.withDescription("Type 'X' was not found").asException(trailers));

    assertThat(mapped).isInstanceOf(SchemaException.class).isNotInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void aKindThisClientDoesNotKnowStillRebuildsTheConflict() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, AlreadyExistsException.class.getName());
    trailers.put(GrpcClientErrorMapper.ALREADY_EXISTS_KIND_KEY, "SOMETHING_A_LATER_SERVER_ADDS");
    final RuntimeException mapped = GrpcClientErrorMapper.toException(
        Status.ALREADY_EXISTS.withDescription("taken").asException(trailers));

    assertThat(mapped).isInstanceOf(AlreadyExistsException.class);
    assertThat(((AlreadyExistsException) mapped).getKind()).isNull();
  }

  /**
   * The guarantee the issue asks for: no admin catch of {@link RemoteGrpcServer} maps a {@link StatusException} past the admin
   * mapping. {@code createDatabase} and {@code dropDatabase} word their own message and run the taken-name check themselves.
   */
  @Test
  void everyAdminCatchGoesThroughTheAdminMapping() throws IOException {
    final String source = Files.readString(Path.of("src/main/java/com/arcadedb/remote/grpc/RemoteGrpcServer.java"));
    final Matcher catches = Pattern.compile("catch \\((?:final )?StatusException e\\) \\{(.*?)\\n    }", Pattern.DOTALL)
        .matcher(source);
    int checked = 0;
    while (catches.find()) {
      ++checked;
      final String body = catches.group(1);
      final boolean direct = body.contains("GrpcClientErrorMapper.toException(") || body.contains("toAutoCommitWriteException(")
          || body.contains("toStreamingWriteException(");
      // a drop creates nothing, so it cannot meet a taken name
      if (direct && !body.contains("\"drop database\""))
        assertThat(body).as("a catch that maps a StatusException directly must check for a taken name first:%n%s", body)
            .containsAnyOf("toAdminException(", "isNameTaken(");
    }
    assertThat(checked).as("StatusException catches found in RemoteGrpcServer").isGreaterThanOrEqualTo(3);
    assertThat(source).contains("GrpcClientErrorMapper.toAdminException(e, operation, true)");
  }
}
