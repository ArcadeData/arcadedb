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

import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.RecordNotFoundException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10795, the client half: the server answers a name that is no database {@code NOT_FOUND}, named by its class in the
 * {@code arcadedb-exception-class} trailer. The client rebuilds a {@link DatabaseNotFoundException}, on the data plane and on the
 * admin paths alike, and not the {@link RecordNotFoundException} the status alone stands for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10795DatabaseNotFoundMappingTest {

  @Test
  void aNameThatIsNoDatabaseIsRebuiltAsItself() {
    final StatusException wire = notFoundNamedBy(DatabaseNotFoundException.class.getName());

    assertThat(GrpcClientErrorMapper.toException(wire)).isInstanceOf(DatabaseNotFoundException.class)
        .hasMessage("Database 'nope' does not exist");
    assertThat(GrpcClientErrorMapper.toAutoCommitWriteException(wire, "executeCommand")).isInstanceOf(DatabaseNotFoundException.class);
    for (final boolean write : new boolean[] { true, false })
      assertThat(GrpcClientErrorMapper.toAdminException(wire, "open database", write)).as("write=%s", write)
          .isInstanceOf(DatabaseNotFoundException.class);
  }

  @Test
  void aMissingRecordStaysAMissingRecord() {
    assertThat(GrpcClientErrorMapper.toException(notFoundNamedBy(RecordNotFoundException.class.getName())))
        .isInstanceOf(RecordNotFoundException.class);
    assertThat(GrpcClientErrorMapper.toException(Status.NOT_FOUND.withDescription("gone").asException()))
        .isInstanceOf(RecordNotFoundException.class);
  }

  private static StatusException notFoundNamedBy(final String className) {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, className);
    return Status.NOT_FOUND.withDescription("Database 'nope' does not exist").asException(trailers);
  }
}
