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
package com.arcadedb.server.grpc;

import com.arcadedb.server.CountedWriteScan;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #10084 and #10085: the gRPC service replies with counters and with the record it wrote, so no write it makes may go through
 * the {@code void} {@code save()}, which throws away the answer a before-event listener's veto gives. The only {@code newEdge()} calls
 * left are the unary {@code createRecord} edge arm, which has to answer a RID and so lets the {@code RecordVetoedException} it raises
 * reach the caller as {@code INVALID_ARGUMENT}, and {@code GraphBatch.newEdge()}, the bulk path that raises no record events.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10084GrpcCountedWritesReportTheVetoTest {
  private static final Pattern DELIBERATE = Pattern.compile("outV\\.newEdge\\(cls, inEl\\)|batch\\.newEdge\\(");

  @Test
  void theServiceWritesOnlyThroughTheReportingForms() throws IOException {
    assertThat(CountedWriteScan.offendersIn("src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java", DELIBERATE))
        .as("lines of the gRPC service that write through save() or newEdge(), which cannot report a veto").isEmpty();
  }
}
