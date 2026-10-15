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
 * the {@code void} {@code save()} or {@code newEdge()}, which throw away (or raise) the answer a before-event listener's veto gives. The
 * only {@code newEdge()} calls left are {@code GraphBatch.newEdge()}, the bulk path that raises no record events. The unary
 * {@code createRecord} has to answer a RID, so it reads the engine's answer and raises a {@code RecordVetoedException} itself, which
 * reaches the caller as {@code INVALID_ARGUMENT}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10084GrpcCountedWritesReportTheVetoTest {
  private static final Pattern DELIBERATE = Pattern.compile("batch\\.newEdge\\(");

  @Test
  void theServiceWritesOnlyThroughTheReportingForms() throws IOException {
    assertThat(CountedWriteScan.offendersIn("src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java", DELIBERATE))
        .as("lines of the gRPC service that write through save() or newEdge(), which cannot report a listener's veto: write through"
            + " saveReportingVeto() or GraphEngine.newEdgeReportingVeto() and count the engine's answer, or throw RecordVetoedException where"
            + " the reply must name a record; a deliberate bare call is added to DELIBERATE with the reason")
        .isEmpty();
  }
}
