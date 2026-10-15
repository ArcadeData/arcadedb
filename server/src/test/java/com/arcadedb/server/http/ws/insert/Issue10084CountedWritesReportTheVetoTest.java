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
package com.arcadedb.server.http.ws.insert;

import com.arcadedb.server.CountedWriteScan;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10084: the {@code /ws} insert session replies with counters, so no write it makes may go through the {@code void}
 * {@code save()} or {@code newEdge()}, which throw away (or raise) the answer a before-event listener's veto gives. Scanning the
 * session's source, like {@code Issue9870BoltFailureWiringTest} scans the Bolt executor, stops a new arm from bypassing
 * {@code saveReportingVeto()} / {@code newEdgeReportingVeto()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10084CountedWritesReportTheVetoTest {
  @Test
  void theInsertSessionWritesOnlyThroughTheReportingForms() throws IOException {
    assertThat(CountedWriteScan.offendersIn("src/main/java/com/arcadedb/server/http/ws/insert/WebSocketInsertSession.java", null))
        .as("lines of the /ws insert session that write through save() or newEdge(), which cannot report a listener's veto: write through"
            + " saveReportingVeto() or GraphEngine.newEdgeReportingVeto() and count the engine's answer (inserted, or ignored when false)")
        .isEmpty();
  }

  @Test
  void theScanFlagsABareWriteAndLeavesCommentsAndAllowedLinesAlone() {
    final String source = """
        document.save();
        // the old arm called document.save() here
        /* and vertex.save(),
           then fromVertex.newEdge(type, to, props) */
        final boolean created = document.saveReportingVeto();
        fromVertex.newEdge(type, to, props);
        graphEngine.newEdgeReportingVeto(fromVertex, type, to, props);
        batch.newEdge(from, type, to);
        """;

    assertThat(CountedWriteScan.offenders(source, null)).containsExactly(1, 6, 8);
    assertThat(CountedWriteScan.offenders(source, Pattern.compile("batch\\.newEdge\\("))).containsExactly(1, 6);
  }
}
