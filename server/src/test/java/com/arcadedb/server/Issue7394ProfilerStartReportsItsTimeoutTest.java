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
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7394 item 4: a recording started with no timeout was documented as open-ended and was not.
 * <p>
 * Three surfaces said that a {@code timeoutSeconds} of zero records until the profiler is stopped - this
 * class's javadoc, {@code ProfilerStartRequest.timeout_seconds} in the proto, and
 * {@code RemoteGrpcServer.profilerStart}. {@link ServerQueryProfiler#start(int)} does
 * {@code timeoutSec > 0 ? timeoutSec : DEFAULT_TIMEOUT_SECONDS}, so both spellings of "no timeout" get 60
 * seconds and an auto-stop timer, and a caller planning a long capture found its recording already stopped.
 * <p>
 * The bounded default stays - an unattended recording keeps every server query on the
 * {@code ProfilingResultSet} wrapping path, so "records forever unless someone remembers" is not the safer
 * behaviour - and is reported instead. The start response now carries the timeout that will actually apply,
 * so a caller can tell when its recording ends without having to know the default.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
 */
class Issue7394ProfilerStartReportsItsTimeoutTest {

  private ArcadeDBServer      server;
  private ServerQueryProfiler profiler;
  private ServerControlPlane  controlPlane;

  @BeforeEach
  void setup(@TempDir final Path rootPath) {
    server = mock(ArcadeDBServer.class);
    // stop() persists the run under <rootPath>/profiler. Unstubbed, getRootPath() answers null and that path
    // becomes the literal "null/profiler", dropped into whatever the working directory happens to be.
    when(server.getRootPath()).thenReturn(rootPath.toString());
    profiler = new ServerQueryProfiler(server);
    when(server.getQueryProfiler()).thenReturn(profiler);
    controlPlane = new ServerControlPlane(server);
  }

  @AfterEach
  void teardown() {
    if (profiler != null && profiler.isRecording())
      profiler.stop();
  }

  @Test
  void aZeroTimeoutReportsTheDefaultItActuallyGets() {
    final JSONObject response = controlPlane.profilerStart(0);

    assertThat(response.getBoolean("recording")).isTrue();
    assertThat(response.getInt("timeoutSeconds")).isEqualTo(60);
    assertThat(profiler.getTimeoutSeconds()).isEqualTo(60);
  }

  @Test
  void aNegativeTimeoutReportsTheSameDefault() {
    final JSONObject response = controlPlane.profilerStart(-1);

    assertThat(response.getInt("timeoutSeconds")).isEqualTo(60);
  }

  @Test
  void anExplicitTimeoutIsReportedBackUnchanged() {
    final JSONObject response = controlPlane.profilerStart(300);

    assertThat(response.getInt("timeoutSeconds")).isEqualTo(300);
    assertThat(profiler.getTimeoutSeconds()).isEqualTo(300);
  }

  /**
   * Starting an already-recording profiler is a no-op, so the reported timeout has to be the one the live
   * recording is running under - not the one this call asked for and did not get.
   */
  @Test
  void aSecondStartReportsTheTimeoutOfTheRecordingThatIsActuallyRunning() {
    controlPlane.profilerStart(300);

    final JSONObject response = controlPlane.profilerStart(5);

    assertThat(response.getInt("timeoutSeconds")).isEqualTo(300);
    assertThat(profiler.getTimeoutSeconds()).isEqualTo(300);
  }
}
