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
package com.arcadedb.bench.workload;

import com.arcadedb.test.support.TimeSeriesDatabaseWrapper.AggResult;
import org.junit.jupiter.api.Test;

import java.util.DoubleSummaryStatistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimeSeriesWorkloadTest {
  @Test
  void casesAreTheFiveOfTheSpec() {
    assertThat(new TimeSeriesWorkload().cases())
        .containsExactly("ingest-line-protocol", "ingest-sql", "range-query", "aggregate-query", "aggregate-continuous");
    assertThat(new TimeSeriesWorkload().name()).isEqualTo("timeseries");
  }

  @Test
  void sAndAboveMeetTheBatchRule() {
    for (final Scale scale : new Scale[] { Scale.S, Scale.M, Scale.L })
      assertThat(TimeSeriesWorkload.pointsPerRep(scale) / TimeSeriesWorkload.BATCH).as(scale.name()).isGreaterThanOrEqualTo(1_000);
  }

  @Test
  void aWindowOfOneHourFitsEverySensorSeries() {
    for (final Scale scale : Scale.values()) {
      assertThat(TimeSeriesWorkload.sensors(scale)).as(scale.name()).isGreaterThan(0);
      assertThat(TimeSeriesWorkload.pointsPerSensor(scale)).as(scale.name()).isGreaterThan(TimeSeriesWorkload.POINTS_PER_WINDOW);
    }
  }

  @Test
  void verificationMatchPasses() {
    assertThatCode(() -> TimeSeriesWorkload.checkVerification(new AggResult(24.5, 15.0, 34.0, 1000), 1000, 15.0, 34.0))
        .doesNotThrowAnyException();
  }

  @Test
  void wrongMinFailsTheCheck() {
    assertThatThrownBy(() -> TimeSeriesWorkload.checkVerification(new AggResult(24.5, 16.0, 34.0, 1000), 1000, 15.0, 34.0))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("expected").hasMessageContaining("16.0");
  }

  @Test
  void wrongMaxFailsTheCheck() {
    assertThatThrownBy(() -> TimeSeriesWorkload.checkVerification(new AggResult(24.5, 15.0, 33.0, 1000), 1000, 15.0, 34.0))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("expected").hasMessageContaining("33.0");
  }

  @Test
  void wrongCountFailsTheCheck() {
    assertThatThrownBy(() -> TimeSeriesWorkload.checkVerification(new AggResult(24.5, 15.0, 34.0, 999), 1000, 15.0, 34.0))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("expected").hasMessageContaining("999");
  }

  @Test
  void emptyAggregateFailsTheCheck() {
    assertThatThrownBy(() -> TimeSeriesWorkload.checkVerification(null, 1000, 15.0, 34.0))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("no rows");
  }

  @Test
  void theVerificationSeriesHasTheKnownExtremes() {
    final String body = TimeSeriesWorkload.verificationBody();
    assertThat(body.lines().count()).isEqualTo(TimeSeriesWorkload.VERIFY_POINTS);
    assertThat(body).startsWith("sensor,sensor_id=verify,region=eu-west temperature=15.0,");
    final DoubleSummaryStatistics temperatures = body.lines().mapToDouble(line -> {
      final int from = line.indexOf("temperature=") + "temperature=".length();
      return Double.parseDouble(line.substring(from, line.indexOf(',', from)));
    }).summaryStatistics();
    assertThat(temperatures.getCount()).isEqualTo(TimeSeriesWorkload.VERIFY_POINTS);
    assertThat(temperatures.getMin()).isEqualTo(15.0);
    assertThat(temperatures.getMax()).isEqualTo(34.0);
  }

  @Test
  void anEmptyAnswerFailsTheReadCase() {
    assertThatThrownBy(() -> TimeSeriesWorkload.checkAnswered("range-query", 3, 1_000)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("3 of 1000").hasMessageContaining("no rows");
    assertThatCode(() -> TimeSeriesWorkload.checkAnswered("range-query", 0, 1_000)).doesNotThrowAnyException();
  }

  @Test
  void only503IsRetriedAndOnlyWhileAttemptsRemain() {
    assertThat(LineProtocolClient.shouldRetry(503, 1)).isTrue();
    assertThat(LineProtocolClient.shouldRetry(503, LineProtocolClient.MAX_ATTEMPTS - 1)).isTrue();
    assertThat(LineProtocolClient.shouldRetry(503, LineProtocolClient.MAX_ATTEMPTS)).isFalse();
    assertThat(LineProtocolClient.shouldRetry(400, 1)).isFalse();
    assertThat(LineProtocolClient.shouldRetry(500, 1)).isFalse();
  }
}
