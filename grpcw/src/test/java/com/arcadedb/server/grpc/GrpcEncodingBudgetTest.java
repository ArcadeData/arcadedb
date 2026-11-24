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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultInternal;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The size of a message the value encoders may build: every value, text and byte string they encode is charged to a
 * {@link GrpcEncodingBudget} and the encoding stops once the message would be larger than the server accepts.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GrpcEncodingBudgetTest {

  private static final long ONE_MEGABYTE = 1024L * 1024L;

  /** {@code depth} nested lists, each holding the one below it twice. */
  private static List<Object> nestedTwice(final int depth) {
    List<Object> list = List.of(1);
    for (int i = 0; i < depth; i++)
      list = List.of(list, list);
    return list;
  }

  private static Map<String, Object> nestedMapTwice(final int depth) {
    Map<String, Object> map = Map.of("k", 1);
    for (int i = 0; i < depth; i++) {
      final Map<String, Object> next = new LinkedHashMap<>();
      next.put("x", map);
      next.put("y", map);
      map = next;
    }
    return map;
  }

  private static StatusRuntimeException refusal(final Runnable encoding) {
    final StatusRuntimeException refusal = catchThrowableOfType(StatusRuntimeException.class, encoding::run);
    assertThat(refusal).as("the encoding must be refused").isNotNull();
    return refusal;
  }

  @Test
  void aNestedListLargerThanTheCeilingIsRefused() {
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    final StatusRuntimeException refusal = refusal(() -> GrpcTypeConverter.toGrpcValue(nestedTwice(40), budget));

    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(refusal.getStatus().getDescription()).contains("too large to encode");
  }

  @Test
  void aNestedMapLargerThanTheCeilingIsRefused() {
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    final StatusRuntimeException refusal = refusal(() -> GrpcTypeConverter.toGrpcValue(nestedMapTwice(40), budget));

    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void anArrayOfNestedListsLargerThanTheCeilingIsRefused() {
    final Object[] array = { nestedTwice(40), nestedTwice(40) };
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    assertThat(refusal(() -> GrpcTypeConverter.toGrpcValue(array, budget)).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void aRefusedBudgetStaysRefused() {
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);
    refusal(() -> GrpcTypeConverter.toGrpcValue(nestedTwice(40), budget));

    // a caller that swallowed the first failure and went on with the next value of the message meets the same ceiling
    assertThat(refusal(() -> GrpcTypeConverter.toGrpcValue(1, budget)).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void aTextLargerThanTheCeilingIsRefused() {
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    assertThat(refusal(() -> GrpcTypeConverter.toGrpcValue("x".repeat((int) ONE_MEGABYTE + 1), budget)).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void aByteStringLargerThanTheCeilingIsRefused() {
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    assertThat(refusal(() -> GrpcTypeConverter.toGrpcValue(new byte[(int) ONE_MEGABYTE + 1], budget)).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void theKeysOfAMapAreCharged() {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("k".repeat((int) ONE_MEGABYTE + 1), 1);
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    assertThat(refusal(() -> GrpcTypeConverter.toGrpcValue(map, budget)).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void aLazySequenceLargerThanTheCeilingIsRefusedWhileItIsWalked() {
    // an Iterable that produces its elements on demand: the walk stops at the ceiling, it does not wait for the end
    final int[] produced = { 0 };
    final Iterable<Integer> endless = () -> new Iterator<>() {
      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public Integer next() {
        produced[0]++;
        return 1;
      }
    };
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    refusal(() -> GrpcTypeConverter.toGrpcValue(endless, budget));

    assertThat(produced[0]).isLessThanOrEqualTo((int) (ONE_MEGABYTE / GrpcEncodingBudget.VALUE_COST) + 1);
  }

  @Test
  void valuesThatFitAreEncodedAsTheyWereBefore() {
    final List<Object> list = new ArrayList<>();
    for (int i = 0; i < 1000; i++)
      list.add(i);
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", "value");
    map.put("list", list);
    map.put("nested", Map.of("a", List.of(1, 2, 3)));
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(map, budget);

    assertThat(encoded).isEqualTo(GrpcTypeConverter.toGrpcValue(map));
    assertThat(encoded.getMapValue().getEntriesOrThrow("list").getListValue().getValuesCount()).isEqualTo(1000);
  }

  @Test
  void aNestedListBelowTheCeilingStillEncodes() {
    // 2^10 leaves: far below the ceiling
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(nestedTwice(10), new GrpcEncodingBudget(ONE_MEGABYTE));

    assertThat(encoded.getListValue().getValuesCount()).isEqualTo(2);
  }

  @Test
  void theCeilingFollowsTheConfiguredMessageSize() {
    assertThat(GrpcEncodingBudget.ofMegabytes(1)).isNotNull();
    // the smallest the setting allows is 1 MB, whatever a misconfigured value says
    final GrpcEncodingBudget smallest = GrpcEncodingBudget.ofMegabytes(0);
    smallest.charge(ONE_MEGABYTE);
    assertThat(refusal(() -> smallest.charge(1)).getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void theDefaultCeilingIsTheDefaultOfTheMessageSizeSetting() {
    final long defaultBytes = ((Number) GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE.getDefValue()).longValue() * ONE_MEGABYTE;
    final GrpcEncodingBudget budget = GrpcEncodingBudget.ofDefault();

    budget.charge(defaultBytes);

    assertThat(refusal(() -> budget.charge(1)).getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void aRowLargerThanTheCeilingIsRefused() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    final Map<String, Object> columns = new LinkedHashMap<>();
    columns.put("a", nestedTwice(40));
    columns.put("b", 1);
    final ResultInternal row = new ResultInternal(columns);

    final StatusRuntimeException refusal = refusal(
        () -> service.convertResultToGrpcRecord(row, null, null, new GrpcEncodingBudget(ONE_MEGABYTE)));

    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void rowsThatShareABudgetAreChargedTogether() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L);
    final List<Object> list = new ArrayList<>();
    for (int i = 0; i < 10_000; i++)
      list.add(i);
    final ResultInternal row = new ResultInternal(Map.of("a", list));
    // one row costs about 10_000 * VALUE_COST bytes: it fits the ceiling, a hundred of them in one message do not
    final GrpcEncodingBudget budget = new GrpcEncodingBudget(ONE_MEGABYTE);

    service.convertResultToGrpcRecord(row, null, null, budget);

    final StatusRuntimeException refusal = refusal(() -> {
      for (int i = 0; i < 100; i++)
        service.convertResultToGrpcRecord(row, null, null, budget);
    });
    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }
}
