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
package com.arcadedb.bolt;

import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.message.RouteMessage;
import com.arcadedb.bolt.message.RunMessage;
import com.arcadedb.bolt.packstream.PackStreamReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalTime;
import java.util.AbstractList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a request costs before the connection's state has been checked: {@link BoltMessage#parse} runs for every request the
 * client sends, in every state, so it may only pick the decoded fields apart. Turning the parameters of a RUN into engine
 * values, or rendering a field as text, belongs to the handler of a request the state accepts.
 * <p>
 * Whether a value was converted is observed through the fields of a structure, which the converter reads and nothing else
 * does, so the assertions count reads instead of timing anything.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BoltRequestParsingOrderTest {
  private static final byte SIG_LOCAL_TIME = 0x74; // 't'

  /** The fields of a structure, counting how often anything looked at them. */
  private static final class WatchedFields extends AbstractList<Object> {
    private final List<Object> fields;
    private       int          reads;

    private WatchedFields(final Object... fields) {
      this.fields = List.of(fields);
    }

    @Override
    public Object get(final int index) {
      ++reads;
      return fields.get(index);
    }

    @Override
    public int size() {
      ++reads;
      return fields.size();
    }
  }

  /** A text-bearing value that records being rendered. */
  private static final class WatchedText {
    private int renders;

    @Override
    public String toString() {
      ++renders;
      return "text";
    }
  }

  @Test
  void parsingARunLeavesItsParametersAsDecoded() throws Exception {
    final WatchedFields direct = new WatchedFields(1L);
    final WatchedFields nested = new WatchedFields(-1L);
    final RunMessage run = parseRun(runParameters(direct, nested));

    assertThat(direct.reads).as("a parameter must not be converted by parse()").isZero();
    assertThat(nested.reads).as("a nested parameter must not be converted by parse()").isZero();
    assertThat(run.getQuery()).isEqualTo("RETURN 1");
  }

  @Test
  void theParametersOfARunAreConvertedWhenTheyAreRead() throws Exception {
    final WatchedFields direct = new WatchedFields(1L);
    final WatchedFields malformed = new WatchedFields(-1L);
    final RunMessage run = parseRun(runParameters(direct, malformed));

    final Map<String, Object> parameters = run.getParameters();

    assertThat(parameters).containsEntry("direct", LocalTime.ofNanoOfDay(1));
    assertThat(direct.reads).isPositive();
    // a value that does not convert stays as it came, exactly as before
    final List<?> list = (List<?>) parameters.get("nested");
    assertThat(((List<?>) list.get(0)).get(0)).isInstanceOf(PackStreamReader.StructureValue.class);
    assertThat(run.getParameters()).as("converted once, then kept").isSameAs(parameters);
  }

  @Test
  void aRunTheStateRefusesIsNotConverted() throws Exception {
    for (final String state : List.of("AUTHENTICATION", "NEGOTIATION", "FAILED")) {
      final WatchedFields direct = new WatchedFields(1L);
      final WatchedFields nested = new WatchedFields(-1L);

      final BoltHandlerProbe.Dispatch dispatch = BoltHandlerProbe.dispatch(parseRun(runParameters(direct, nested)), state);

      if ("FAILED".equals(state))
        assertThat(dispatch.reply()).as("RUN in %s", state).isEqualTo(BoltMessage.IGNORED);
      else {
        assertThat(dispatch.reply()).as("RUN in %s", state).isEqualTo(BoltMessage.FAILURE);
        assertThat(dispatch.code()).as("RUN in %s", state).isEqualTo(BoltException.PROTOCOL_ERROR);
      }
      assertThat(dispatch.stateAfter()).as("RUN in %s", state).isEqualTo("FAILED");
      assertThat(direct.reads).as("RUN in %s must not convert its parameters", state).isZero();
      assertThat(nested.reads).as("RUN in %s must not convert its nested parameters", state).isZero();
    }
  }

  @Test
  void parsingARouteDoesNotRenderItsDatabaseEntry() throws Exception {
    final WatchedText text = new WatchedText();
    final Map<String, Object> extra = new LinkedHashMap<>();
    extra.put("db", List.of(List.of(text)));

    final RouteMessage route = (RouteMessage) BoltMessage.parse(
        new PackStreamReader.StructureValue(BoltMessage.ROUTE, List.of(Map.of(), List.of(), extra)));

    assertThat(text.renders).as("a ROUTE field must not be rendered as text by parse()").isZero();
    assertThat(route.getDatabase()).isNull();
  }

  @Test
  void parsingARouteKeepsTheDatabaseName() throws Exception {
    final RouteMessage current = (RouteMessage) BoltMessage.parse(
        new PackStreamReader.StructureValue(BoltMessage.ROUTE, List.of(Map.of(), List.of(), Map.of("db", "shop"))));
    assertThat(current.getDatabase()).isEqualTo("shop");

    final RouteMessage legacy = (RouteMessage) BoltMessage.parse(
        new PackStreamReader.StructureValue(BoltMessage.ROUTE, List.of(Map.of(), List.of(), "shop")));
    assertThat(legacy.getDatabase()).isEqualTo("shop");
  }

  private static Map<String, Object> runParameters(final WatchedFields direct, final WatchedFields nested) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("direct", new PackStreamReader.StructureValue(SIG_LOCAL_TIME, direct));
    parameters.put("nested", List.of(List.of(new PackStreamReader.StructureValue(SIG_LOCAL_TIME, nested))));
    return parameters;
  }

  private static RunMessage parseRun(final Map<String, Object> parameters) throws IOException {
    return (RunMessage) BoltMessage.parse(
        new PackStreamReader.StructureValue(BoltMessage.RUN, List.of("RETURN 1", parameters, Map.of())));
  }
}
