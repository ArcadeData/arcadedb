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
import com.arcadedb.bolt.packstream.PackStreamReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10771: the two rules of the Bolt state machine that hold for every request - in FAILED
 * everything but RESET and GOODBYE is IGNORED, and a request out of turn fails the connection - were restated in each of
 * thirteen handlers, and HELLO and LOGON kept neither: in FAILED they answered FAILURE, and out of turn they answered
 * FAILURE and left the connection serving.
 * <p>
 * Written as a census, not per handler, because a per-handler test is how two of thirteen were missed. The messages come
 * from {@link BoltMessage#parse}, the dispatcher's own source of truth, so a request added to the parser is in the census
 * the day it is added. The states each one is valid in come from the Bolt specification, written down here independently
 * of the production table, so the two have to agree rather than one being checked against itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10771BoltStateGuardTest {

  /** The states a request can arrive in, FAILED excepted (it has its own rule), as {@code BoltNetworkExecutor.State} names them. */
  private static final List<String> STATES = List.of("NEGOTIATION", "AUTHENTICATION", "READY", "STREAMING", "TX_READY",
      "TX_STREAMING");

  /**
   * Where each request is valid, per the Bolt specification plus ArcadeDB's two long-standing leniencies (LOGON in READY,
   * TELEMETRY anywhere). RESET and GOODBYE are valid everywhere and are not listed.
   */
  private static final Map<Byte, Set<String>> VALID_IN = Map.ofEntries(
      Map.entry(BoltMessage.HELLO, Set.of("NEGOTIATION", "AUTHENTICATION")),
      Map.entry(BoltMessage.LOGON, Set.of("AUTHENTICATION", "READY")),
      Map.entry(BoltMessage.LOGOFF, Set.of("READY")),
      Map.entry(BoltMessage.RUN, Set.of("READY", "TX_READY", "TX_STREAMING")),
      Map.entry(BoltMessage.PULL, Set.of("STREAMING", "TX_STREAMING")),
      Map.entry(BoltMessage.DISCARD, Set.of("STREAMING", "TX_STREAMING")),
      Map.entry(BoltMessage.BEGIN, Set.of("READY")),
      Map.entry(BoltMessage.COMMIT, Set.of("TX_READY")),
      Map.entry(BoltMessage.ROLLBACK, Set.of("TX_READY", "TX_STREAMING")),
      Map.entry(BoltMessage.ROUTE, Set.of("READY")),
      Map.entry(BoltMessage.TELEMETRY, Set.copyOf(STATES)));

  /** Every FAILED connection answers IGNORED to every request but RESET and GOODBYE, and stays FAILED. */
  @Test
  void aFailedConnectionIgnoresEveryRequestButResetAndGoodbye() throws Exception {
    for (final BoltMessage request : requestMessages()) {
      final byte signature = request.getSignature();
      if (signature == BoltMessage.RESET || signature == BoltMessage.GOODBYE)
        continue;

      final BoltHandlerProbe.Dispatch dispatch = BoltHandlerProbe.dispatch(request, "FAILED");
      assertThat(dispatch.reply()).as("%s in FAILED", BoltMessage.signatureName(signature)).isEqualTo(BoltMessage.IGNORED);
      assertThat(dispatch.stateAfter()).as("%s in FAILED", BoltMessage.signatureName(signature)).isEqualTo("FAILED");
    }
  }

  /** RESET is the way out of FAILED, and GOODBYE ends the connection from there too. */
  @Test
  void resetAndGoodbyeAreHonouredInFailed() throws Exception {
    final BoltHandlerProbe.Dispatch reset = BoltHandlerProbe.dispatch(parse(BoltMessage.RESET), "FAILED");
    assertThat(reset.reply()).isEqualTo(BoltMessage.SUCCESS);
    assertThat(reset.stateAfter()).as("no user is bound, so RESET goes back to AUTHENTICATION").isEqualTo("AUTHENTICATION");

    assertThat(BoltHandlerProbe.dispatch(parse(BoltMessage.GOODBYE), "FAILED").stateAfter()).isEqualTo("DISCONNECTED");
  }

  /** A request delivered in a state outside its valid set is a protocol error AND fails the connection, for every request. */
  @Test
  void aRequestOutOfTurnFailsTheConnection() throws Exception {
    for (final BoltMessage request : requestMessages()) {
      final byte signature = request.getSignature();
      if (signature == BoltMessage.RESET || signature == BoltMessage.GOODBYE)
        continue;

      final Set<String> validIn = VALID_IN.get(signature);
      assertThat(validIn).as("BoltMessage.parse knows %s: write down the states it is valid in", BoltMessage.signatureName(signature))
          .isNotNull();

      for (final String state : STATES) {
        if (validIn.contains(state))
          continue;

        final BoltHandlerProbe.Dispatch dispatch = BoltHandlerProbe.dispatch(request, state);
        assertThat(dispatch.reply()).as("%s in %s", BoltMessage.signatureName(signature), state).isEqualTo(BoltMessage.FAILURE);
        assertThat(dispatch.code()).as("%s in %s", BoltMessage.signatureName(signature), state)
            .isEqualTo(BoltException.PROTOCOL_ERROR);
        assertThat(dispatch.stateAfter()).as("%s in %s must leave the connection FAILED", BoltMessage.signatureName(signature),
            state).isEqualTo("FAILED");
      }
    }
  }

  /** The two cases of the report, pinned by name. */
  @Test
  void helloInReadyAndLogonInStreamingFailTheConnection() throws Exception {
    final BoltHandlerProbe.Dispatch hello = BoltHandlerProbe.dispatch(parse(BoltMessage.HELLO), "READY");
    assertThat(hello.reply()).isEqualTo(BoltMessage.FAILURE);
    assertThat(hello.stateAfter()).isEqualTo("FAILED");

    final BoltHandlerProbe.Dispatch logon = BoltHandlerProbe.dispatch(parse(BoltMessage.LOGON), "STREAMING");
    assertThat(logon.reply()).isEqualTo(BoltMessage.FAILURE);
    assertThat(logon.stateAfter()).isEqualTo("FAILED");

    assertThat(BoltHandlerProbe.dispatch(parse(BoltMessage.HELLO), "FAILED").reply()).isEqualTo(BoltMessage.IGNORED);
    assertThat(BoltHandlerProbe.dispatch(parse(BoltMessage.LOGON), "FAILED").reply()).isEqualTo(BoltMessage.IGNORED);
  }

  /**
   * One message per request signature {@link BoltMessage} declares and {@link BoltMessage#parse} accepts: the response
   * signatures are declared in the same class and refused by the parser, which is how they are told apart.
   */
  private static List<BoltMessage> requestMessages() throws Exception {
    final List<BoltMessage> requests = new ArrayList<>();
    for (final Field field : BoltMessage.class.getDeclaredFields()) {
      if (field.getType() != byte.class || !Modifier.isStatic(field.getModifiers()) || !Modifier.isPublic(field.getModifiers()))
        continue;
      final BoltMessage message = parse(field.getByte(null));
      if (message != null)
        requests.add(message);
    }
    assertThat(requests).as("the census found no request at all").hasSizeGreaterThanOrEqualTo(13);
    return requests;
  }

  /**
   * Parses {@code signature} with the first field shape its parser accepts, or answers null for a signature the parser does
   * not know (a response).
   */
  private static BoltMessage parse(final byte signature) {
    final List<List<Object>> shapes = List.of(List.of(), List.of(Map.of()), List.of("RETURN 1", Map.of(), Map.of()));
    for (final List<Object> fields : shapes) {
      try {
        return BoltMessage.parse(new PackStreamReader.StructureValue(signature, fields));
      } catch (final IOException e) {
        return null;
      } catch (final RuntimeException e) {
        // NOT THIS SHAPE: TRY THE NEXT ONE
      }
    }
    throw new AssertionError("no field shape parses signature " + BoltMessage.signatureName(signature));
  }
}
