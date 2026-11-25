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

import com.arcadedb.database.Database;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.HAServerPlugin;
import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The parsing, the bookmark and the clearing of {@link GrpcReadConsistencyInterceptor} without a cluster (issue #11948):
 * {@code GrpcFollowerForwardingIT.ReadYourWrites} proves the follower read end to end, this pins the rules it relies on.
 */
class GrpcReadConsistencyInterceptorTest {

  @Test
  void aMissingOrBlankConsistencyLeavesTheConfiguredOne() {
    assertThat(GrpcReadConsistencyInterceptor.parseConsistency(null)).isNull();
    assertThat(GrpcReadConsistencyInterceptor.parseConsistency("")).isNull();
    assertThat(GrpcReadConsistencyInterceptor.parseConsistency("   ")).isNull();
  }

  @Test
  void theConsistencyIsReadInAnyCaseAndTrimmed() {
    assertThat(GrpcReadConsistencyInterceptor.parseConsistency("eventual")).isEqualTo(Database.READ_CONSISTENCY.EVENTUAL);
    assertThat(GrpcReadConsistencyInterceptor.parseConsistency(" Read_Your_Writes ")).isEqualTo(
        Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    assertThat(GrpcReadConsistencyInterceptor.parseConsistency("LINEARIZABLE")).isEqualTo(Database.READ_CONSISTENCY.LINEARIZABLE);
  }

  @Test
  void anUnknownConsistencyIsRefusedWithoutEchoingIt() {
    assertThatThrownBy(() -> GrpcReadConsistencyInterceptor.parseConsistency("strong<script>"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("strong");
  }

  @Test
  void aMissingOrBlankBookmarkIsNoBookmark() {
    assertThat(GrpcReadConsistencyInterceptor.parseReadAfter(null)).isEqualTo(-1L);
    assertThat(GrpcReadConsistencyInterceptor.parseReadAfter("")).isEqualTo(-1L);
    assertThat(GrpcReadConsistencyInterceptor.parseReadAfter("  ")).isEqualTo(-1L);
  }

  @Test
  void aBookmarkIsTrimmedAndANegativeOneMeansNone() {
    assertThat(GrpcReadConsistencyInterceptor.parseReadAfter(" 42 ")).isEqualTo(42L);
    assertThat(GrpcReadConsistencyInterceptor.parseReadAfter("-1")).isEqualTo(-1L);
  }

  @Test
  void aBookmarkThatIsNotANumberIsRefusedWithoutEchoingIt() {
    assertThatThrownBy(() -> GrpcReadConsistencyInterceptor.parseReadAfter("12abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("12abc");
  }

  @Test
  void aMalformedHeaderClosesTheCallWithInvalidArgumentBeforeTheHandler() {
    final Metadata headers = new Metadata();
    headers.put(GrpcReadConsistencyInterceptor.READ_AFTER_HEADER, "secret-value");
    final RecordingCall call = new RecordingCall();
    final boolean[] started = { false };

    new GrpcReadConsistencyInterceptor().interceptCall(call, headers, (c, h) -> {
      started[0] = true;
      return new ServerCall.Listener<>() {
      };
    });

    assertThat(started[0]).isFalse();
    assertThat(call.closedWith.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(call.closedWith.getDescription()).doesNotContain("secret-value");
  }

  @Test
  void theBookmarkGoesInTheResponseHeaders() {
    final FakeReplicatedDatabase db = new FakeReplicatedDatabase(17);
    final RecordingCall call = new RecordingCall();

    final ServerCall.Listener<String> listener = start(call, new Metadata(), c -> {
      GrpcReadConsistencyInterceptor.current().applyTo(db, null);
      c.sendHeaders(new Metadata());
      c.close(Status.OK, new Metadata());
    });
    listener.onHalfClose();

    assertThat(call.sentHeaders.get(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER)).isEqualTo("17");
    assertThat(call.closedTrailers.get(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER)).isNull();
  }

  @Test
  void aCallClosedWithoutHeadersCarriesTheBookmarkInItsTrailers() {
    // A failure still has to tell the client what it can read back of the commits made before it (issue #5862)
    final FakeReplicatedDatabase db = new FakeReplicatedDatabase(23);
    final RecordingCall call = new RecordingCall();

    final ServerCall.Listener<String> listener = start(call, new Metadata(), c -> {
      GrpcReadConsistencyInterceptor.current().bind(db);
      c.close(Status.ABORTED, new Metadata());
    });
    listener.onHalfClose();

    assertThat(call.sentHeaders).isNull();
    assertThat(call.closedWith.getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(call.closedTrailers.get(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER)).isEqualTo("23");
  }

  @Test
  void aCallThatReachedNoReplicatedDatabaseCarriesNoBookmark() {
    final RecordingCall call = new RecordingCall();

    final ServerCall.Listener<String> listener = start(call, new Metadata(), c -> {
      c.sendHeaders(new Metadata());
      c.close(Status.OK, new Metadata());
    });
    listener.onHalfClose();

    assertThat(call.sentHeaders.containsKey(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER)).isFalse();
    assertThat(call.closedTrailers.containsKey(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER)).isFalse();
  }

  @Test
  void theRequestedConsistencyAndBookmarkAreSetAndClearedAfterEveryCallback() {
    final Metadata headers = new Metadata();
    headers.put(GrpcReadConsistencyInterceptor.READ_CONSISTENCY_HEADER, "read_your_writes");
    headers.put(GrpcReadConsistencyInterceptor.READ_AFTER_HEADER, "99");
    final FakeReplicatedDatabase db = new FakeReplicatedDatabase(100);
    final RecordingCall call = new RecordingCall();

    final ServerCall.Listener<String> listener = start(call, headers, c -> GrpcReadConsistencyInterceptor.current().applyTo(db, "eventual"));

    listener.onMessage("m");
    assertThat(db.set).containsExactly(Database.READ_CONSISTENCY.READ_YOUR_WRITES + "@99");
    assertThat(db.cleared).as("onMessage clears what it set").isEqualTo(1);
    listener.onHalfClose();
    assertThat(db.cleared).as("onHalfClose clears what it set").isEqualTo(2);
    listener.onReady();
    assertThat(db.cleared).as("onReady clears").isEqualTo(3);
    listener.onCancel();
    assertThat(db.cleared).as("onCancel clears").isEqualTo(4);
    listener.onComplete();
    assertThat(db.cleared).as("onComplete clears").isEqualTo(5);
  }

  @Test
  void aCallbackThatThrowsStillClears() {
    final FakeReplicatedDatabase db = new FakeReplicatedDatabase(1);
    final ServerCall.Listener<String> listener = start(new RecordingCall(), new Metadata(), c -> {
      GrpcReadConsistencyInterceptor.current().applyTo(db, "linearizable");
      throw new IllegalStateException("boom");
    });

    assertThatThrownBy(listener::onHalfClose).isInstanceOf(IllegalStateException.class);
    assertThat(db.set).containsExactly(Database.READ_CONSISTENCY.LINEARIZABLE + "@-1");
    assertThat(db.cleared).isEqualTo(1);
  }

  @Test
  void withoutAHeaderTheConfiguredConsistencyApplies() {
    final FakeReplicatedDatabase db = new FakeReplicatedDatabase(1);
    final ServerCall.Listener<String> listener = start(new RecordingCall(), new Metadata(),
        c -> GrpcReadConsistencyInterceptor.current().applyTo(db, "linearizable"));
    listener.onHalfClose();

    assertThat(db.set).containsExactly(Database.READ_CONSISTENCY.LINEARIZABLE + "@-1");
  }

  @Test
  void withoutAHeaderOrAConfiguredConsistencyNothingIsSet() {
    final FakeReplicatedDatabase db = new FakeReplicatedDatabase(1);
    final ServerCall.Listener<String> listener = start(new RecordingCall(), new Metadata(),
        c -> GrpcReadConsistencyInterceptor.current().applyTo(db, null));
    listener.onHalfClose();

    assertThat(db.set).isEmpty();
  }

  @Test
  void workHandedToAnotherThreadClearsWhatItSetThere() throws Exception {
    // A bidirectional stream resolves its database on a thread of its own, where no listener callback runs
    final FakeReplicatedDatabase db = new FakeReplicatedDatabase(1);
    final AtomicReference<Context> callContext = new AtomicReference<>();
    final ServerCall.Listener<String> listener = start(new RecordingCall(), new Metadata(), c -> callContext.set(Context.current()));
    listener.onHalfClose();
    final int clearedByCallbacks = db.cleared;

    final Thread other = new Thread(callContext.get().wrap(GrpcReadConsistencyInterceptor.clearingAfter(
        () -> GrpcReadConsistencyInterceptor.current().applyTo(db, "linearizable"))));
    other.start();
    other.join();

    assertThat(db.set).containsExactly(Database.READ_CONSISTENCY.LINEARIZABLE + "@-1");
    assertThat(db.cleared).isEqualTo(clearedByCallbacks + 1);
  }

  @Test
  void outsideACallThereIsNoState() {
    assertThat(GrpcReadConsistencyInterceptor.current()).isNull();
  }

  /** Starts a call through the interceptor whose listener runs {@code onCallback} in {@code onMessage} and {@code onHalfClose}. */
  private static ServerCall.Listener<String> start(final RecordingCall call, final Metadata headers,
      final Consumer<ServerCall<String, String>> onCallback) {
    final ServerCallHandler<String, String> handler = (c, h) -> new ServerCall.Listener<>() {
      @Override
      public void onMessage(final String message) {
        onCallback.accept(c);
      }

      @Override
      public void onHalfClose() {
        onCallback.accept(c);
      }
    };
    return new GrpcReadConsistencyInterceptor().interceptCall(call, headers, handler);
  }

  private static final class RecordingCall extends ServerCall<String, String> {
    Metadata sentHeaders;
    Status   closedWith;
    Metadata closedTrailers;

    @Override
    public void request(final int numMessages) {
    }

    @Override
    public void sendHeaders(final Metadata headers) {
      sentHeaders = headers;
    }

    @Override
    public void sendMessage(final String message) {
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      closedWith = status;
      closedTrailers = trailers;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public Attributes getAttributes() {
      return Attributes.EMPTY;
    }

    @Override
    public MethodDescriptor<String, String> getMethodDescriptor() {
      return null;
    }
  }

  private static final class FakeReplicatedDatabase implements HAReplicatedDatabase {
    private final long         appliedIndex;
    private final List<String> set = new ArrayList<>();
    private       int          cleared;

    FakeReplicatedDatabase(final long appliedIndex) {
      this.appliedIndex = appliedIndex;
    }

    @Override
    public void setReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
      set.add(consistency + "@" + readAfterIndex);
    }

    @Override
    public void clearReadConsistencyContext() {
      cleared++;
    }

    @Override
    public long getLastAppliedIndex() {
      return appliedIndex;
    }

    @Override
    public void createInReplicas() {
    }

    @Override
    public void dropInReplicas() {
    }

    @Override
    public HAServerPlugin.QUORUM getQuorum() {
      return null;
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public String getLeaderHttpAddress() {
      return null;
    }
  }
}
