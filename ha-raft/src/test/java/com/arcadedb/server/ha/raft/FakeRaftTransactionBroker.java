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
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.CallLog;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link RaftTransactionBroker} that submits nothing to Raft: it records every entry it is asked to replicate on a
 * {@link CallLog} and answers what the test set (issue #9464). It replaces a Mockito mock of the broker, whose effect is
 * the outgoing call itself - there is no state a test could read back instead.
 * <p>
 * The real broker's group committer is stopped as the fake is built, so it holds no thread. Overloads that delegate to
 * a longer form (as the 3-argument {@code replicateTransaction} does) are recorded under that form's name. A method
 * with no answer set answers what an unstubbed mock did: {@code 0} for an index, {@code false} for "applied".
 */
public class FakeRaftTransactionBroker extends RaftTransactionBroker {
  /** What each recorded method answers; {@code Void} for the ones that answer nothing. */
  private static final Map<String, Class<?>> RETURN_TYPES = Map.ofEntries(Map.entry("replicateTransaction", Long.class),
      Map.entry("replicateSchema", Void.class), Map.entry("replicateSealedChunk", Void.class),
      Map.entry("replicateSchemaInstalment", Void.class), Map.entry("replicateInstallDatabase", Void.class),
      Map.entry("replicateDropDatabase", Long.class), Map.entry("replicateBootstrapFingerprint", Void.class),
      Map.entry("replicateSecurityUsers", Boolean.class), Map.entry("replicateSecurityGroups", Boolean.class),
      Map.entry("replicateSecurityApiTokens", Boolean.class), Map.entry("stop", Void.class),
      Map.entry("transferPendingTo", Integer.class));

  private final CallLog         log;
  private final CallLog.Answers answers = new CallLog.Answers(RETURN_TYPES.keySet());

  /** A broker on its own call log. */
  public FakeRaftTransactionBroker() {
    this(new CallLog());
  }

  /** A broker recording on {@code log}, which other fakes may share. */
  public FakeRaftTransactionBroker(final CallLog log) {
    super(null, Quorum.MAJORITY, 1_000L);
    // The real committer started a flusher thread: nothing here ever submits to it
    super.stop();
    this.log = log;
  }

  public CallLog log() {
    return log;
  }

  /** The argument lists of every {@code method} call, in arrival order. */
  public List<List<Object>> calls(final String method) {
    return log.argsOf(this, method);
  }

  /** {@code method} answers {@code value} from now on; a value of the wrong type is refused here, where it is written. */
  public FakeRaftTransactionBroker returns(final String method, final Object value) {
    final Class<?> expected = RETURN_TYPES.get(method);
    if (expected != null && expected != Void.class && !expected.isInstance(value))
      throw new IllegalArgumentException("'" + method + "' answers a " + expected.getSimpleName() + ", not "
          + (value == null ? "null" : value.getClass().getSimpleName() + " (write 42L for a long)"));
    return on(method, args -> value);
  }

  /** {@code method} throws {@code failure} from now on. */
  public FakeRaftTransactionBroker fails(final String method, final RuntimeException failure) {
    return on(method, args -> {
      throw failure;
    });
  }

  /** {@code method} runs {@code answer} on its arguments from now on. */
  public FakeRaftTransactionBroker on(final String method, final Function<Object[], Object> answer) {
    answers.set(method, answer);
    return this;
  }

  private Object call(final String method, final Supplier<Object> fallback, final Object... args) {
    log.record(this, method, args);
    final Function<Object[], Object> answer = answers.get(method);
    final Object value = answer != null ? answer.apply(args) : fallback.get();
    // An on(...) function is checked at the call: a wrong type or a null would otherwise surface as a bare cast failure
    final Class<?> expected = RETURN_TYPES.get(method);
    if (expected != Void.class && !expected.isInstance(value))
      throw new IllegalStateException("The answer set for '" + method + "' must be a " + expected.getSimpleName()
          + ", it gave " + (value == null ? "null" : value.getClass().getSimpleName()));
    return value;
  }

  @Override
  public long replicateTransaction(final String dbName, final byte[] walData, final Map<Integer, Integer> bucketDeltas,
      final long preparedAtIndex) {
    return (Long) call("replicateTransaction", () -> 0L, dbName, walData, bucketDeltas, preparedAtIndex);
  }

  @Override
  public void replicateSchema(final String dbName, final String schemaJson, final Map<Integer, String> filesToAdd,
      final Map<Integer, String> filesToRemove, final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final List<RaftLogEntryCodec.TsSealedBlob> sealedFileBlobs, final List<RaftLogEntryCodec.TsSealedChunk> sealedFileChunks,
      final SchemaDelta.Payload schemaDelta) {
    call("replicateSchema", () -> null, dbName, schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas,
        sealedFileBlobs, sealedFileChunks, schemaDelta);
  }

  @Override
  public void replicateSealedChunk(final String dbName, final RaftLogEntryCodec.TsSealedChunk chunk) {
    call("replicateSealedChunk", () -> null, dbName, chunk);
  }

  @Override
  public void replicateSchemaInstalment(final String dbName, final Map<Integer, String> filesToAdd,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas) {
    call("replicateSchemaInstalment", () -> null, dbName, filesToAdd, walEntries, bucketDeltas);
  }

  @Override
  public void replicateInstallDatabase(final String dbName, final boolean forceSnapshot) {
    call("replicateInstallDatabase", () -> null, dbName, forceSnapshot);
  }

  @Override
  public long replicateDropDatabase(final String dbName) {
    return (Long) call("replicateDropDatabase", () -> 0L, dbName);
  }

  @Override
  public void replicateBootstrapFingerprint(final String dbName, final String fingerprint, final long lastTxId) {
    call("replicateBootstrapFingerprint", () -> null, dbName, fingerprint, lastTxId);
  }

  @Override
  public boolean replicateSecurityUsers(final String usersJson, final String expectedFingerprint) {
    return (Boolean) call("replicateSecurityUsers", () -> false, usersJson, expectedFingerprint);
  }

  @Override
  public boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
    return (Boolean) call("replicateSecurityGroups", () -> false, groupsJson, expectedFingerprint);
  }

  @Override
  public boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
    return (Boolean) call("replicateSecurityApiTokens", () -> false, apiTokensJson, expectedFingerprint);
  }

  @Override
  public void stop() {
    call("stop", () -> null);
  }

  @Override
  public int transferPendingTo(final RaftTransactionBroker target) {
    return (Integer) call("transferPendingTo", () -> 0, target);
  }
}
