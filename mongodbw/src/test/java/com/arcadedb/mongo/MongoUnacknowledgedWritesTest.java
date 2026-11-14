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
package com.arcadedb.mongo;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two handlers of issue #11375 on their own: the decoder clears {@code moreToCome} and remembers the request, the
 * filter drops that request's reply and nothing else.
 */
class MongoUnacknowledgedWritesTest {

  private static final int OP_MSG   = 2013;
  private static final int OP_QUERY = 2004;

  @Test
  void opMsgWithMoreToComeIsPassedOnWithTheFlagClearedAndRemembered() {
    final Set<Integer> unacknowledged = new HashSet<>();
    final EmbeddedChannel channel = new EmbeddedChannel(new MongoUnacknowledgedWrites.MoreToComeDecoder(unacknowledged));

    channel.writeInbound(message(42, OP_MSG, 1 << 1));

    final ByteBuf passed = channel.readInbound();
    assertThat(passed.getIntLE(16)).isZero();
    assertThat(passed.getIntLE(4)).isEqualTo(42);
    assertThat(unacknowledged).containsExactly(42);
    passed.release();
  }

  @Test
  void otherMessagesArePassedOnUnchanged() {
    final Set<Integer> unacknowledged = new HashSet<>();
    final EmbeddedChannel channel = new EmbeddedChannel(new MongoUnacknowledgedWrites.MoreToComeDecoder(unacknowledged));

    // checksumPresent alone, and an OP_QUERY whose flags happen to have bit 1 set (tailable cursor)
    channel.writeInbound(message(1, OP_MSG, 1));
    channel.writeInbound(message(2, OP_QUERY, 1 << 1));

    final ByteBuf first = channel.readInbound();
    final ByteBuf second = channel.readInbound();
    assertThat(first.getIntLE(16)).isEqualTo(1);
    assertThat(second.getIntLE(16)).isEqualTo(1 << 1);
    assertThat(unacknowledged).isEmpty();
    first.release();
    second.release();
  }

  @Test
  void messagesSplitAcrossReadsAreFramed() {
    final Set<Integer> unacknowledged = new HashSet<>();
    final EmbeddedChannel channel = new EmbeddedChannel(new MongoUnacknowledgedWrites.MoreToComeDecoder(unacknowledged));
    final ByteBuf whole = Unpooled.wrappedBuffer(message(7, OP_MSG, 1 << 1), message(8, OP_MSG, 0));

    channel.writeInbound(whole.readRetainedSlice(10));
    assertThat((Object) channel.readInbound()).isNull();
    channel.writeInbound(whole.readRetainedSlice(whole.readableBytes()));
    whole.release();

    final ByteBuf first = channel.readInbound();
    final ByteBuf second = channel.readInbound();
    assertThat(first.getIntLE(4)).isEqualTo(7);
    assertThat(first.getIntLE(16)).isZero();
    assertThat(second.getIntLE(4)).isEqualTo(8);
    assertThat(unacknowledged).containsExactly(7);
    first.release();
    second.release();
  }

  @Test
  void framesTooShortToHoldTheFlagBitsArePassedOnUnchanged() {
    for (int length = 17; length < 20; length++) {
      final Set<Integer> unacknowledged = new HashSet<>();
      final EmbeddedChannel channel = new EmbeddedChannel(new MongoUnacknowledgedWrites.MoreToComeDecoder(unacknowledged));
      // a buffer of exactly the frame's size, so reading past it is out of bounds
      final ByteBuf frame = Unpooled.buffer(length, length);
      frame.writeIntLE(length);
      frame.writeIntLE(1);
      frame.writeIntLE(0);
      frame.writeIntLE(OP_MSG);
      frame.writeZero(length - 16);

      channel.writeInbound(frame);

      final ByteBuf passed = channel.readInbound();
      assertThat(passed.readableBytes()).isEqualTo(length);
      assertThat(unacknowledged).isEmpty();
      passed.release();
    }
  }

  @Test
  void theReplyToAnUnacknowledgedRequestIsDroppedAndOthersAreWritten() {
    final Set<Integer> unacknowledged = new HashSet<>(Set.of(42));
    final EmbeddedChannel channel = new EmbeddedChannel(new MongoUnacknowledgedWrites.ReplyFilter(unacknowledged));

    assertThat(channel.writeOutbound(new MongoMessage(channel, new MessageHeader(100, 42), new Document("ok", 1)))).isFalse();
    assertThat(unacknowledged).isEmpty();

    final MongoMessage other = new MongoMessage(channel, new MessageHeader(101, 43), new Document("ok", 1));
    assertThat(channel.writeOutbound(other)).isTrue();
    assertThat((Object) channel.readOutbound()).isSameAs(other);
  }

  @Test
  void installPutsTheHandlersBeforeTheLibraryDecoderOnce() {
    final EmbeddedChannel channel = new EmbeddedChannel(new MongoWireProtocolHandler());

    MongoUnacknowledgedWrites.install(channel);
    MongoUnacknowledgedWrites.install(channel);

    assertThat(channel.pipeline().names()).startsWith(MongoUnacknowledgedWrites.DECODER_NAME,
        MongoUnacknowledgedWrites.REPLY_FILTER_NAME);
    assertThat(channel.pipeline().names().stream().filter(MongoUnacknowledgedWrites.DECODER_NAME::equals)).hasSize(1);
  }

  /** A wire message of the header and one int of flags, padded with an empty BSON document. */
  private static ByteBuf message(final int requestId, final int opCode, final int flags) {
    final ByteBuf buf = Unpooled.buffer();
    buf.writeIntLE(0);
    buf.writeIntLE(requestId);
    buf.writeIntLE(0);
    buf.writeIntLE(opCode);
    buf.writeIntLE(flags);
    buf.writeByte(0);
    buf.writeIntLE(5);
    buf.writeByte(0);
    buf.setIntLE(0, buf.readableBytes());
    return buf;
  }
}
