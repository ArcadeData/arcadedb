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

import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unacknowledged ({@code w=0}) writes on the MongoDB wire (issue #11375). A driver sends them as an OP_MSG with the
 * {@code moreToCome} flag bit set and reads no reply. The bundled mongo-java-server (1.47.0) rejects every OP_MSG flag but
 * {@code checksumPresent} while it decodes the message, so the write never ran and the connection was dropped, and the client,
 * waiting for nothing, saw no error.
 * <p>
 * Two handlers go in front of the library's decoder on each connection. The inbound one frames the messages, clears the bit
 * on such an OP_MSG and remembers its request id; the library then runs it as an ordinary command. The outbound one drops
 * the reply to a remembered request: a reply the client does not read would be taken as the answer to its next request.
 * Remove this class once the library handles the flag itself (bwaldvogel/mongo-java-server#257).
 */
final class MongoUnacknowledgedWrites {
  static final String DECODER_NAME = "arcadedb-mongo-more-to-come";
  static final String REPLY_FILTER_NAME = "arcadedb-mongo-more-to-come-replies";

  // The wire message header: messageLength, requestID, responseTo, opCode, each a little-endian int32
  private static final int REQUEST_ID_OFFSET = 4;
  private static final int OP_CODE_OFFSET    = 12;
  private static final int FLAG_BITS_OFFSET  = 16;
  private static final int OP_MSG            = 2013;
  private static final int MORE_TO_COME      = 1 << 1;
  // the header and the OP_MSG flag bits, the shortest frame whose flags can be read
  private static final int MIN_OP_MSG_LENGTH = FLAG_BITS_OFFSET + Integer.BYTES;

  private MongoUnacknowledgedWrites() {
  }

  /**
   * Puts the two handlers on the connection, once. Called from the first request a connection makes, which is the driver's
   * handshake: drivers wait for its reply before they send anything else, so no later message is already buffered past the
   * new decoder. A channel without the library's decoder (a test's mock) is left as it is.
   */
  static void install(final Channel channel) {
    if (channel == null)
      return;
    final ChannelPipeline pipeline = channel.pipeline();
    if (pipeline == null || pipeline.get(DECODER_NAME) != null)
      return;
    final ChannelHandlerContext libraryDecoder = pipeline.context(MongoWireProtocolHandler.class);
    if (libraryDecoder == null)
      return;

    // an id leaves the set when its reply is written; one the library never replies to stays until the connection closes
    final Set<Integer> unacknowledged = ConcurrentHashMap.newKeySet();
    pipeline.addBefore(libraryDecoder.name(), DECODER_NAME, new MoreToComeDecoder(unacknowledged));
    pipeline.addBefore(libraryDecoder.name(), REPLY_FILTER_NAME, new ReplyFilter(unacknowledged));
  }

  /**
   * Passes each complete wire message on as its own buffer, with the {@code moreToCome} bit of an OP_MSG cleared. A length
   * that is no message is passed on as it is, for the library to refuse as before.
   */
  static final class MoreToComeDecoder extends ByteToMessageDecoder {
    private final Set<Integer> unacknowledged;

    MoreToComeDecoder(final Set<Integer> unacknowledged) {
      this.unacknowledged = unacknowledged;
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {
      if (in.readableBytes() < Integer.BYTES)
        return;
      final int length = in.getIntLE(in.readerIndex());
      if (length < MIN_OP_MSG_LENGTH || length > MongoWireProtocolHandler.MAX_MESSAGE_SIZE_BYTES) {
        out.add(in.readRetainedSlice(in.readableBytes()));
        return;
      }
      if (in.readableBytes() < length)
        return;

      final int start = in.readerIndex();
      final int flagBits = in.getIntLE(start + FLAG_BITS_OFFSET);
      if (in.getIntLE(start + OP_CODE_OFFSET) != OP_MSG || (flagBits & MORE_TO_COME) == 0) {
        out.add(in.readRetainedSlice(length));
        return;
      }

      // a copy, so the cumulation buffer the slice would share is not written to
      final ByteBuf message = in.readBytes(length);
      message.setIntLE(FLAG_BITS_OFFSET, flagBits & ~MORE_TO_COME);
      unacknowledged.add(message.getIntLE(REQUEST_ID_OFFSET));
      out.add(message);
    }
  }

  /** Drops the reply to a request the client sent with {@code moreToCome}, and lets every other write through. */
  static final class ReplyFilter extends ChannelOutboundHandlerAdapter {
    private final Set<Integer> unacknowledged;

    ReplyFilter(final Set<Integer> unacknowledged) {
      this.unacknowledged = unacknowledged;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
      if (msg instanceof MongoMessage reply && unacknowledged.remove(reply.getHeader().getResponseTo())) {
        promise.setSuccess();
        return;
      }
      super.write(ctx, msg, promise);
    }
  }
}
