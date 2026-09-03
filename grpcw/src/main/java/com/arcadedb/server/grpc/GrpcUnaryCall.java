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

import com.arcadedb.log.LogManager;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import java.util.function.Function;
import java.util.logging.Level;

/**
 * Terminates a unary gRPC call exactly once. The handler body computes the response; this helper hands it to the
 * observer and closes the call, and it maps a failure to {@code onError} only while the call has not been answered
 * yet.
 * <p>
 * The guard is the point (issues #6192 and #6756): a client cancel landing between {@code onNext} and
 * {@code onCompleted} makes {@code onCompleted()} throw, and a handler that catches that and calls {@code onError}
 * on the already-closed call lets an {@code IllegalStateException} ("call already closed") escape instead of the
 * cancel being absorbed. The flag is set before {@code onCompleted()}, not after: once {@code onNext} has delivered
 * the response the call must never fall through to {@code onError}, whatever {@code onCompleted()} itself does.
 * <p>
 * {@link ArcadeDbGrpcService} carries the same guard inline in every handler, because its error mapping is
 * per-handler and threaded through the transaction registry; this helper is for the handlers whose shape is
 * "authenticate, compute, answer", so the next one added cannot be added without the guard.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GrpcUnaryCall {
  /** The body of a unary handler: computes the response, or throws. */
  interface Body<T> {
    T call() throws Exception;
  }

  private GrpcUnaryCall() {
  }

  /**
   * Runs {@code body} and delivers its result to {@code resp}. A failure raised by the body is mapped through
   * {@code errorMapper} and sent with {@code onError}; a failure raised by the observer once the response has been
   * delivered is logged and swallowed, since the call is already terminated from the caller's point of view.
   *
   * @param resp        the observer of the unary call
   * @param body        the handler body
   * @param errorMapper maps a failure of the body to the status the client receives
   */
  static <T> void respond(final StreamObserver<T> resp, final Body<T> body,
      final Function<Exception, StatusException> errorMapper) {
    boolean responded = false;
    try {
      final T out = body.call();
      resp.onNext(out);
      responded = true;
      resp.onCompleted();
    } catch (final Exception e) {
      if (responded)
        LogManager.instance().log(GrpcUnaryCall.class, Level.FINE,
            "Unary call already answered when its completion failed (client cancelled?): %s", e.getMessage());
      else
        resp.onError(errorMapper.apply(e));
    }
  }
}
