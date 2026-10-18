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

import com.arcadedb.server.ServerControlPlane;
import io.grpc.StatusException;

import java.util.ArrayList;
import java.util.List;

/**
 * Opens the package-private server half of the admin error contract to the client module's tests, so a test can take the
 * status the admin service builds and hand it to the client mapper that rebuilds an exception from it (issue #10143).
 * Lives in the server's package on the test classpath only; nothing in production reaches it.
 */
public final class AdminStatusTestBridge {
  private AdminStatusTestBridge() {
  }

  /** What {@code ArcadeDbGrpcAdminService} answers a handler failure with. */
  public static StatusException toStatus(final ArcadeDbGrpcAdminService service, final String operation, final Exception e) {
    return service.toStatus(operation, e);
  }

  /** What {@code ConnectCluster} answers a join that left a security document unseeded with. */
  public static StatusException seedFailureStatus(final ServerControlPlane.ConnectClusterResult result) {
    return ArcadeDbGrpcAdminService.seedFailureStatus(result);
  }

  /** The admin service's own authorization refusal, a package-private type. */
  public static Exception adminAuthorizationRefusal(final String message) {
    return new ArcadeDbGrpcAdminService.AdminAuthorizationException(message);
  }

  /** The type of every classified arm of {@code toStatus}, in table order. */
  public static List<Class<? extends Throwable>> refusalArmTypes() {
    final List<Class<? extends Throwable>> types = new ArrayList<>();
    for (final ArcadeDbGrpcAdminService.RefusalArm arm : ArcadeDbGrpcAdminService.REFUSAL_ARMS)
      types.add(arm.type());
    return types;
  }
}
