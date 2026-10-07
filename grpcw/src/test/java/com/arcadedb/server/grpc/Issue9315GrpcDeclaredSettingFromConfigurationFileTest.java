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

import com.arcadedb.ContextConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9315: {@code ContextConfiguration.fromJSON} stores a declared setting under its declared type, so an integer setting
 * written in {@code server-configuration.json} (as a number or as a string) is an {@link Integer} in the overlay, and the
 * plugin used to read it as text and take the whole server down with a {@link ClassCastException}. Every setting the plugin
 * reads now goes through the same typed resolution (issue #9316); the metadata cap is a declared integer read through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9315GrpcDeclaredSettingFromConfigurationFileTest {

  @Test
  void anIntegerWrittenAsANumberIsRead() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"grpc.maxMetadataSize\":8}}");

    assertThat(new GrpcServerPlugin().getMaxMetadataSizeBytes(configuration)).isEqualTo(8 * 1024);
  }

  @Test
  void anIntegerWrittenAsAStringIsRead() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"grpc.maxMetadataSize\":\"8\"}}");

    assertThat(new GrpcServerPlugin().getMaxMetadataSizeBytes(configuration)).isEqualTo(8 * 1024);
  }
}
