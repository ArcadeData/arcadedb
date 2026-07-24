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
package org.wildfly.common;

/**
 * Build-time shadow of {@code org.wildfly.common.Substitutions} shipped in wildfly-common 1.5.4.Final
 * (pulled transitively by Undertow/XNIO). That class carries GraalVM native-image {@code @TargetClass}
 * substitutions ({@code Target_GraalDirectives}, {@code Target_Branch}) that inject Graal branch-probability
 * intrinsics. The substitution target {@code org.graalvm.compiler.api.directives.GraalDirectives} was renamed
 * to the {@code jdk.graal.compiler.*} namespace in the GraalVM shipped for JDK 21+, so on GraalVM CE 25 the
 * native-image builder aborts with "Substitution target ... is not loaded".
 *
 * <p>The arcadedb-native jar precedes wildfly-common on the image classpath, so these stripped, un-annotated
 * classes shadow the broken originals and remove the substitutions entirely. They are pure optimization hints
 * (branch-probability predictions); dropping them has zero behavioral effect - the substituted methods simply
 * run their normal Java bodies. The unrelated wildfly substitutions (net/os/lock, including the run-time
 * HostName handling) live in different classes and are untouched.
 */
final class Substitutions {

  static final class Target_GraalDirectives {
  }

  static final class Target_Branch {
  }

  private Substitutions() {
  }
}
