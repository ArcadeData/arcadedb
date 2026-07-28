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
package com.arcadedb.query.polyglot;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * GHSA-wx28-2265-f788, end-to-end on a real polyglot context: an embedder that configures a deliberately broad
 * allow-list must still be unable to resolve a process, a filesystem, a socket, a reflection or a class-loader class
 * through {@code Java.type(...)}, because the deny-list built into {@link HostClassLookupFilter} is enforced on top of
 * whatever the allow-list says.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraalPolyglotEngineHostClassLookupTest {

  @Test
  void aWideOpenAllowListStillCannotReachTheDangerousClasses() throws IOException {
    try (final GraalPolyglotEngine engine = GraalPolyglotEngine.newBuilder(null,
        PolyglotEngineManager.getInstance().getSharedEngine())//
        .setLanguage("js")//
        .setAllowedPackages(List.of("java.**", "javax.**", "jdk.**", "sun.**", "com.sun.**"))//
        .build()) {

      // VALUE TYPES REMAIN USABLE
      assertThat(engine.eval("var l = new (Java.type('java.util.ArrayList'))(); l.add('x'); l.size();").asInt()).isEqualTo(1);

      for (final String className : new String[] {                //
          "java.lang.Runtime",                                    //
          "java.lang.ProcessBuilder",                             //
          "java.lang.System",                                     //
          "java.lang.Thread",                                     //
          "java.lang.ClassLoader",                                //
          "java.lang.reflect.Method",                             //
          "java.lang.invoke.MethodHandles",                       //
          "java.io.File",                                         //
          "java.io.FileInputStream",                              //
          "java.nio.file.Files",                                  //
          "java.net.Socket",                                      //
          "java.net.URL",                                         //
          "java.util.zip.ZipFile",                                //
          "java.util.jar.JarFile",                                //
          "java.util.logging.FileHandler",                        //
          "java.util.concurrent.ThreadPoolExecutor",              //
          "java.util.Formatter",                                  //
          "java.util.Scanner",                                    //
          "java.util.Timer",                                      //
          "java.util.ServiceLoader",                              //
          "java.security.AccessController",                       //
          "java.sql.DriverManager",                               //
          "javax.script.ScriptEngineManager",                     //
          "javax.tools.ToolProvider",                             //
          "javax.naming.InitialContext" }) {

        assertThatThrownBy(() -> engine.eval("Java.type('" + className + "');"))
            .as("host-class lookup of %s must be denied", className)
            .hasMessageContaining(className);
      }
    }
  }

  @Test
  void anEntryWithoutWildcardIsAnExactClassAndDoesNotOpenItsPackage() throws IOException {
    try (final GraalPolyglotEngine engine = GraalPolyglotEngine.newBuilder(null,
        PolyglotEngineManager.getInstance().getSharedEngine())//
        .setLanguage("js")//
        .setAllowedPackages(List.of("java.math.BigDecimal"))//
        .build()) {

      assertThat(engine.eval("new (Java.type('java.math.BigDecimal'))('2').multiply(new (Java.type('java.math.BigDecimal'))('3')).toString();")
          .asString()).isEqualTo("6");

      assertThatThrownBy(() -> engine.eval("Java.type('java.math.BigInteger');")).hasMessageContaining("java.math.BigInteger");
    }
  }

  @Test
  void callerSuppliedRestrictedPackagesAreEnforced() throws IOException {
    try (final GraalPolyglotEngine engine = GraalPolyglotEngine.newBuilder(null,
        PolyglotEngineManager.getInstance().getSharedEngine())//
        .setLanguage("js")//
        .setAllowedPackages(List.of("java.util.*"))//
        .setRestrictedPackages(List.of("java.util.Random"))//
        .build()) {

      assertThat(engine.eval("new (Java.type('java.util.ArrayList'))().size();").asInt()).isZero();
      assertThatThrownBy(() -> engine.eval("Java.type('java.util.Random');")).hasMessageContaining("java.util.Random");
    }
  }
}
