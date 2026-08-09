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
package com.arcadedb.query.opencypher.tck;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;
import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

/**
 * Runs the openCypher TCK feature files against the engine's Cypher implementation.
 * <p>
 * This suite runs in a CI lane of its own (the {@code opencypher-tck-tests} job in {@code .github/workflows/mvn-test.yml}), not in the general
 * unit-test lane. It is one Surefire "class" holding ~3900 scenarios, and conformance is a number worth reading on its own rather than averaged into a
 * 15,000-test pass count.
 * <p>
 * <b>The lane is selected by file pattern, not by a JUnit tag, and that is not a style preference.</b> Surefire's {@code groups} /
 * {@code excludedGroups} become JUnit tag filters that the suite engine propagates <i>into</i> the nested Cucumber discovery, where they are matched
 * against the scenarios' own feature-file tags rather than against this class. Measured against the real suite:
 * <ul>
 *   <li>with {@code @Tag("opencypher")} here and {@code -Dgroups=opencypher}, no scenario carries that tag, so discovery yields
 *       {@code NoTestsDiscovered} and the build fails;</li>
 *   <li>with the same tag and {@code -DexcludedGroups=opencypher}, the filter again matches no scenario, so all 3897 run anyway.</li>
 * </ul>
 * A tag on a Cucumber {@code @Suite} is therefore inert in one direction and actively fatal in the other. The jobs select by
 * {@code -Dsurefire.includes}: the TCK lane asks for {@code **}{@code /*Suite.java}, and the unit-test lane asks only for {@code **}{@code /*Test.java},
 * which no longer matches this file. Adding a second {@code *Suite.java} under {@code engine} puts it in the TCK lane too - move it, or narrow that
 * job's include, rather than reaching for a tag.
 */
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("opencypher/tck/features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.arcadedb.query.opencypher.tck")
@ConfigurationParameter(key = PLUGIN_PROPERTY_NAME, value = "pretty, html:target/tck-report.html, com.arcadedb.query.opencypher.tck.TCKReportPlugin")
public class OpenCypherTCKSuite {
}
