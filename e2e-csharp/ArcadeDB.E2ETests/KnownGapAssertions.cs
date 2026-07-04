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
 */

using Neo4j.Driver;
using Xunit.Sdk;

namespace ArcadeDB.E2ETests;

// xUnit has no built-in equivalent of pytest's xfail(strict=True). This
// helper reproduces that property: the action is the target-correct
// assertion; a failure today means the gap still reproduces (test passes),
// success means the gap was fixed and this now fails the build - forcing
// whoever fixed it to convert the test to a plain [Fact] and correct
// bolt/conformance/spec.yaml's current_status in the same PR.
public static class KnownGapAssertions
{
    public static async Task AssertStillFailsAsync(Func<Task> action, string reason)
    {
        try
        {
            await action();
        }
        catch (Exception ex) when (ex is XunitException or Neo4jException or InvalidCastException)
        {
            return;
        }

        throw new XunitException(
            $"Expected known gap to still reproduce, but the assertion passed - " +
            $"convert this to a normal [Fact] and update current_status in " +
            $"bolt/conformance/spec.yaml. Gap: {reason}");
    }
}
