//
// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package e2e_go

import "testing"

// assertStillFails ports C#'s KnownGapAssertions.AssertStillFailsAsync: Go's
// testing package has no equivalent of pytest's xfail(strict=True). body
// asserts the Neo4j-correct behavior and returns a non-nil error while the
// known gap still reproduces (a driver error, or a value that does not match
// the correct expectation). If body returns nil the gap has been fixed - fail
// loudly so whoever fixed it converts this to a normal test and updates
// current_status in bolt/conformance/spec.yaml in the same PR.
func assertStillFails(t *testing.T, reason string, body func() error) {
	t.Helper()
	if err := body(); err != nil {
		t.Logf("known gap still reproduces (expected): %v", err)
		return
	}
	t.Fatalf("XPASS: known gap no longer reproduces - convert to a normal "+
		"test and update current_status in bolt/conformance/spec.yaml. Gap: %s", reason)
}
