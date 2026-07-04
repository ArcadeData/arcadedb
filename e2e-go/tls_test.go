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

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var (
	tlsImageOnce sync.Once
	tlsImageTag  string
	tlsImageErr  error

	tlsReqOnce sync.Once
	tlsReq     *arcadeContainer
	tlsReqErr  error

	tlsOptOnce sync.Once
	tlsOpt     *arcadeContainer
	tlsOptErr  error
)

// buildTLSImage generates a throwaway self-signed keystore/truststore with the
// JDK keytool and bakes them into a derived image via `docker build` COPY. This
// matches the e2e-python/e2e-csharp approach: bind mounts are unreliable on CI
// runners (the mounted dir is sometimes empty), while a build-context COPY
// always resolves correctly. Returns the derived image tag.
func buildTLSImage() (string, error) {
	tlsImageOnce.Do(func() {
		keytool, err := exec.LookPath("keytool")
		if err != nil {
			tlsImageErr = fmt.Errorf("keytool not found on PATH - a JDK is required for the TLS scenarios (CONN-002/CONN-005): %w", err)
			return
		}
		dir, err := os.MkdirTemp("", "bolt-tls-certs")
		if err != nil {
			tlsImageErr = err
			return
		}
		// Remove the temp cert dir if we bail out before the package-level
		// cleanup (which also removes it) is registered on the success path.
		cleanupDir := true
		defer func() {
			if cleanupDir {
				_ = os.RemoveAll(dir)
			}
		}()
		ks := filepath.Join(dir, "keystore.p12")
		ts := filepath.Join(dir, "truststore.jks")
		cer := filepath.Join(dir, "bolt.cer")

		runKeytool := func(args ...string) error {
			out, err := exec.Command(keytool, args...).CombinedOutput()
			if err != nil {
				return fmt.Errorf("keytool %v: %v\n%s", args, err, out)
			}
			return nil
		}
		if tlsImageErr = runKeytool("-genkeypair", "-alias", "bolt", "-keyalg", "RSA",
			"-keysize", "2048", "-validity", "3650", "-keystore", ks, "-storetype", "PKCS12",
			"-storepass", tlsStorePass, "-keypass", tlsStorePass,
			"-dname", "CN=localhost, OU=ArcadeDB, O=ArcadeDB, L=Test, ST=Test, C=US"); tlsImageErr != nil {
			return
		}
		if tlsImageErr = runKeytool("-exportcert", "-alias", "bolt", "-keystore", ks,
			"-storetype", "PKCS12", "-storepass", tlsStorePass, "-file", cer); tlsImageErr != nil {
			return
		}
		if tlsImageErr = runKeytool("-importcert", "-alias", "bolt", "-keystore", ts,
			"-storetype", "JKS", "-storepass", tlsStorePass, "-file", cer, "-noprompt"); tlsImageErr != nil {
			return
		}

		dockerfile := fmt.Sprintf("FROM %s\nCOPY --chown=arcadedb:arcadedb keystore.p12 truststore.jks /home/arcadedb/tls_certs/\n", imageName())
		if tlsImageErr = os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o644); tlsImageErr != nil {
			return
		}

		tag := "arcadedb-bolt-tls-go:latest"
		if out, err := exec.Command("docker", "build", "-t", tag, dir).CombinedOutput(); err != nil {
			tlsImageErr = fmt.Errorf("docker build: %v\n%s", err, out)
			return
		}
		tlsImageTag = tag
		cleanupDir = false // ownership of dir passes to the package-level cleanup below
		addTLSCleanup(func() {
			_ = exec.Command("docker", "image", "rm", "-f", tag).Run()
			_ = os.RemoveAll(dir)
		})
	})
	return tlsImageTag, tlsImageErr
}

// startTLSContainer boots a container from the derived TLS image with the given
// arcadedb.bolt.ssl mode and the keystore/truststore sysprops.
func startTLSContainer(sslMode string) (*arcadeContainer, error) {
	image, err := buildTLSImage()
	if err != nil {
		return nil, err
	}
	opts := baseJavaOpts + " " +
		"-Darcadedb.bolt.ssl=" + sslMode + " " +
		"-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/keystore.p12 " +
		"-Darcadedb.ssl.keyStorePassword=" + tlsStorePass + " " +
		"-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/truststore.jks " +
		"-Darcadedb.ssl.trustStorePassword=" + tlsStorePass
	// TLS keystore/truststore loading adds startup latency beyond the plain
	// container's margin, especially on shared CI runners - give it more room.
	c, err := startArcade(opts, image, 150*time.Second)
	if err != nil {
		return nil, err
	}
	addTLSCleanup(func() { _ = c.container.Terminate(ctx) })
	return c, nil
}

// tlsRequiredContainer lazily starts (once) the TLS-REQUIRED container, skipping
// the calling test if keytool/docker are unavailable locally.
func tlsRequiredContainer(t *testing.T) *arcadeContainer {
	t.Helper()
	tlsReqOnce.Do(func() { tlsReq, tlsReqErr = startTLSContainer("REQUIRED") })
	if tlsReqErr != nil {
		t.Skipf("TLS-required container unavailable: %v", tlsReqErr)
	}
	return tlsReq
}

// tlsOptionalContainer lazily starts (once) the TLS-OPTIONAL container.
func tlsOptionalContainer(t *testing.T) *arcadeContainer {
	t.Helper()
	tlsOptOnce.Do(func() { tlsOpt, tlsOptErr = startTLSContainer("OPTIONAL") })
	if tlsOptErr != nil {
		t.Skipf("TLS-optional container unavailable: %v", tlsOptErr)
	}
	return tlsOpt
}
