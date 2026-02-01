class Arcadedb < Formula
  desc "Multi-Model DBMS: Graph, Document, Key/Value, Search, Time Series, Vector"
  homepage "https://arcadedb.com"
  url "https://github.com/ArcadeData/arcadedb/releases/download/26.1.1/arcadedb-26.1.1.tar.gz"
  sha256 "975dad6ae63b82faf86fa0bf03bff0568c60d35ece5f9849beb3bf5dba759b7e"
  license "Apache-2.0"

  livecheck do
    url :stable
    strategy :github_latest
  end

  depends_on "openjdk@21"

  def install
    # Remove Windows batch files
    rm Dir["bin/*.bat"]

    # Install everything to libexec
    libexec.install Dir["*"]

    # Set up environment for wrapper scripts
    env = {
      JAVA_HOME:     Formula["openjdk@21"].opt_prefix,
      ARCADEDB_HOME: libexec,
    }

    # Create bin wrappers
    (bin/"arcadedb-server").write_env_script libexec/"bin/server.sh", env
    (bin/"arcadedb-console").write_env_script libexec/"bin/console.sh", env
  end

  def post_install
    # Create runtime directories
    (var/"arcadedb/databases").mkpath
    (var/"arcadedb/backups").mkpath
    (var/"arcadedb/config").mkpath
    (var/"log/arcadedb").mkpath

    # Copy default config files if not present
    %w[arcadedb-log.properties server-groups.json gremlin-server.yaml gremlin-server.groovy].each do |f|
      target = var/"arcadedb/config"/f
      cp libexec/"config"/f, target unless target.exist?
    end
  end

  service do
    run [opt_bin/"arcadedb-server",
         "-Darcadedb.server.rootPath=#{HOMEBREW_PREFIX}/var/arcadedb"]
    working_dir var/"arcadedb"
    log_path var/"log/arcadedb/server.log"
    error_log_path var/"log/arcadedb/server-error.log"
    keep_alive true
  end

  def caveats
    <<~EOS
      Data:    #{var}/arcadedb/databases
      Logs:    #{var}/log/arcadedb
      Config:  #{var}/arcadedb/config

      First-time setup (required before starting the service):
        Set the root password via JVM property:
          arcadedb-server -Darcadedb.server.rootPath=#{var}/arcadedb \\
            -Darcadedb.server.rootPassword=yourpassword

        Then stop the server (Ctrl+C) and start the service:
          brew services start arcadedb

      Or run manually (without service):
        arcadedb-server

      Web Studio available at: http://localhost:2480
    EOS
  end

  test do
    port = free_port
    pid = fork do
      # Disable JMX to avoid port conflicts in test environment
      ENV["ARCADEDB_JMX"] = " "
      exec bin/"arcadedb-server",
           "-Darcadedb.server.httpIncomingHost=127.0.0.1",
           "-Darcadedb.server.httpIncomingPort=#{port}",
           "-Darcadedb.server.databaseDirectory=#{testpath}/databases",
           "-Darcadedb.server.rootPassword=playwithdata"
    end
    sleep 15
    begin
      # /api/v1/ready returns 204 No Content on success
      system "curl", "-sf", "http://127.0.0.1:#{port}/api/v1/ready"
    ensure
      Process.kill("TERM", pid)
      Process.wait(pid)
    end
  end
end
