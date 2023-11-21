# Overview

> _Describe your changes.  This can also be the commit message (if it was a [good one](https://cbea.ms/git-commit/))._

## Related Issues

> _Reference any issue(s) this PR should either close or is related to in some way._

* Closes raft-tech/data-fabric#0000
* Part of raft-tech/data-fabric#0000
* Relates to raft-tech/data-fabric#0000

# Test Procedure

## Setup

From a new shell session in any clean/empty directory, perform the following 👇.

### Environment

1. Export your GitHub credentials to pull from the GitHub container registry.
    ```shell
    export GHP_USERNAME=your_github_username_goes_here
    export GHP_SECRET=your_github_pat_goes_here
    ```

1. Create a subdirectory for this PR test.
    ```shell
    export PR_HOME=$(pwd)/pr-test
    mkdir -pv $PR_HOME
    ```

### Clone Repositories

1. Clone the main `data-fabric` repo, checking out the branch we'll use to test this PR.
    ```shell
    export DF_HOME=$PR_HOME/data-fabric
    git clone --branch dev https://github.com/raft-tech/data-fabric.git $DF_HOME
    ```

1. Clone the `df-arcadedb`, checking out the branch for this PR.
    ```shell
    export DF_ARCADEDB_HOME=$PR_HOME/df-arcadedb    
    git clone --branch YOUR_PR_BRANCH https://github.com/raft-tech/df-arcadedb.git $DF_ARCADEDB_HOME
    ```

1. Clone `dfdev` to help with building and running things. \
   ℹ️ _If you already have [`dfdev`](https://github.com/raft-tech/dfdev) you can skip this step._
    ```shell
    git clone https://github.com/raft-tech/dfdev.git $PR_HOME/dfdev
    PATH=$PATH:$PR_HOME/dfdev/dfdev
    ```
   
### Deploy

1. (Re)Create a new local `kind` cluster and deploy Data Fabric to it.
    ```shell
    dfdev cluster recreate
    ```

1. Re-build and re-deploy `df-arcadedb`

    ```shell
    dfdev redeploy df-arcadedb
    ``` 

## Verification Steps

1. Do this
    ```shell
    hack hack hack
    ```
1. ...then do this...
1. ...and finally