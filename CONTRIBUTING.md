[![Java CI - deploy](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml/badge.svg)](https://github.com/ArcadeData/arcadedb/actions/workflows/mvn-deploy.yml)
[![License](https://img.shields.io/github/license/ArcadeData/arcadedb)](https://github.com/ArcadeData/arcadedb)

## How to contribute to ArcadeDB

#### **Did you find a bug?**

* **Do not open up a GitHub issue if the bug is a security vulnerability**, and instead write to support -at- arcadedb.com.

* **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/ArcadeData/arcadedb/issues).

* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/ArcadeData/arcadedb/issues/new)
  . Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **
  executable test case** demonstrating the expected behavior that is not occurring.

#### **Did you write a patch that fixes a bug?**

* Open a new GitHub pull request with the patch.

* Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

#### **Did you fix whitespace, format code, or make a purely cosmetic patch?**

Changes that are cosmetic in nature and do not add anything substantial to the stability, functionality, or testability will
generally not be accepted.

#### **Do you intend to add a new feature or change an existing one?**

* Suggest your change in the [ArcadeDB Community]() and start writing code.

* Do not open an issue on GitHub until you have collected positive feedback about the change. GitHub issues are primarily intended
  for bug reports and fixes.

### Prepare your environment

## Pre-commit

This project uses [pre-commit](https://pre-commit.com/). Every developer should install it locally, please
follow [installation instructions](https://pre-commit.com/#install) for your operative system.

### Developer guide

In order to use the latest distribution before the official release, you have to build it from the latest SNAPSHOT version in the `main` branch of GitHub. It's super easy, just follow the following steps:

(1) Download the sources

If you have git installed, execute the following command on a shell or command prompt:

```bash
$ git clone git@github.com:ArcadeData/arcadedb.git
```

With this command above you have just cloned the `main` branch continaing the latest updates. If you don't have git installed, you can always download the latest souces from [GitHub as a zip file](https://github.com/ArcadeData/arcadedb/archive/refs/heads/main.zip). After downloaded, unzip the file on a local folder.

(2) Now build the project using Maven.

Assure you have Apache Maven installed on your computer and execute the following commands:

```bash
$ cd arcadedb
$ mvn clean install -DskipTests
```

The build should take less than a minute.

(3) Use the fresh built distribution

You will find a fresh built distribution under the directory `package/target/arcadedb-<version>.dir/arcadedb-<version>/`.

For example, if the latest release is the `23.2.1-SNAPSHOT`, then the release will be available under the directory `package/target/arcadedb-23.2.1-SNAPSHOT.dir/arcadedb-23.2.1-SNAPSHOT/`

