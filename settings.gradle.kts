/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.gradle.enterprise.gradleplugin.internal.extension.BuildScanExtensionWithHiddenFeatures

pluginManagement {
  plugins {
     id("org.javacc.javacc") version "3.0.0" // enable the JavaCC parser generator
  }
}

plugins {
  id("com.gradle.enterprise") version "3.15.1"
  id("com.gradle.common-custom-user-data-gradle-plugin") version "1.11.3"
}


// JENKINS_HOME and BUILD_ID set automatically during Jenkins execution
val isJenkinsBuild = arrayOf("JENKINS_HOME", "BUILD_ID").all { System.getenv(it) != null }
// GITHUB_REPOSITORY and GITHUB_RUN_ID set automatically during Github Actions run
val isGithubActionsBuild = arrayOf("GITHUB_REPOSITORY", "GITHUB_RUN_ID").all { System.getenv(it) != null }
val isCi = isJenkinsBuild || isGithubActionsBuild

gradleEnterprise {
  server = "https://ge.apache.org"
  allowUntrustedServer = false

  buildScan {
    capture { isTaskInputFiles = true }
    isUploadInBackground = !isCi
    publishAlways()
    this as BuildScanExtensionWithHiddenFeatures
    publishIfAuthenticated()
    obfuscation {
      ipAddresses { addresses -> addresses.map { "0.0.0.0" } }
    }
  }
}

buildCache {
  local {
    isEnabled = true
  }
  remote<HttpBuildCache> {
    url = uri("https://beam-cache.apache.org/cache/")
    isAllowUntrustedServer = false
    credentials {
      username = System.getenv("GRADLE_ENTERPRISE_CACHE_USERNAME")
      password = System.getenv("GRADLE_ENTERPRISE_CACHE_PASSWORD")
    }
    isEnabled = true
    isPush = isCi
  }
}

rootProject.name = "beam"

include(":examples:java")
include(":model:fn-execution")
include(":model:job-management")
include(":model:pipeline")
include(":release")
include(":release:go-licenses:java")
include(":runners:core-construction-java")
include(":runners:core-java")
include(":runners:direct-java")
include(":runners:flink:1.16")
include(":runners:google-cloud-dataflow-java")
include(":runners:google-cloud-dataflow-java:worker")
include(":runners:google-cloud-dataflow-java:worker:windmill")
include(":runners:java-fn-execution")
include(":runners:java-job-service")
include(":runners:local-java")
include(":runners:spark:3")
include(":sdks:go:test")
include(":sdks:java:build-tools")
include(":sdks:java:container:agent")
include(":sdks:java:container:java11")
include(":sdks:java:container:java8")
include(":sdks:java:core")
include(":sdks:java:expansion-service")
include(":sdks:java:extensions:arrow")
include(":sdks:java:extensions:avro")
include(":sdks:java:extensions:google-cloud-platform-core")
include(":sdks:java:extensions:ml")
include(":sdks:java:extensions:protobuf")
include(":sdks:java:extensions:python")
include(":sdks:java:fn-execution")
include(":sdks:java:harness")
include(":sdks:java:io:common")
include(":sdks:java:io:google-cloud-platform")
include(":sdks:java:io:hadoop-common")
include(":sdks:java:io:hadoop-file-system")
include(":sdks:java:io:hadoop-format")
include(":sdks:java:io:jdbc")
include(":sdks:java:io:kafka")
include(":sdks:java:io:mongodb")
include(":sdks:java:io:parquet")
include(":sdks:java:io:synthetic")
include(":sdks:java:testing:expansion-service")
include(":sdks:java:testing:test-utils")
include(":sdks:java:transform-service:launcher")
include(":sdks:python")
