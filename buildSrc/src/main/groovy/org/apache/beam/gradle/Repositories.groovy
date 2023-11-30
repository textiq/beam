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

package org.apache.beam.gradle

import org.gradle.api.Project

class Repositories {

  private static def loadArtifactoryConfig() {
    def config = [:];

    def userHome = System.getProperty("user.home");
    def configFile = java.nio.file.Paths.get(userHome, ".gradle", "artifactory.groovy").toFile();
    if (configFile.exists()) {
      config = new ConfigSlurper().parse(configFile.toURL());
    }

    for (key in ['url', 'username', 'password']) {
      def fromEnv = System.env["ARTIFACTORY_${key.toUpperCase()}"];
      if (fromEnv != null) {
        config[key] = fromEnv;
      }

      if (config[key] == null || config[key] == '' || config[key] == [:]) {
        throw new Exception("No artifactory ${key} configured :(");
      }
    }

    return config;
  }
  static def artifactoryConfig = loadArtifactoryConfig();


  static void register(Project project) {
    project.repositories {
      maven {
        url = "https://${artifactoryConfig.url}/maven-anthology"
        credentials {
          username = artifactoryConfig.username
          password = artifactoryConfig.password
        }
        allowInsecureProtocol = false
      }

    }
  }

}
