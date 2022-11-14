#!/usr/bin/env bash
#
# Copyright (c) YugaByte, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

set -exo pipefail

. /etc/os-release

if [[ "${ID_LIKE:-}" == *rhel* ]]; then
  sudo yum -y -q install java-11-openjdk-devel
  sudo alternatives --set java java-11-openjdk.x86_64
else
  echo "OS not supported"
  exit 1
fi

# Set docker permissions
sudo chmod 666 /var/run/docker.sock