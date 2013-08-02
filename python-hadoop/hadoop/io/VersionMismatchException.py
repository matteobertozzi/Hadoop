#!/usr/bin/env python
# ========================================================================
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
class VersionPrefixException(Exception):
    def __init__(self, expected, discovered):
        self.expected_prefix = expected
        self.discovered_prefix = discovered
    def __str__(self):
        return "Sequence file prefix found %r but expected %r" \
            % (self.discovered_prefix, self.expected_prefix)

class VersionMismatchException(Exception):
    def __init__(self, expected_version, founded_version):
        self.expected_version = expected_version
        self.founded_version = founded_version

    def toString(self):
        return "A record version mismatch occured. Expecting %r, found %r" \
            % (self.expected_version, self.founded_version)

    def __str__(self):
        self.toString()
