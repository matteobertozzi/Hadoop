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

from hadoop.io import *

def hadoopClassFromName(class_path):
    if class_path.startswith('org.apache.hadoop.'):
        class_path = class_path[11:]
    return classFromName(class_path)

def hadoopClassName(class_type):
    if hasattr(class_type, "hadoop_module_name") and \
       hasattr(class_type, "hadoop_class_name"):
        module_name = class_type.hadoop_module_name
        class_name = class_type.hadoop_class_name
    else:
        module_name = class_type.__module__
        class_name = class_type.__name__
    if module_name.startswith('hadoop.io.'):
        module_name, _, file_name = module_name.rpartition('.')
        return 'org.apache.%s.%s' % (module_name, class_name)
    return '%s.%s' % (module_name, class_name)

def classFromName(class_path):
    module_name, _, class_name = class_path.rpartition('.')
    if not module_name:
        raise ValueError('Class name must contain module part.')

    module = __import__(module_name, globals(), locals(), [str(class_name)], -1)
    return getattr(module, class_name)

