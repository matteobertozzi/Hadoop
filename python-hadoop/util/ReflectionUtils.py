#!/usr/bin/env python

def hadoopClassFromName(class_path):
    if class_path.startswith('org.apache.hadoop.'):
        class_path = class_path[18:]
    return classFromName(class_path)

def classFromName(class_path):
    module_name, _, class_name = class_path.rpartition('.')
    if not module_name:
        raise ValueError('Class name must contain module part.')

    module = __import__(module_name, globals(), locals(), [class_name], -1)
    return getattr(module, class_name)

if __name__ == '__main__':
    print classFromName('io.SequenceFile')
