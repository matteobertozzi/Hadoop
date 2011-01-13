#!/usr/bin/env python

class VersionMismatchException(Exception):
    def __init__(self, expected_version, founded_version):
        self.expected_version = expected_version
        self.founded_version = founded_version

    def toString(self):
        return "A record version mismatch occured. Expecting %r, found %r" % (self.expected_version, self._faounded_version)

