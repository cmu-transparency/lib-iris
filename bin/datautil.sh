#!/bin/bash

cd `dirname "$0"`/..
sbt "runMain edu.cmu.spf.iris.DataUtil $*"
