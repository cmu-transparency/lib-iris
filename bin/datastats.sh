#!/bin/bash

args="runMain edu.cmu.spf.iris.DataStats $*"
projdir=`dirname $0`/..
(cd $projdir && exec sbt "$args")
