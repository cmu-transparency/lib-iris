#!/bin/bash

args="runMain edu.cmu.spf.iris.DataStats $*"
#args="java -jar ../target/scala-2.11/iris-assembly-0.2.jar $*"
projdir=`dirname $0`/..
(cd $projdir && exec sbt "$args")
#exec $args 
