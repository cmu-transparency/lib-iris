#!/bin/bash

cd ..
sbt "runMain edu.cmu.spf.iris.DataStats $@"
