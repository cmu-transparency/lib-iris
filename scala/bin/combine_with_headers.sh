#!/bin/bash

FILES=($*)
FIRST=${FILES[0]}

head -1 ${FIRST}
tail -n +2 -q $*
