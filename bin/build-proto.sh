#!/bin/bash

BASEDIR=$(dirname "$0")/../

git submodule update --init --recursive

cd ${BASEDIR}/tipb
mvn clean install

cd ${BASEDIR}/kvproto
mvn clean install