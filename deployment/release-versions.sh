#!/bin/bash
# Script to determine specific versions for a release.
# Determine versions for: service, tapis-bom, tapis-client-java, tapis-shared-java

SVC_NAME=jobs

PrgName=$(basename "$0")

# Determine absolute path to location from which we are running
#  and change to that directory.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

MVN_CACHE=~/.m2/repository/edu/utexas/tacc/tapis
VER_PREFIX="2.0."
RELEASE_PROP_FILE="release.properties"
BOM_NAME="tapis-bom"
CLIENT_NAME="tapis-client-java"
SHARED_NAME="tapis-shared-java"

BOM_DIR=${MVN_CACHE}/${BOM_NAME}
CLIENT_DIR=${MVN_CACHE}/${CLIENT_NAME}
SHARED_DIR=${MVN_CACHE}/${SHARED_NAME}

# Determine shared code versions
FILES=$(echo "${BOM_DIR}/${VER_PREFIX}*")
BOM_VER=$(ls -1 -d $FILES | tail -n 1 | xargs -n 1 basename)
FILES=$(echo "${CLIENT_DIR}/${VER_PREFIX}*")
CLIENT_VER=$(ls -1 -d $FILES | tail -n 1 | xargs -n 1 basename)
FILES=$(echo "${SHARED_DIR}/${VER_PREFIX}*")
SHARED_VER=$(ls -1 -d $FILES | tail -n 1 | xargs -n 1 basename)

# Determine service version
SVC_VER=$(cd ..;mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

# Update release.properties file
echo "${SVC_NAME}=${SVC_VER}" > ${RELEASE_PROP_FILE}
echo "${BOM_NAME}=${BOM_VER}" >> ${RELEASE_PROP_FILE}
echo "${CLIENT_NAME}=${CLIENT_VER}" >> ${RELEASE_PROP_FILE}
echo "${SHARED_NAME}=${SHARED_VER}" >> ${RELEASE_PROP_FILE}
