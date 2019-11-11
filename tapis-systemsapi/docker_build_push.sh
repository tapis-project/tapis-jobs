#!/bin/sh
set -xv
export VER=`cat target/systems/WEB-INF/classes/tapis.version`
export GIT_COMMIT=`awk '{print $2}' target/systems/WEB-INF/classes/git.info`
export TAG="tapis/systems:${VER}"
export TAG2="tapis/systems:${VER}"
# Login to docker. Credentials set by Jenkins
docker login -u $USERNAME -p $PASSWD
# Build image from Dockerfile
docker build --build-arg VER=${VER} --build-arg GIT_COMMIT=${GIT_COMMIT} -t ${TAG} .
# Create tagged image for remote repo
docker tag $TAG $TAG2
# Push to remote repo
docker push $TAG2
