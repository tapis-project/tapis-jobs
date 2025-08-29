#!/bin/sh
# Special build and optionally push docker image for Jobs service for specific branch
# This is the script run in Jenkins as part of job TapisJava->3_ManualBuildDeploy->jobs-branch
# Branch name to use for tag must be passed in as first argument
# Existing docker login is used for push
# No unique docker image tag is created
#

PrgName=$(basename "$0")

USAGE="Usage: $PrgName <branch_name> [ -push ]"

# Determine absolute path to location from which we are running
#  and change to that directory.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

SVC_NAME="jobs"
SVC1="jobsapi"
SVC2="jobsmigrate"
SVC3="jobslib"
SVC3TAG="jobsworker"
REPO="tapis"
BRANCH_NAME="$1"

export SVC1_WAR=v3
export SVC2_JAR=${SVC2}.jar
export SVC3_JAR=shaded-${SVC3}.jar

# Directories for docker build context. Dockerfiles are located here in repo
BUILD_DIR1=$PRG_PATH/tapis-${SVC1}
BUILD_DIR2=$PRG_PATH/tapis-${SVC2}
BUILD_DIR3=$PRG_PATH/tapis-${SVC3TAG}
# Target build directories containing compiled code and jar/war files
BUILD_DIRT1=$PRG_PATH/../tapis-${SVC1}/target
BUILD_DIRT2=$PRG_PATH/../tapis-${SVC2}/target
BUILD_DIRT3=$PRG_PATH/../tapis-${SVC3}/target
ENV=$1

# Check number of arguments
if [ $# -lt 1 -o $# -gt 2 ]; then
  echo "$USAGE"
  exit 1
fi

# Check second arg
if [ $# -eq 2 -a "x$2" != "x-push" ]; then
  echo "$USAGE"
  exit 1
fi

# Make sure service has been built
if [ ! -d "$BUILD_DIRT1" ]; then
  echo "Build directory missing. Please build. Directory: $BUILD_DIRT1"
  exit 1
fi
if [ ! -d "$BUILD_DIRT2" ]; then
  echo "Build directory missing. Please build. Directory: $BUILD_DIRT2"
  exit 1
fi
if [ ! -d "$BUILD_DIRT3" ]; then
  echo "Build directory missing. Please build. Directory: $BUILD_DIRT3"
  exit 1
fi

# Set variables used for builds
VER=$(cat $BUILD_DIRT1/classes/tapis.version)
GIT_BRANCH_LBL=$(awk '{print $1}' $BUILD_DIRT1/classes/git.info)
GIT_COMMIT_LBL=$(awk '{print $2}' $BUILD_DIRT1/classes/git.info)

TAG_BRANCH1="${REPO}/${SVC1}:${BRANCH_NAME}"
TAG_BRANCH2="${REPO}/${SVC2}:${BRANCH_NAME}"
TAG_BRANCH3="${REPO}/${SVC3TAG}:${BRANCH_NAME}"

export BUILD_TIME1="$(awk '{print $1}' ${BUILD_DIRT1}/classes/build.time)"
export BUILD_TIME2="$(awk '{print $1}' ${BUILD_DIRT2}/classes/build.time)"
export BUILD_TIME3="$(awk '{print $1}' ${BUILD_DIRT3}/classes/build.time)"

# If branch name is UNKNOWN or empty as might be the case in a jenkins job then
#   set it to GIT_BRANCH. Jenkins jobs should have this set in the env.
if [ -z "$GIT_BRANCH_LBL" -o "x$GIT_BRANCH_LBL" = "xUNKNOWN" ]; then
  GIT_BRANCH_LBL=$(echo "$GIT_BRANCH" | awk -F"/" '{print $2}')
fi

echo "-----------------------------------------------------------------"
echo "-------                   Environment                      ------"
echo "-----------------------------------------------------------------"
echo "  ENV=        ${ENV}"
echo "  VER=        ${VER}"
echo "  GIT_BRANCH_LBL= ${GIT_BRANCH_LBL}"
echo "  GIT_COMMIT_LBL= ${GIT_COMMIT_LBL}"

# Build image for jobsapi
echo "======================================================================"
echo "Building local image for service ${SVC1} using tag: $TAG_BRANCH1"
echo "======================================================================"
# Make sure we do not accidentally run rm -fr /
if [ -z "$BUILD_DIR1" ] ||  [ -z "$SVC1_WAR" ]; then
  echo "ERROR Missing env variable BUILD_DIR1=$BUILD_DIR1, SVC1_WAR=$SVC1_WAR"
  exit 1
fi
# Move to the build directory
cd $BUILD_DIR1 || exit
echo "**** Removing any old service war files from Docker build context."
rm -f $SVC1_WAR.war
echo "**** Removing war directory from Docker build context"
rm -fr ${BUILD_DIR1:=/tmp}/${SVC1_WAR:=jobsapi}
echo "**** Unzipping $SVC_NAME.war to ${BUILD_DIR1}/${SVC_NAME} "
unzip $BUILD_DIRT1/$SVC1_WAR.war -d ${BUILD_DIR1}/${SVC1_WAR}

echo "**** Building docker image: $TAG_BRANCH1"
docker build -f Dockerfile \
   --build-arg SRVC_ROOT=${SVC1_WAR} --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT \
   --build-arg BUILD_TIME=$BUILD_TIME -t "${TAG_BRANCH1}" .


# Build image for jobsmigrate
echo "======================================================================"
echo "Building local image for service ${SVC2} using tag: $TAG_BRANCH2"
echo "======================================================================"
# Move to the build directory
cd $BUILD_DIR2 || exit
echo "**** Removing any old service jar files from Docker build context"
rm -f $SVC2_JAR
echo "**** Copying $SVC2_JAR to Dockerfile context"
cp $BUILD_DIRT2/${SVC2_JAR} $BUILD_DIR2

echo "**** Building docker image: $TAG_BRANCH2"
docker build -f Dockerfile \
   --build-arg SRVC_JAR=${SVC2_JAR} --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT \
   --build-arg BUILD_TIME=$BUILD_TIME -t "${TAG_BRANCH2}" .

echo "**** Removing jar from Docker build context"
rm ${BUILD_DIR2}/${SVC2_JAR}

# Build image for jobsworker
echo "======================================================================"
echo "Building local image for service ${SVC3TAG} using primary tag: $TAG_BRANCH3"
echo "======================================================================"
# Move to the build directory
cd $BUILD_DIR3 || exit
echo "**** Removing any old service jar files from Docker build context"
rm -f $SVC3_JAR
echo "**** Copying $SVC3_JAR to Dockerfile context"
cp $BUILD_DIRT3/${SVC3_JAR} $BUILD_DIR3

echo "**** Building docker image: $TAG_BRANCH3"
docker build -f Dockerfile --build-arg VER=$VER --build-arg GIT_COMMIT=$GIT_COMMIT --build-arg BUILD_TIME=$BUILD_TIME \
    -t "${TAG_UNIQ3}" .

echo "**** Removing jar from Docker build context"
rm ${BUILD_DIR3}/${SVC3_JAR}


# Push to remote repo
if [ "x$2" = "x-push" ]; then
  echo "Pushing images to docker hub."
  # NOTE: Use current login. Jenkins job does login
  docker push "$TAG_BRANCH1"
  docker push "$TAG_BRANCH2"
  docker push "$TAG_BRANCH3"
fi


echo "**** Removing exploded war directory for service jobsapi"
echo "======================================================================"
echo "rm -fr ${BUILD_DIR1:=/tmp}/${SVC1_WAR:=jobsapi}"
rm -fr ${BUILD_DIR1:=/tmp}/${SVC1_WAR:=jobsapi}
echo "======================================================================"

cd "$RUN_DIR"
