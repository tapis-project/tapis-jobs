# Backup of script used in Jenkins job TapisJava->3_ManualBuildDeploy->jobs
# NOTE there is no guarantee that this copy in the git repo is in sync with
#      the version in use for the Jenkins job at jenkins-cic.tacc.utexas.edu
#!/bin/bash
source ~/.bash_profile

SVC_NAME=jobs

sdk use java 21.0.5-tem
sdk use maven 3.6.3

java -version
mvn  -version

echo "*******************"
export TAPIS_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)
echo "tapis-java TAPIS_VERSION = ${TAPIS_VERSION}"
export GIT_COMMIT=$(git log -1 --pretty=format:"%h")
export GIT_BRANCH=$GIT_BRANCH
echo "git commit revision $GIT_COMMIT"
echo "GIT_BRANCH=$GIT_BRANCH"
echo "*******************"

echo "************************ Building service: $SVC_NAME"
echo "************************ Full install: mvn clean install"
mvn clean install
RET_CODE=$?
if [ $RET_CODE -ne 0 ]; then
  echo "======================================================================"
  echo "Build of merged branch failed."
  echo "Exiting ..."
  echo "======================================================================"
  exit $RET_CODE
fi

echo "************************ Build jobslib/shaded-pom"
mvn -f tapis-jobslib/shaded-pom.xml package
RET_CODE=$?
if [ $RET_CODE -ne 0 ]; then
  echo "======================================================================"
  echo "Build of shaded jobslib jar failed."
  echo "Exiting ..."
  echo "======================================================================"
  exit $RET_CODE
fi

if [ "$JobsDeploy" == "true" -a "$JobsPublish" == "true" ]; then
  # Set flag indicating this is a manual deploy
  # docker_build.sh script will tag image with tapis/<service_name>:dev
  export TAPIS_DEPLOY_MANUAL=true
fi
# Jobs Publish Image
echo "JobsPublish = $JobsPublish"
if [ "$JobsPublish" == "true" ]; then
  cd $WORKSPACE
  echo "************************ Building & Publishing Image"
# Build agent nodes should already logged in to docker
#  docker login -u $DCKR_USERNAME -p $DCKR_PASSWD
  deployment/docker_build.sh dev -push
  RET_CODE=$?
  if [ $RET_CODE -ne 0 ]; then
    echo "======================================================================"
    echo "Docker build and push failure."
    echo "Exiting ..."
    echo "======================================================================"
    exit $RET_CODE
  fi
  cd $WORKSPACE
#  echo "************************ Removing docker images"
#  release/docker_rmi.sh dev
#  RET_CODE=$?
#  if [ $RET_CODE -ne 0 ]; then
#    echo "======================================================================"
#    echo "Docker rmi failure."
#    echo "Exiting ..."
#    echo "======================================================================"
#    exit $RET_CODE
#  fi
#  cd $WORKSPACE
fi

# Jobs Deploy Service
echo "JobsDeploy = $JobsDeploy"
if [ "$JobsDeploy" == "true" ]; then
  echo "************************ Deploying Service: $SVC_NAME"
  # SSH to cic02 as the tapisdev account with access to the tapisdev k8s namespace -
  ssh -i $HOME/.ssh/Jenkins-2018 tapisdev@129.114.35.220 \
     "cd ~/tapis-kube/$SVC_NAME/api; ./burndown; ./burnup; cd ../workers; ./burndown; ./burnup; cd ../readers; ./burndown; ./burnup"
fi
