# Backup of script used in Jenkins job TapisJava->3_ManualBuildDeploy->jobs
# NOTE there is no guarantee that this copy in the git repo is in sync with
#      the version in use for the Jenkins job at jenkins-cic.tacc.utexas.edu
#!/bin/bash
source ~/.bash_profile

SVC_NAME=jobs

# sdk use java 17.0.2-open
#sdk use java 17.0.6-tem
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

# Jobs Publish Image
echo "JobsPublish = $JobsPublish"
if [ "$JobsPublish" == "true" ]; then
	echo "************************ Building & Publishing Jobs Image"
	docker login -u $DCKR_USERNAME -p $DCKR_PASSWD
	deployment/build-jobsapi.sh
	docker tag tapis/jobsapi:${TAPIS_VERSION} tapis/jobsapi:dev
	docker push tapis/jobsapi:dev
    
    echo "************************ Building & Publishing Jobs Migrate Image"
	deployment/build-jobsmigrate.sh
	docker tag tapis/jobsmigrate:${TAPIS_VERSION} tapis/jobsmigrate:dev
	docker push tapis/jobsmigrate:dev
    
    echo "************************ Building & Publishing Jobs Worker Image"
	deployment/build-jobsworker.sh
	docker tag tapis/jobsworker:${TAPIS_VERSION} tapis/jobsworker:dev
	docker push tapis/jobsworker:dev
fi

# Jobs Deploy Service
echo "JobsDeploy = $JobsDeploy"
if [ "$JobsDeploy" == "true" ]; then
	echo "************************ Deploying Service: $SVC_NAME"
	# SSH to cic02 as the tapisdev account with access to the tapisdev k8s namespace -
	# delete pod to make it automatically pull the latest.
	ssh -i $HOME/.ssh/Jenkins-2018 tapisdev@129.114.35.220 \
 "cd ~/tapis-kube/$SVC_NAME/api; ./burndown; ./burnup; cd ../workers; ./burndown; ./burnup; cd ../readers; ./burndown; ./burnup"
 #  ssh -i $HOME/.ssh/Jenkins-2018 tapisdev@129.114.35.220 'cd ~/tapis-kube/jobs/workers; ./burndown; ./burnup'
 #  ssh -i $HOME/.ssh/Jenkins-2018 tapisdev@129.114.35.220 'cd ~/tapis-kube/jobs/readers; ./burndown; ./burnup'
fi

