#!/bin/bash
# source ~/.bash_profile
JobsPublish=true
DCKR_TAG="staging"

if [ -z "$DCKR_USER" -o -z "$DCKR_PW" ]; then
  echo "Please set DCKR_USER and DCKR_PW"
  exit 1
fi
# sdk use java 17.0.2-open
sdk use java 17.0.6-tem
sdk use maven 3.6.3

java -version
mvn  -version

echo "*******************"
export TAPIS_VER=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)
export TAPIS_VERSION=${TAPIS_VER}-hotfix
echo "tapis-jobs TAPIS_VERSION = ${TAPIS_VERSION}"
export GIT_COMMIT=$(git log -1 --pretty=format:"%h")
export GIT_BRANCH=$GIT_BRANCH
echo "git commit revision $GIT_COMMIT"
echo "GIT_BRANCH=$GIT_BRANCH"
	docker login -u $DCKR_USER -p $DCKR_PW
echo "*******************"
exit 0

echo '************************ Full install: mvn clean install'
mvn clean install

echo '************************ Build jobslib/shaded-pom'
mvn -f tapis-jobslib/shaded-pom.xml package

# Jobs Publish Image
echo "$JobsPublish"
if [ "$JobsPublish" == "true" ]; then
	echo '************************ Building & Publishing Jobs Image'
	docker login -u $USERNAME -p $PASSWD
	deployment/build-jobsapi.sh
	docker tag tapis/jobsapi:${TAPIS_VERSION} tapis/jobsapi:$DCKR_TAG
	docker push tapis/jobsapi:$DCKR_TAG
    
    echo '************************ Building & Publishing Jobs Migrate Image'
	deployment/build-jobsmigrate.sh
	docker tag tapis/jobsmigrate:${TAPIS_VERSION} tapis/jobsmigrate:$DCKR_TAG
	docker push tapis/jobsmigrate:$DCKR_TAG
    
    echo '************************ Building & Publishing Jobs Worker Image'
	deployment/build-jobsworker.sh
	docker tag tapis/jobsworker:${TAPIS_VERSION} tapis/jobsworker:$DCKR_TAG
	docker push tapis/jobsworker:$DCKR_TAG
fi

# Jobs Deploy Service
# echo "$JobsDeploy"
# if [ "$JobsDeploy" == "true" ]; then
# 	echo '************************ Deploying Jobs Service'
# 	# SSH to cic02 as the tapisdev account with access to the tapisdev k8s namespace -
# 	# delete pod to make it automatically pull the latest. 
# 	ssh -i $HOME/.ssh/Jenkins-2018 tapisdev@cic02 'cd ~/tapis-kube/jobs/api; ./burndown; ./burnup'
#     ssh -i $HOME/.ssh/Jenkins-2018 tapisdev@cic02 'cd ~/tapis-kube/jobs/workers; ./burndown; ./burnup'
#     ssh -i $HOME/.ssh/Jenkins-2018 tapisdev@cic02 'cd ~/tapis-kube/jobs/readers; ./burndown; ./burnup'
# fi
