package edu.utexas.tacc.tapis.jobs.api;

public class JobSharedAppVersion {
    String jobUuid;
    String appId;
    String appVersion;
    String tenant;

    public JobSharedAppVersion(String uuid, String appId, String appVersion, String tenant ){
            this.jobUuid = uuid;
            this.appId = appId;
            this.appVersion = appVersion;
            this.tenant = tenant;
    }

    public String getAppId(){

            return appId;
    }
    public String getAppVersion() {
            return appVersion;
    }
    public String getJobUuid() {
            return jobUuid;
    }
    
    public String getTenant() {
    	return tenant;
    }
    
}
