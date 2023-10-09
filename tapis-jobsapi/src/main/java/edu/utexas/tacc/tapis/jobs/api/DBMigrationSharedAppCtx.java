package edu.utexas.tacc.tapis.jobs.api;

import static edu.utexas.tacc.tapis.client.shared.Utils.DEFAULT_SELECT_ALL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.TenantManager;

/**
 * This migration utility needs to be run after the V014__UpdateSharedAppCtx.sql Flyway
 * migration script was run.
 * Context of Migration:
 * 		We observed a privilege escalation issue in the shared app context. The fix for this issue requires the
 *      jobs table data migrations. It consists of two steps:
 * 		1. V014__UpdateSharedAppCtx.sql Flyway migration. This script changes the shared_app_ctx column from type 
 *    	   boolean to string. In the rows where the shared_app_ctx column has the value 'false', 
 *         the shared_app_ctx's value is replaced with ''(empty string), and the rows get updated. 
 *         No change is made to the rows where the shared_app_ctx column has value the 'true'.
 *      2. This DBMigrationSharedAppCtx replaces 'true' in the shared_app_ctx column in jobs table with 
 *         shared app owner value for each corresponding job. It obtains this value by making an API request
 *         to Apps service. 
 *         Since it requires querying to Apps service for which the Jobs service require service JWT and site information,
 *         we decided to write this utility instead of putting it in the Flyway script.
 */
final class DBMigrationSharedAppCtx 
{
     /* **************************************************************************** */
	 /*                                    Fields                                    */
	 /* **************************************************************************** */
	 // Local logger.
	 private static final Logger _log = LoggerFactory.getLogger(DBMigrationSharedAppCtx.class);
	 
	 // Get the site on which this service is running.
	 private final String _siteId;
	 
     /* **************************************************************************** */
	 /*                                 Constructors                                 */
	 /* **************************************************************************** */
	 DBMigrationSharedAppCtx(String siteId) {_siteId = siteId;}
	 
     /* **************************************************************************** */
	 /*                                   Methods                                    */
	 /* **************************************************************************** */
	 public void migrate() throws Exception {
			        
		// Connect to db.
		Connection conn = JobsDao.getDataSource().getConnection();
		
		// This code assumes there's no user named 'true'.
		String sql = "SELECT uuid, app_id, app_version, tenant FROM jobs WHERE shared_app_ctx='true'";
		
		 // Select all the jobs where sharedAppCtx is true and needs update
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
      	    
	    try {
	      // Return null if the results are empty
	      if (!rs.next()) {
	    	  System.out.println("**** DB MIGRATION: no jobs with sharedAppCtx=true is found. No jobs are updated ****\n");
		      return;
	      }
	    } catch (Exception e) {
	      String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
	      throw new TapisJDBCException(msg, e);
	    }
	    ArrayList<JobSharedAppVersion>uuidAppList = new ArrayList<JobSharedAppVersion>();
	  do {
	        String uuid = rs.getString(1);
	        String appId = rs.getString(2);
	        String appVersion = rs.getString(3);
	        String tenant = rs.getString(4);
	        JobSharedAppVersion jobSharedApp = new JobSharedAppVersion(uuid,appId,appVersion,tenant);
	        uuidAppList.add(jobSharedApp);
	       	               
        } while(rs.next());
        
        
        // Clean up.
        rs.close();
        pstmt.close();
        conn.commit();
        
        AppsClient appClient = getAppsClient();
        HashMap<String,String> jobAppOwner = new HashMap<String,String>();
        
        // Get the shared apps and app's owners 
        // These owners will be added to the sharedAppCtx field of respective job
        for(JobSharedAppVersion japp: uuidAppList) {
        	// We either get the app or null.
        	TapisApp app = loadAppDefinition(appClient, japp.getAppId(), japp.getAppVersion(), 
        			                         japp.getJobUuid(), japp.getTenant());
        	if (app == null) continue; // problem already logged, try next one
        	
        	String appOwner = app.getOwner();
        	
        	_log.debug("appOwner: " + appOwner + " japp.getAppId(): " +  japp.getAppId()
        	+ " AppVersion: " + japp.getAppVersion()+ " job uuid:"+ japp.getJobUuid() 
        	+ " tenant:" +japp.getTenant());
        	if(StringUtils.isBlank(appOwner)) {
        		_log.debug("Do not update the jobtable with blank appowner");
        		continue;
        	}	
        	jobAppOwner.put(japp.getJobUuid(), appOwner); 
        } 
        
        // Update in the Jobs table
        UpdateJobsTable(conn, jobAppOwner);
        
        // Close db connection.
        try {conn.close();}
        catch (Exception e) 
        {
            // If commit worked, we can swallow the exception.  
            // If not, the commit exception will be thrown.
            String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
            _log.error(msg, e);
        }
	}
	
	 /*
	  * Update the shared_app_ctx field in Tapis Jobs Table
	  */
	void UpdateJobsTable(Connection conn, HashMap<String,String> jobAppOwner) {
		String UPDATE = "UPDATE jobs SET shared_app_ctx=? WHERE uuid=?";
		PreparedStatement jobsUpdate = null;
		try {
			jobsUpdate = conn.prepareStatement(UPDATE);
		} catch (Exception e) {
			String msg = MsgUtils.getMsg("DB_JOBS_UPDATE_SHAREDAPPCTX_CONNECTION_ERROR",e.getMessage());
			_log.error(msg,e);
			return; // Not much we can do.
		}
		
		try {
			for(String uuid: jobAppOwner.keySet()) {
			
				jobsUpdate.setString(1, jobAppOwner.get(uuid));
				jobsUpdate.setString(2,  uuid);
				jobsUpdate.addBatch();
			    
			}
			int[] rows = jobsUpdate.executeBatch();
			int expectedRowsUpdate = rows.length;
			int totalRowsUpdated = 0;
			for(int k = 0; k < rows.length; k++) {
				if(rows[k] != 1) {
					_log.debug(MsgUtils.getMsg("DB_UPDATE_JOB_SHAREDAPP_CTX_FAILURE", "jobs", rows[k]));
				} else totalRowsUpdated = totalRowsUpdated + 1;
			}
	        if (totalRowsUpdated < expectedRowsUpdate ) {
	        	_log.warn(MsgUtils.getMsg("DB_INSERT_UNEXPECTED_ROWS",expectedRowsUpdate,totalRowsUpdated, UPDATE,jobAppOwner ));
	        }
	        conn.commit();
		}
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_UPDATE_SHAREDAPPCTX_VALUE_ERROR",e.getMessage());
            _log.error(msg,e);
        }
	}
	
	/* ---------------------------------------------------------------------------- */
    /* getServiceClient:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** Get a new or cached Apps service client.  This can only be called after
     * the request tenant and owner have be assigned.
     * 
     * @return the client
     * @throws TapisImplException
     */
    public AppsClient getAppsClient() throws TapisImplException
    {
        // Get the application client for this user@tenant.
    	AppsClient appsClient = null;
	   	String svcTenant = TenantManager.getInstance().getSiteAdminTenantId(_siteId);
	   	
        try {
            appsClient = ServiceClients.getInstance().getClient(TapisConstants.SERVICE_NAME_JOBS, svcTenant, AppsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "Apps", "getClient", svcTenant, TapisConstants.APPS_SERVICE);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }

        return appsClient;
    }
    
	/* ---------------------------------------------------------------------------- */
    /* loadAppDefinition:                                                            */
    /* ---------------------------------------------------------------------------- */
    private TapisApp loadAppDefinition(AppsClient appsClient, String appId, String appVersion, 
    		                           String jobUuid, String tenant)
     throws TapisException
    {
    	// Load the system definition.
    	TapisApp app = null;
    	try {
    		 //app = appsClient.getApp(appId, appVersion);
    	     app = appsClient.getApp(appId, appVersion, false, "admin", "allAttributes",tenant);} 
    	   	catch (TapisClientException e) {
    	   		// Determine why we failed.
    	        String appString = appId + "-" + appVersion;
    	        String msg;
    	        String appStatus="";
  	            switch (e.getCode()) {
   	                case 400:
   	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_INPUT_ERROR", appString, jobUuid, tenant);
  	                break;
    	                
   	                case 401:
   	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_AUTHZ_ERROR", appString, "READ", "", tenant);
   	                break;
   	                
   	                case 404:
   	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_NOT_FOUND", appString, "unknown", tenant);
   	                    appStatus="Probably the app " + appString +" has been deleted. Please check the app.";
   	                break;
   	                
   	                default:
   	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_INTERNAL_ERROR", appString, "", tenant);
    	        }
    	          _log.debug(msg + " for job " + jobUuid + ". "+ appStatus);
    	   	}
    	    catch (Exception e) {
    	    	String appString = appId + "-" + appVersion;
    	        String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", appString, 
    	          		                     TapisConstants.APPS_SERVICE, "Admin Tenant");
    	        _log.error(msg);
    	    }
    	        
    	return app;
    }

    /* **************************************************************************** */
	/*                            Class JobSharedAppVersion                         */
	/* **************************************************************************** */
    private static final class JobSharedAppVersion 
    {
    	private String jobUuid;
    	private String appId;
    	private String appVersion;
    	private String tenant;

        JobSharedAppVersion(String uuid, String appId, String appVersion, String tenant ){
                this.jobUuid = uuid;
                this.appId = appId;
                this.appVersion = appVersion;
                this.tenant = tenant;
        }

        String getAppId(){
                return appId;
        }
        String getAppVersion() {
                return appVersion;
        }
        String getJobUuid() {
                return jobUuid;
        }
        String getTenant() {
        	return tenant;
        }
    }
	
}
