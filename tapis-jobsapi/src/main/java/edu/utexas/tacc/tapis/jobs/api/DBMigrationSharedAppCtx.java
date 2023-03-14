package edu.utexas.tacc.tapis.jobs.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.AppsClient;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public class DBMigrationSharedAppCtx {
	
	/* **************************************************************************** */
	 /*                                    Fields                                   */
	 /* *************************************************************************** */
	 // Local logger.
	 private static Logger _log = LoggerFactory.getLogger(DBMigrationSharedAppCtx.class);
	 public static final int HTTP_INTERNAL_SERVER_ERROR = 500;
   
	 
	 public void migrate() throws Exception {
		
		Connection conn = JobsDao.getDataSource().getConnection();
		
		String sql = "SELECT uuid, app_id, app_version, tenant FROM jobs WHERE shared_app_ctx='true'";
		
		 // Select all the jobs where sharedAppCtx is true and needs update
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        ArrayList<JobSharedAppVersion>uuidAppList = new ArrayList<JobSharedAppVersion>();
        String uuid = "";
        String appId = "";
        String appVersion = "";
        String tenant = "";
        do {
	        uuid = rs.getString(0);
	        appId = rs.getString(1);
	        appVersion = rs.getString(2);
	        tenant = rs.getString(3);
	        JobSharedAppVersion jobSharedApp = new JobSharedAppVersion(uuid,appId,appVersion,tenant);
	        uuidAppList.add(jobSharedApp);
        } while(rs.next());
        
        AppsClient appClient = getAppsClient();
        HashMap<String,String> jobAppOwner = new HashMap<String,String>();
        
        // Get the shared apps and app's owners 
        // These owners will be added to the sharedAppCtx field of respective job
        for(JobSharedAppVersion japp: uuidAppList) {
        	TapisApp app = loadAppDefinition(appClient,japp.getAppId(),japp.getAppVersion(), japp.getJobUuid(),japp.getTenant());
        	
        	String appOwner = app.getSharedAppCtx();
        	jobAppOwner.put(japp.getJobUuid(),appOwner); 
               			
        } 
         // Update in the Jobs table
        UpdateJobsTable(conn, jobAppOwner);
	}
	
	 /*
	  * Update the shared_app_ctx field in Tapis Jobs Table
	  */
	void UpdateJobsTable(Connection conn, HashMap<String,String> jobAppOwner) {
		String UPDATE = "UPDATE jobs SET shared_app_ctx=? WHERE uuid= ?";
		PreparedStatement jobsUpdate = null;
		try {
			jobsUpdate = conn.prepareStatement(UPDATE);
		} catch (SQLException e) {
			String msg = MsgUtils.getMsg("JOBS_UPDATE_SHAREDAPPCTX_CONNECTION_ERROR",e.getMessage());
			_log.error(msg,e);
		    
		}
		
		try {
		for(String uuid: jobAppOwner.keySet()) {
		
			jobsUpdate.setString(1, jobAppOwner.get(uuid));
			jobsUpdate.setString(2,  uuid);
			jobsUpdate.addBatch();
		    
		}
		int[] rows = jobsUpdate.executeBatch();
		for(int k = 0; k < rows.length; k++) {
			if(rows[k] != 1) {
				
				_log.warn(MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", "jobs", rows[k], 1));
				_log.debug(MsgUtils.getMsg("DB_UPDATE_JOB_SHAREDAPP_CTX_FAILURE", "jobs","" ));
			}
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
            //throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
            if (conn != null) 
                try {conn.close();}
                  catch (Exception e) 
                  {
                      // If commit worked, we can swallow the exception.  
                      // If not, the commit exception will be thrown.
                      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                      _log.error(msg, e);
                  }
        }
		
	}
	 /* ---------------------------------------------------------------------------- */
    /* getServiceClient:                                                             */
    /* ----------------------------------------------------------------------------  */
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
        TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
        String siteId = threadContext.getSiteId();
	   	String svcTenant = TenantManager.getInstance().getSiteAdminTenantId(siteId);
	   	
        try {
            appsClient = ServiceClients.getInstance().getClient(TapisConstants.APPS_SERVICE, svcTenant, AppsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "Apps", "getClient", svcTenant, TapisConstants.APPS_SERVICE);
            throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
        }

        return appsClient;
    }
    
   

    private TapisApp loadAppDefinition(AppsClient appsClient, String appId, String appVersion, String jobUuid, String tenant)
    	      throws TapisException
    	    {
    	        // Load the system definition.
    	        TapisApp app = null;
    	        try {app = appsClient.getApp(appId, appVersion);} 
    	        catch (TapisClientException e) {
    	            // Look for a recoverable error in the exception chain. Recoverable
    	            // exceptions are those that might indicate a transient network
    	            // or server error, typically involving loss of connectivity.
    	            Throwable transferException = 
    	                TapisUtils.findFirstMatchingException(e, TapisConstants.CONNECTION_EXCEPTION_PREFIX);
    	            if (transferException != null) {
    	                throw new TapisServiceConnectionException(transferException.getMessage(), 
    	                            e, RecoveryUtils.captureServiceConnectionState(
    	                               appsClient.getBasePath(), TapisConstants.APPS_SERVICE));
    	            }
    	            
    	            // Determine why we failed.
    	            String appString = appId + "-" + appVersion;
    	            String msg;
    	            //msg = "";
    	            switch (e.getCode()) {
    	                case 400:
    	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_INPUT_ERROR", appString, jobUuid, 
    	                                         tenant);
    	                break;
    	                
    	                case 401:
    	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_AUTHZ_ERROR", appString, "READ", 
    	                                          "", tenant);
    	                break;
    	                
    	                case 404:
    	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_NOT_FOUND", appString,"", 
    	                                          tenant);
    	                break;
    	                
    	                default:
    	                    msg = MsgUtils.getMsg("TAPIS_APPLOAD_INTERNAL_ERROR", appString, "", 
    	                                         tenant);
    	            }
    	            throw new TapisImplException(msg, e, e.getCode());
    	        }
    	        catch (Exception e) {
    	            String appString = appId + "-" + appVersion;
    	            String msg = MsgUtils.getMsg("TAPIS_APPCLIENT_INTERNAL_ERROR", appString, TapisConstants.APPS_SERVICE, "Admin Tenant"
    	                                         );
    	            throw new TapisImplException(msg, e, HTTP_INTERNAL_SERVER_ERROR);
    	        }
    	        
    	       
    	        
    	        return app;
    	    }

	
}
