package edu.utexas.tacc.tapis.jobs.utils;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils.AuditData;

public final class JobWorkerAudit 
{
	/** This method initializes an audit record in the context of a worker executing a job.
	 * 
	 * @param job the non-null job that is currently executing
	 * @param action the action that the job is requesting, should always be specified
	 * @return a partially complete audit data object
	 */
	public static AuditData getAuditData(Job job, AuditUtils.AUDIT_ACTIONS action)
	{
		var auditData = new AuditData();
    	auditData.component  = AuditUtils.AUDIT_JOBSWORKER;
    	auditData.action     = action != null ? action.toString() : AuditUtils.AUDIT_NULL;
    	auditData.trackingId = job.getUuid();
    	
    	// Auditing must not occur before the worker is fully initialized.
    	// If this invariant is upheld, then the following 2 call won't fail.
    	var siteId           = RuntimeParameters.getInstance().getSiteId();
    	auditData.jwtTenant  = TenantManager.getInstance().getSiteAdminTenantId(siteId);;
    	auditData.jwtUser    = TapisConstants.SERVICE_NAME_JOBS;
        auditData.oboTenant  = job.getTenant();
    	auditData.oboUser    = job.getOwner();

    	// Tracking ids.
		auditData.trackingId = AuditUtils.TRACKING_PREFIX_JOB_UUID + job.getUuid();
		auditData.parentTrackingId = job.getTrackingId() != null ? job.getTrackingId() : AuditUtils.AUDIT_NULL;
		
    	// The data field must be json and is initialized to be empty 
    	// but the caller can replace its value.
		auditData.data       = TapisConstants.EMPTY_JSON; 
		
		return auditData;
	}
}
