package edu.utexas.tacc.tapis.jobs.api.resources;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.api.responses.RespCancelJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.dto.JobCancelDisplay;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

@Path("/")
public class JobCancelResource extends AbstractResource {
	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobCancelResource.class);

    private final String className = getClass().getSimpleName();

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    /* Jax-RS context dependency injection allows implementations of these abstract
     * types to be injected (ch 9, jax-rs 2.0):
     * 
     *      javax.ws.rs.container.ResourceContext
     *      javax.ws.rs.core.Application
     *      javax.ws.rs.core.HttpHeaders
     *      javax.ws.rs.core.Request
     *      javax.ws.rs.core.SecurityContext
     *      javax.ws.rs.core.UriInfo
     *      javax.ws.rs.core.Configuration
     *      javax.ws.rs.ext.Providers
     * 
     * In a servlet environment, Jersey context dependency injection can also 
     * initialize these concrete types (ch 3.6, jersey spec):
     * 
     *      javax.servlet.HttpServletRequest
     *      javax.servlet.HttpServletResponse
     *      javax.servlet.ServletConfig
     *      javax.servlet.ServletContext
     *
     * Inject takes place after constructor invocation, so fields initialized in this
     * way can not be accessed in constructors.
     */ 
     @Context
     private HttpHeaders        _httpHeaders;
  
     @Context
     private Application        _application;
  
     @Context
     private UriInfo            _uriInfo;
  
     @Context
     private SecurityContext    _securityContext;
  
     @Context
     private ServletContext     _servletContext;
  
     @Context
     private HttpServletRequest _request;

     /* **************************************************************************** */
     /*                                Public Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* getJob:                                                                      */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/cancel")
     @Produces(MediaType.APPLICATION_JSON)
     public Response cancelJob(@PathParam("jobUuid") String jobUuid)
     {
       String opName = "cancelJob";
       String msg;
       // ------------------------- Retrieve and validate thread context -------------------------
       Response resp = JobsApiUtils.checkContext(TapisThreadLocal.tapisThreadContext.get());
       if (resp != null) return resp;

       // Create a user that collects together tenant, user and request information needed by the service call
       ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) _securityContext.getUserPrincipal());
       // Trace this request.
       if (_log.isTraceEnabled())
           JobsApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "jobUuid="+jobUuid);

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

       // ------------------------- Input Processing -------------------------
       if (StringUtils.isBlank(jobUuid)) {
         msg = JobUtils.getMsgAuth("JOBS_MISSING_PARAMETER", rUser, "jobUuid");
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // ------------------------- Create Context ---------------------------
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();

       // ------------------------- Retrieve Job -----------------------------
       Job job = null;
       var jobsImpl = JobsImpl.getInstance();
       try {
           job = jobsImpl.getJobByUuid(jobUuid, threadContext.getOboUser(), threadContext.getOboTenantId());
       }
       catch (TapisImplException e) {
           _log.error(e.getMessage());
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
       }
       catch (Exception e) {
           _log.error(e.getMessage(), e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
       }

       // ------------------------- Process Results --------------------------
       // Adjust status based on whether we found the job.
       if (job == null) {
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), r)).build();
       }
       // ------------------------- Check the Job's status -----------------------------
       // If job is in terminal state then job cancellation cannot be performed.
       
       if(job.getStatus().isTerminal()) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           msg = JobsApiUtils.getMsgAuth("JOBSAPI_IN_TERM_STATE", rUser, jobUuid, job.getStatus());
           _log.warn(msg);
    	   return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, r)).build();
       }

       // If cancelling this type of job is not supported then reject the request here.
       // See also JobCancelerFactory
       // Extract required information from app and job.
       JobExecutionContext jobCtx;
       TapisApp app;
       TapisSystem system;
       // Wrap calls that can throw TapisException, return 500 on exception
       try
       {
         jobCtx = new JobExecutionContext(job, new JobsDao());
         app = jobCtx.getApp();
         system = jobCtx.getExecutionSystem();
       }
       catch (TapisException te)
       {
         msg = JobUtils.getMsgAuth("JOBS_CANCEL_OP_ERR", rUser, jobUuid, te.getMessage());
         _log.error(msg);
         return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       RuntimeEnum runtime = app.getRuntime();
       JobType jobType = job.getJobType();
       SchedulerTypeEnum schedulerType = system.getBatchScheduler();
       // Check that we support cancel for this type of job. If not, return 400.
       msg = JobUtils.checkCancelSupported(jobType, runtime, schedulerType, jobUuid);
       if (!StringUtils.isBlank(msg))
       {
         String msg2 = JobUtils.getMsgAuth("JOBS_UNSUPPORTED_CANCEL_OP_API", rUser, msg);
         _log.error(msg2);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg2)).build();
       }

       //------------------------- Cancel the Job  -----------------------------
       // initiate the cancellation.
       if (!jobsImpl.doCancelJob(jobUuid, threadContext))
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(JobUtils.getMsg("JOBS_QMGR_POST_CANCEL", jobUuid))).build();
                       
       // ---------------------------- Success -------------------------------
       // Success.
       JobCancelDisplay cancelMsg = new JobCancelDisplay();
       msg = JobUtils.getMsg("JOBS_JOB_CANCEL_ACCEPTED", jobUuid);
       cancelMsg.setMessage(msg);
       
       RespCancelJob r = new RespCancelJob(cancelMsg); 
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               JobUtils.getMsg("JOBS_JOB_CANCEL_ACCEPTED_DETAILS", jobUuid),r)).build();
     }
     
     

}
