package edu.utexas.tacc.tapis.jobs.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
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
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.model.SubmitContext;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqUserEvent;
import edu.utexas.tacc.tapis.jobs.api.responses.RespGetResubmit;
import edu.utexas.tacc.tapis.jobs.api.responses.RespSubmitJob;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.dao.JobResubmitDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.events.JobEventManager;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.JobResubmit;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientFactory;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.HTMLizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

import static edu.utexas.tacc.tapis.jobs.model.Job.NOTES_FIELD;

@Path("/")
public class JobSubmitResource 
 extends AbstractResource
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobSubmitResource.class);
    
    // Json schema resource files.
    private static final String FILE_JOB_SUBMIT_REQUEST = 
        "/edu/utexas/tacc/tapis/jobs/api/jsonschema/SubmitJobRequest.json";
    private static final String FILE_USER_EVENT_REQUEST = 
        "/edu/utexas/tacc/tapis/jobs/api/jsonschema/UserEventRequest.json";

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
     /* submitJob:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/submit")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     public Response submitJob(InputStream payloadStream)
     {
       String opName = "submitJob";
       // ------------------------- Retrieve and validate thread context -------------------------
       Response resp = JobsApiUtils.checkContext(TapisThreadLocal.tapisThreadContext.get());
       if (resp != null) return resp;

       // Create a user that collects together tenant, user and request information needed by the service call
       ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) _securityContext.getUserPrincipal());
       // Trace this request.
       if (_log.isTraceEnabled())
           JobsApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString());

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

       // The shared code takes it from here.
       return doSubmit(rUser, payloadStream);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* resubmitJob:                                                                 */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/resubmit")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     public Response resubmitJob(@PathParam("jobUuid") String jobUuid)
     {
       String opName = "resubmitJob";
       // ------------------------- Retrieve and validate thread context -------------------------
       Response resp = JobsApiUtils.checkContext(TapisThreadLocal.tapisThreadContext.get());
       if (resp != null) return resp;

       // Create a user that collects together tenant, user and request information needed by the service call
       ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) _securityContext.getUserPrincipal());
       // Trace this request.
       if (_log.isTraceEnabled())
           JobsApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "jobUuid="+jobUuid);

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

       // ------------------------- Validate Parameter -----------------------
       if (StringUtils.isAllBlank(jobUuid)) {
         String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "resubmit", "jobuuid");
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // ------------------------- Get Resubmit -----------------------------
       // We have a job to resubmit now go lookup the stored job definition
       JobResubmit jobResubmit;
       try {
           var jobResubmitDao = new JobResubmitDao();
           jobResubmit = jobResubmitDao.getJobResubmitByUUID(jobUuid);
       } catch (Exception e) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_RESUBMIT_NOT_FOUND", rUser, jobUuid, e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // Make sure we got something.
       if (jobResubmit == null) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_RESUBMIT_NOT_FOUND", rUser, jobUuid, "unknown job uuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // The shared code takes it from here.
       return doSubmit(rUser, jobResubmit.getJobDefinition());
     }
     
     /* ---------------------------------------------------------------------------- */
     /* getResubmitRequestJson:                                                      */
     /* ---------------------------------------------------------------------------- */
     @GET
     @Path("/{jobUuid}/resubmit_request")
     @Produces(MediaType.APPLICATION_JSON)
     public Response getResubmitRequestJson(@PathParam("jobUuid") String jobUuid)
     {
       String opName = "getResubmitRequest";
         // ------------------------- Retrieve and validate thread context -------------------------
         Response resp = JobsApiUtils.checkContext(TapisThreadLocal.tapisThreadContext.get());
         if (resp != null) return resp;

       // Create a user that collects together tenant, user and request information needed by the service call
       ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) _securityContext.getUserPrincipal());
       // Trace this request.
       if (_log.isTraceEnabled())
           JobsApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),"jobUuid="+jobUuid);

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

       // ------------------------- Validate Parameter -----------------------
       if (StringUtils.isAllBlank(jobUuid)) {
         String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "resubmit_reques_json", "jobuuid");
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // ------------------------- Get Resubmit -----------------------------
       // We need resubmit request now go lookup the stored job definition
       JobResubmit jobResubmit;
       try {
           var jobResubmitDao = new JobResubmitDao();
           jobResubmit = jobResubmitDao.getJobResubmitByUUID(jobUuid);
       } catch (Exception e) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_RESUBMIT_NOT_FOUND", rUser, jobUuid, e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // Make sure we got something.
       if (jobResubmit == null) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_RESUBMIT_NOT_FOUND", rUser, jobUuid, "unknown job uuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // ------------------------- Input Processing -------------------------
       // Parse and validate the json in the request payload, which must exist.
       ReqSubmitJob payload = null;
       try {payload = getPayload(jobResubmit.getJobDefinition(), FILE_JOB_SUBMIT_REQUEST, ReqSubmitJob.class);} 
       catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", "resubmitrequestjson", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       RespGetResubmit r = new RespGetResubmit(payload);
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               JobUtils.getMsg("JOBS_RESUBMIT_REQUEST_RETRIEVED", jobUuid), r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* sendEvent:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/sendEvent")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     public Response sendEvent(@PathParam("jobUuid") String jobUuid, InputStream payloadStream)
     {
       String opName = "sendEvent";
       // ------------------------- Retrieve and validate thread context -------------------------
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       // Check that we have all we need from the context, the jwtTenantId and jwtUserId
       // Utility method returns null if all OK and appropriate error response if there was a problem.
       Response resp = JobsApiUtils.checkContext(TapisThreadLocal.tapisThreadContext.get());
       if (resp != null) return resp;

       // Create a user that collects together tenant, user and request information needed by the service call
       ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) _securityContext.getUserPrincipal());
       // Trace this request.
       if (_log.isTraceEnabled())
           JobsApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString());

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

       // ------------------------- Validate Payload -------------------------
       // Read the payload into a string.
       String json = null;
       try {json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8);}
         catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job send event", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
         }
       
       // ------------------------- Input Processing -------------------------
       // Parse and validate the json in the request payload, which must exist.
       ReqUserEvent payload = null;
       try {payload = getPayload(json, FILE_USER_EVENT_REQUEST, ReqUserEvent.class);} 
       catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", "sendEvent", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       // ------------------------- Inspect Job ------------------------------
       // Retrieve the target job.
       Job job = null;
       try {
         final boolean throwNotFound = true;
         var jobsDao = new JobsDao();
         job = jobsDao.getJobByUUID(jobUuid, throwNotFound);
       }
       catch (TapisNotFoundException e) {
         _log.error(e.getMessage(), e);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
       }
       catch (Exception e) {
         _log.error(e.getMessage(), e);
         return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
       }
       
       // Make sure the job is in the same tenant as the requester. Note that this restriction
       // applies even to services, which should not be sending user events anyway.
       if (!job.getTenant().equals(threadContext.getJwtTenantId())) {
         String msg = JobUtils.getMsg("JOBS_MISMATCHED_TENANT", threadContext.getJwtTenantId(), job.getTenant());
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       // Don't send events to terminated jobs. The job could terminate in the window between 
       // performing this check and sending the event.  This is not a big deal since we are
       // only adding an event to the job history and, possibly, sending notifications.
       if (job.getStatus().isTerminal()) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_IN_TERM_STATE", rUser, jobUuid, job.getStatus().name());
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // ------------------------- Send Event -------------------------------
       final var eventName = JobEventType.JOB_USER_EVENT.name();
       JobEvent event = null;
       try {
           // Create and send a user event to the job.
           event = JobEventManager.getInstance().recordUserEvent(jobUuid, threadContext.getJwtTenantId(), 
                         threadContext.getJwtUser(), payload.getEventData(), payload.getEventDetail(), null);
       }
       catch (Exception e) {
         String msg = JobUtils.getMsgAuth("JOBS_CREATE_EVENT_ERR1", rUser, jobUuid, eventName, e.getMessage());
         _log.error(msg, e);
         return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // Success.
       RespBasic r = new RespBasic(event.getDescription());
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_CREATED", "event", eventName), r)).build();
     }
     
     /* **************************************************************************** */
     /*                               Private Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* doSubmit:                                                                    */
     /* ---------------------------------------------------------------------------- */
     /** Dump the payload from the input stream into a string and then call the 
      * real doSubmit method.
      * 
      * @param payloadStream the request's payload
      * @return the response to the user
      */
     private Response doSubmit(ResourceRequestUser rUser, InputStream payloadStream)
     {
         // ------------------------- Validate Payload -------------------------
         // Read the payload into a string.
         String json;
         try {json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8);}
         catch (Exception e) {
           String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job submission", e.getMessage());
           _log.error(msg, e);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
         }
         // The real submit.
         return doSubmit(rUser, json);
     }
     
     /* ---------------------------------------------------------------------------- */
     /* doSubmit:                                                                    */
     /* ---------------------------------------------------------------------------- */
     /** All the work gets done here from both submit and resubmit.
      * 
      * @param json the request's payload as json
      * @return the response to the user
      */
     private Response doSubmit(ResourceRequestUser rUser, String json)
     {
         String msg;
         // Log the incoming json
         if (_log.isDebugEnabled()) _log.debug(JobsApiUtils.getMsgAuth("JOBSAPI_SUBMIT_JSON", rUser, json));

         // ------------------------- Input Processing -------------------------
         // Parse and validate the json in the request payload, which must exist.
         ReqSubmitJob payload = null;
         try {payload = getPayload(json, FILE_JOB_SUBMIT_REQUEST, ReqSubmitJob.class);} 
         catch (Exception e) {
             msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR", "submitJob", e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
         }

         // Extract Notes from the raw json. Notes require special handling. Else they end up as a LinkedTreeMap which
         // causes trouble when attempting to convert to a JsonObject.
         Object notes = extractNotes(json);
         payload.setNotes(notes);

         // Create the request context object.
         var reqCtx = new SubmitContext(payload);
         
         // ------------------------- Initialize the Job -----------------------
         // Initialize job with calculated effective parameters.
         Job job = null;
         try {job = reqCtx.initNewJob();}
         catch (TapisImplException e) {
             _log.error(e.getMessage());
             return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
         }
         catch (Exception e) {
             // This should never happen, but we defend against it. 
             _log.error(e.getMessage(), e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).
                     entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
         }
         
         // ------------------- Create User Subscriptions ----------------------
         // Subscribe to Notifications service on behalf of user.  The complete list
         // of subscriptions are guaranteed by context initialization to have been
         // calculated and non-null by this point. Subscriptions are created before
         // we make any database changes so the caller can access any events generated.
         var response = createSubscriptions(rUser, reqCtx, job);
         if (response != null) return response;
         
         // ------------------------- Save Job ---------------------------------
         Job dbJob;
         // Write the job to the database.
         try {
             var jobsDao = new JobsDao();
             dbJob = jobsDao.createJob(rUser, job);
         }
         catch (Exception e) {
           _log.error(e.getMessage(), e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
         }

         // Log info for created job.
         msg = JobsApiUtils.getMsgAuth("JOBSAPI_JOB_CREATED", rUser, dbJob.getUuid(), dbJob.getOwner(), dbJob.getName(),
                                       dbJob.getStatus());
         _log.info(msg);
         // Save and sent any initial subscription events.
         createSubscriptionEvents(rUser, reqCtx, job);
       
         // -------------------------- Queue Request ---------------------------
         // Submit the job to the worker queue. Exceptions are mapped to HTTP error codes.
         try {JobQueueManager.getInstance().queueJob(job);}
         catch (Exception e) {
           // Log the error.
           msg = JobsApiUtils.getMsgAuth("JOBSAPI_SUBMIT_ERROR1", rUser, job.getUuid(), job.getAppId(), e.getMessage());
           _log.error(msg, e);

           // Fail the job.
           failJob(rUser, job, msg);

           // Let the user know the job failed.
           return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
         }

         // Log that message sent to MessageBroker
         _log.debug(JobsApiUtils.getMsgAuth("JOBSAPI_POST_MBQ", rUser, job.getUuid()));

         // ------------------------- Save Resubmit Info -----------------------
         // Save the valid job json definition for resubmission in the future
         // table is indexed on id & uuid.  If the actual job submission below
         // fails after this database insertion succeeds, we will have a resubmit
         // record that can never be referenced--no big deal.
         try {
             // Create the resubmit object.
             JobResubmit jobResubmit = new JobResubmit();
             jobResubmit.setJobUuid(job.getUuid());
             jobResubmit.setJobDefinition(json);
             
             // persist job definition json to resubmit table
             var jobResubmitDao = new JobResubmitDao();
             jobResubmitDao.createJobResubmit(jobResubmit);
         } catch (Exception e) {
             // Log the error.
             msg = JobsApiUtils.getMsgAuth("JOBSAPI_RESUBMIT_FAILED_PERSIST", rUser, job.getUuid(), e.getMessage());
             _log.error(msg);
         }
         // Log success of persisting resubmit info
         msg = JobsApiUtils.getMsgAuth("JOBSAPI_RESUBMIT_PERSISTED", rUser, dbJob.getUuid(), dbJob.getOwner(),
                                       dbJob.getName(), dbJob.getStatus());
         _log.debug(msg);

         // Trace log resolved job details for the job.
         if (_log.isTraceEnabled())
         {
             String dbJobStr = TapisGsonUtils.getGson(true).toJson(dbJob, Job.class);
             msg = JobsApiUtils.getMsgAuth("JOBSAPI_JOB_DETAILS", rUser, dbJob.getUuid(), dbJob.getOwner(),
                                           dbJob.getName(), dbJob.getStatus(), dbJobStr);
             _log.trace(msg);
         }

         // Success.
         RespSubmitJob r = new RespSubmitJob(job);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 JobUtils.getMsg("JOBS_CREATED", job.getUuid()), r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* createSubscriptions:                                                         */
     /* ---------------------------------------------------------------------------- */
     /** Post subscription create messages to Notifications.  Return null on success,
      * an error Response object when a subscription could not be created.
      * 
      * @param reqCtx submit request context
      * @param job the populated job object
      * @return null if ok, a response object on error
      */
     private Response createSubscriptions(ResourceRequestUser rUser, SubmitContext reqCtx, Job job)
     {
         List<ReqSubscribe> subscriptions = reqCtx.getSubmitReq().getSubscriptions();
         // Does the job have any subscriptions?
         if (subscriptions.isEmpty()) return null;
         
         // We assume the subscription requests are validated, so any failure create
         // a subscription in Notifications is a system problem that aborts the job.
         for (var req : subscriptions) {
           String url = null;
           try {url = JobsApiUtils.postSubscriptionRequest(req, job.getOwner(), job.getTenant(), job.getUuid());}
           catch (Exception e) {
             String msg = JobsApiUtils.getMsgAuth("JOBSAPI_SUBSCRIPTION_ERROR", rUser, job.getUuid(), job.getOwner(), e.getMessage());
             _log.error(msg, e);
             return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg)).build();
           }
                 
           // Log subscriptions created.
           if (_log.isDebugEnabled()) {
             var typeFilter = JobsApiUtils.getNotifTypeFilter(req.getEventCategoryFilter(), JobsApiUtils.TYPE_FILTER_WILDCARD);
             var msg = MsgUtils.getMsg("NOTIFICATIONS_SUBSCRIPTION_CREATED", job.getUuid(), typeFilter);
             _log.debug(msg);
           }
         }
         _log.debug(JobsApiUtils.getMsgAuth("JOBSAPI_SUBSCR_CREATED", rUser, job.getUuid(), subscriptions.size()));

         // Success.
         return null;
     }
     
     /* ---------------------------------------------------------------------------- */
     /* createSubscriptionEvents:                                                    */
     /* ---------------------------------------------------------------------------- */
     /** Record and post events for the subscriptions that are part of the job
      * submission, if any.  This is a best-effort calculation that never throws an
      * exception.
      * 
      * @param reqCtx submit request context
      * @param job the populated job object
      */
     private void createSubscriptionEvents(ResourceRequestUser rUser, SubmitContext reqCtx, Job job)
     {
         // Does the job have any subscriptions?
         int count = reqCtx.getSubmitReq().getSubscriptions().size();
         if (count < 1) return;
         
         // Record the event in the database and send notifications to the 
         // just established subscribers.
         try { JobEventManager.getInstance().recordJobSubmitSubscriptionsEvent(job, count); }
         catch (Exception e) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_SUBSCRIPTION_ERROR", rUser, job.getUuid(), job.getOwner(), e.getMessage());
           _log.error(msg, e);
         }
     }
     
     /* ---------------------------------------------------------------------------- */
     /* failJob:                                                                     */
     /* ---------------------------------------------------------------------------- */
     /** Mark the job as failed in the database.
      * 
      * @param job the failed job
      * @param failMsg the failure message
     */
     private static void failJob(ResourceRequestUser rUser, Job job, String failMsg)
     {
         // Fail the job.  Note that current status used in the transition 
         // to FAILED is the status of the job as defined in the db.
         try {
        	 // Always set the job condition before calling any dao method.
        	 job.setCondition(JobConditionCode.JOB_INTERNAL_ERROR);
             var jobsDao = new JobsDao();
             jobsDao.failJob("submitJob", job, failMsg);
         }
         catch (Exception e) {
             // Swallow exception and attempt to send an email.
             String msg = JobsApiUtils.getMsgAuth("JOBSAPI_ZOMBIE_ERROR", rUser, job.getUuid());
             _log.error(msg, e);
             sendZombieEmail(job, msg);
         }
     }
       
     /* ---------------------------------------------------------------------------- */
     /* sendZombieEmail:                                                             */
     /* ---------------------------------------------------------------------------- */
     /** Send an email to alert support that a zombie job exists.
      * 
      * @param job the job whose status update failed
      * @param zombiMsg failure message
      */
     private static void sendZombieEmail(Job job, String zombiMsg)
     {
         String subject = "Zombie Job Alert: " + job.getUuid() + " is in a zombie state.";
         try {
               RuntimeParameters runtime = RuntimeParameters.getInstance();
               EmailClient client = EmailClientFactory.getClient(runtime);
               client.send(runtime.getSupportName(),
                       runtime.getSupportEmail(),
                       subject,
                       zombiMsg, HTMLizer.htmlize(zombiMsg));
         }
         catch (Exception e1) {
               // log msg that we tried to send email notice to support.
               RuntimeParameters runtime = RuntimeParameters.getInstance();
               String recipient = runtime == null ? "unknown" : runtime.getSupportEmail();
               String msg = MsgUtils.getMsg("TAPIS_SUPPORT_EMAIL_ERROR", recipient, subject, e1.getMessage());
               _log.error(msg, e1);
         }
     }
  /*
   * Extract notes from the incoming json.
   * Return null if no notes provided by incoming request
   * This explicit method to extract is needed because notes is an unstructured object and other seemingly simpler
   * approaches caused problems with the json marshalling. This method ensures notes end up as a JsonObject rather
   * than a LinkedTreeMap.
   */
  private static JsonObject extractNotes(String rawJson)
  {
    // Check inputs
    if (StringUtils.isBlank(rawJson)) return null;
    // Turn the request string into a json object and extract the notes object
    JsonObject topObj = TapisGsonUtils.getGson().fromJson(rawJson, JsonObject.class);
    if (!topObj.has(NOTES_FIELD)) return null;
    return topObj.getAsJsonObject(NOTES_FIELD);
  }
}
