package edu.utexas.tacc.tapis.jobs.api.resources;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.TreeSet;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqJobAnnotation;
import edu.utexas.tacc.tapis.jobs.api.responses.RespHideJob;
import edu.utexas.tacc.tapis.jobs.api.responses.RespJobAnnotationUpdate;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.JobAnnotation;
import edu.utexas.tacc.tapis.jobs.model.dto.JobHideDisplay;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

@Path("/")
public class JobActionResource extends AbstractResource {

	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobActionResource.class);
    private static final String FILE_JOB_ANNOTATION_REQUEST =
        "/edu/utexas/tacc/tapis/jobs/api/jsonschema/JobAnnotationRequest.json";
    
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
     private Response updateJobAnnotations(String jobUuid, InputStream payloadStream,
         boolean replace) {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "updateJobAnnotations",
             "  " + _request.getRequestURL());
         _log.trace(msg);
       }

       // ------------------------- Input Processing -------------------------
       if (StringUtils.isBlank(jobUuid)) {
         String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "jobUuid");
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       // ------------------------- Validate Payload -------------------------
       // Read the payload into a string.
       String json = null;
       try {
         json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8);
       } catch (Exception e) {
         String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job submission", e.getMessage());
         _log.error(msg, e);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       // Parse and validate the json in the request payload, which must exist.
       ReqJobAnnotation payload = null;
       try {
         payload = getPayload(json, FILE_JOB_ANNOTATION_REQUEST, ReqJobAnnotation.class);
       } catch (Exception e) {
         String msg = MsgUtils.getMsg("NET_REQUEST_PAYLOAD_ERROR",
             "submitJob", e.getMessage());
         _log.error(msg, e);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       // ------------------------- Create Context ---------------------------
       // Validate the threadlocal content here so no subsequent code on this request
       // needs to.
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       if (!threadContext.validate()) {
         var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
         _log.error(msg);
         return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

       // ------------------- Job Status Check -----------------------------
       // There is no need for job status check for now.

       // ------------------------- Update Annotations -------------------------
       JobAnnotation jobAnnotation = null;
       var jobsImpl = JobsImpl.getInstance();
       String user = threadContext.getOboUser();
       String tenant = threadContext.getOboTenantId();
       var tags = payload.getTags() == null ? null : new TreeSet<String>(payload.getTags());
       var notes = payload.getNotes();
       try {
         jobAnnotation = jobsImpl.doUpdateAnnotation(jobUuid, tenant, user, tags, notes, replace);
         if (jobAnnotation == null) {
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), r)).build();
         }
       } 
       catch (TapisImplException e) {
         String msg = e.getMessage();
         _log.error(msg, e);
         return Response.status(e.condition.getHttpStatus()).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       catch (Exception e) {
        String msg = e.getMessage();
         _log.error(msg, e);
         return Response.status(Status.INTERNAL_SERVER_ERROR)
             .entity(TapisRestUtils.createErrorResponse(msg)).build();
       } catch (Throwable e) {
         String msg = JobUtils.getMsg("JOBS_JOB_ANNOTATION_UPDATE_ERROR", replace ? "PUT" : "PATCH", jobUuid,
             tenant, user, tags, notes, e);
         _log.error(msg, e);
         return Response.status(Status.INTERNAL_SERVER_ERROR)
             .entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       // ---------------------------- Success -------------------------------
       // Success.
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
           JobUtils.getMsg("JOBS_JOB_ANNOTATION_UPDATED", replace ? "UPDATED" : "PATCHED", jobUuid, user, tenant),
           new RespJobAnnotationUpdate(jobAnnotation))).build();
     }

     @PUT
     @Path("/{jobUuid}/annotations")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     public Response putJobAnnotations(@PathParam("jobUuid") String jobUuid, InputStream payloadStream)
     {
       return updateJobAnnotations(jobUuid, payloadStream, true);
     }

     @PATCH
     @Path("/{jobUuid}/annotations")
     @Consumes(MediaType.APPLICATION_JSON)
     @Produces(MediaType.APPLICATION_JSON)
     public Response patchJobAnnotations(@PathParam("jobUuid") String jobUuid, InputStream payloadStream) {
       return updateJobAnnotations(jobUuid, payloadStream, false);
     }
     

     /* ---------------------------------------------------------------------------- */
     /* hideJob:                                                                     */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/hide")
     @Produces(MediaType.APPLICATION_JSON)
     public Response hideJob(@PathParam("jobUuid") String jobUuid)
                               
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "hideJob", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }
       
       // ------------------------- Input Processing -------------------------
       if (StringUtils.isBlank(jobUuid)) {
           String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "jobUuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).
                      entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // ------------------------- Create Context ---------------------------
       // Validate the threadlocal content here so no subsequent code on this request needs to.
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       if (!threadContext.validate()) {
           var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
           _log.error(msg);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

    // ------------------------- Retrieve Job Status-----------------------------
       JobStatusDTO jobstatus = null;
       var jobsImpl = JobsImpl.getInstance();
       try {
           
           jobstatus = jobsImpl.getJobStatusByUuid(jobUuid, threadContext.getOboUser(),
                                       threadContext.getOboTenantId());
       }
       catch (TapisImplException e) {
           _log.error(e.getMessage(), e);
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
       if (jobstatus == null) {
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), r)).build();
       
       }
       // ------------------------- Check the Job's status -----------------------------
       // If job is not in terminal state then hiding job cannot be performed.
       
       if(!jobstatus.getStatus().isTerminal()) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           String msg = JobUtils.getMsg("JOBS_JOB_NOT_IN_TERMINAL_STATE", jobUuid);
           _log.warn(msg);
    	   return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(
                   msg, r)).build();
       }
       // Don't change visibility if already set to hidden
       if(!jobstatus.getVisible()) {
    	   JobHideDisplay hideMsg = new JobHideDisplay();
    	   String msg = JobUtils.getMsg("JOBS_JOB_VISBILITY", jobUuid, "hidden");
    	   hideMsg.setMessage(msg);
    	   RespHideJob r = new RespHideJob(hideMsg);
       	   
    	   return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
    			   JobUtils.getMsg("JOBS_JOB_VISBILITY", jobUuid, "hidden"),r)).build();
       }
       
       //------------------------- Change the visibility  -----------------------------
       // set visible to false
      
		if (!jobsImpl.doHideJob(jobUuid, threadContext.getOboTenantId(), threadContext.getOboUser() ))
    {
      return Response.status(Status.INTERNAL_SERVER_ERROR).
        entity(TapisRestUtils.createErrorResponse(JobUtils.getMsg("JOBS_JOB_UNCHANGED_VISIBILITY", jobUuid, "unhidden"))).build();
    }
     
       
       // ---------------------------- Success -------------------------------
       // Success.
       JobHideDisplay hideMsg = new JobHideDisplay();
       String msg = JobUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "hidden");
       hideMsg.setMessage(msg);
       
       RespHideJob r = new RespHideJob(hideMsg); 
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               JobUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "hidden"),r)).build();
     }
     
     /* ---------------------------------------------------------------------------- */
     /* unhideJob:                                                                   */
     /* ---------------------------------------------------------------------------- */
     @POST
     @Path("/{jobUuid}/unhide")
     @Produces(MediaType.APPLICATION_JSON)
     public Response unhideJob(@PathParam("jobUuid") String jobUuid)
                               
     {
       // Trace this request.
       if (_log.isTraceEnabled()) {
         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "unhideJob", 
                                      "  " + _request.getRequestURL());
         _log.trace(msg);
       }

       JobsApiUtils.checkRestrictedSvcs(_securityContext);

       // ------------------------- Input Processing -------------------------
       if (StringUtils.isBlank(jobUuid)) {
           String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "jobUuid");
           _log.error(msg);
           return Response.status(Status.BAD_REQUEST).
                      entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
       
       // ------------------------- Create Context ---------------------------
       // Validate the threadlocal content here so no subsequent code on this request needs to.
       TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
       if (!threadContext.validate()) {
           var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
           _log.error(msg);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(msg)).build();
       }

       // ------------------------- Retrieve Job Status-----------------------------
       JobStatusDTO jobstatus = null;
       var jobsImpl = JobsImpl.getInstance();
       try {
           
           jobstatus = jobsImpl.getJobStatusByUuid(jobUuid, threadContext.getOboUser(),
                                       threadContext.getOboTenantId());
       }
       catch (TapisImplException e) {
           _log.error(e.getMessage(), e);
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
       if (jobstatus == null) {
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), r)).build();
       
       }
       // ------------------------- Check the Job's status -----------------------------
       // If job is not in terminal state then hiding job cannot be performed.
       
       if(!jobstatus.getStatus().isTerminal()) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           String msg = JobUtils.getMsg("JOBS_JOB_NOT_IN_TERMINAL_STATE", jobUuid);
           _log.warn(msg);
    	   return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(
                   msg, r)).build();
       }
       // Don't change visibility if already set to unhidden
       if(jobstatus.getVisible()) {
    	   
    	   JobHideDisplay hideMsg = new JobHideDisplay();
    	   String msg = JobUtils.getMsg("JOBS_JOB_VISBILITY", jobUuid, "unhidden");
    	   hideMsg.setMessage(msg);
    	   RespHideJob r = new RespHideJob(hideMsg);
       	   
    	   return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
    			   JobUtils.getMsg("JOBS_JOB_VISBILITY", jobUuid, "unhidden"),r)).build();
    	   
       }
       
       //------------------------- Change the visibility  -----------------------------
       // set visible to true
      
		if (!jobsImpl.doUnHideJob(jobUuid, threadContext.getOboTenantId(), threadContext.getOboUser() ))
    {
      return Response.status(Status.INTERNAL_SERVER_ERROR).
         entity(TapisRestUtils.createErrorResponse(JobUtils.getMsg("JOBS_JOB_UNCHANGED_VISIBILITY", jobUuid, "unhidden"))).build();
    }
     
       
       // ---------------------------- Success -------------------------------
       // Success.
       JobHideDisplay hideMsg = new JobHideDisplay();
       String msg = JobUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "unhidden");
       hideMsg.setMessage(msg);
       
       RespHideJob r = new RespHideJob(hideMsg); 
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               JobUtils.getMsg("JOBS_JOB_CHANGED_VISIBILITY", jobUuid, "unhidden"),r)).build();
     }

}
