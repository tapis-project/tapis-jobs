package edu.utexas.tacc.tapis.jobs.api.resources;

import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.jobs.api.responses.RespGetJobOutputList;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespName;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

@Path("/")
public class JobOutputListingResource extends AbstractResource{
	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobOutputListingResource.class);

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
     /* getJobOutputListing:                                                         */
     /* ---------------------------------------------------------------------------- */
    
     @GET
     @Path("/{jobUuid}/output/list/{outputPath: (.*+)}")
     @Produces(MediaType.APPLICATION_JSON)
     public Response getJobOutputList(@PathParam("jobUuid") String jobUuid,@DefaultValue("")@PathParam("outputPath") String outputPath,
    		 						  @QueryParam("limit") int limit,	@QueryParam("skip") int skip,
    		 						  @DefaultValue("false") @QueryParam("allowIfRunning") boolean allowIfRunning)
                               
     {
         String opName = "getJobOutputList";
         // ------------------------- Retrieve and validate thread context -------------------------
         Response resp = JobsApiUtils.checkContext(TapisThreadLocal.tapisThreadContext.get());
         if (resp != null) return resp;

         // Create a user that collects together tenant, user and request information needed by the service call
         ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) _securityContext.getUserPrincipal());
         // Trace this request.
         if (_log.isTraceEnabled())
             JobsApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "jobUuid="+jobUuid,
                     "outputPath="+outputPath,"limit="+limit,"skip="+skip,"allowIfRunning="+allowIfRunning);

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

       // ------------------------- Retrieve Job -----------------------------
       Job job = null;
       var jobsImpl = JobsImpl.getInstance();
       
       try {
           job = jobsImpl.getJobByUuid(jobUuid, threadContext.getOboUser(), threadContext.getOboTenantId(),
        		   JobResourceShare.JOB_OUTPUT.name(), JobTapisPermission.READ.name());
       } catch (TapisImplException e) {
           _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
       } catch (Exception e) {
           _log.error(e.getMessage(), e);
           return Response.status(Status.INTERNAL_SERVER_ERROR).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
       }
       
       if (job == null) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_JOB_NOT_FOUND", rUser, jobUuid, threadContext.getOboTenantId());
           _log.warn(msg);
           ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job", jobUuid), r)).build();
           
       } else if(!job.isVisible()) {
           String msg = JobsApiUtils.getMsgAuth("JOBSAPI_JOB_NOT_VISIBLE", rUser, jobUuid);
           _log.warn(msg);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg)).build();
       }
        
       // ------------------------- Check the Job's status -----------------------------
       // If job is still running and not in terminal state then output listing cannot be performed.
       
       if(!job.getStatus().isTerminal() && allowIfRunning == false) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(JobUtils.getMsg("JOBS_JOB_NOT_TERMINATED",
    			   jobUuid,threadContext.getOboTenantId(),threadContext.getOboUser(),job.getStatus()),r)).build();
       }
       
       // Set default parameters
       SearchParameters srchParms = threadContext.getSearchParameters();
       boolean recursiveFlag = true;
       /* If limit is not specified and (either skip=0 is provided or skip is not specified), 
        * then recursive listing of output files is returned by default.
        * If limit is not specified and skip is specified and set to non-zero value, non-recursive 
        * output files listing is returned
        * If limit is specified, again non-recursive output files listing is returned
        */
       if(srchParms.getLimit() == null) {
           srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);
           if (srchParms.getSkip() != SearchParameters.DEFAULT_SKIP) {
   	          recursiveFlag = false; 
   	       }
       }
       else recursiveFlag = false;
             
       List<FileInfo> filesList = null;
       
       try {
		filesList = jobsImpl.getJobOutputList(job, threadContext.getOboTenantId(), threadContext.getOboUser(), outputPath, 
				srchParms.getLimit(),skip, JobResourceShare.JOB_OUTPUT.name(), JobTapisPermission.READ.name(),recursiveFlag);
	   } catch (TapisImplException e) {
		   _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
	   }
       if(filesList == null) {
    	   ResultName missingName = new ResultName();
           missingName.name = jobUuid;
           RespName r = new RespName(missingName);
           return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createSuccessResponse(
               MsgUtils.getMsg("TAPIS_NOT_FOUND", "Job Output Files List", jobUuid), r)).build();
       }
       
       // ------------------------- Process Results --------------------------
      
       
       // Success.
       RespGetJobOutputList r = new RespGetJobOutputList(filesList,srchParms.getLimit(),srchParms.getSkip());
       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
               JobUtils.getMsg("JOBS_OUTPUT_FILES_LIST_RETRIEVED", jobUuid, threadContext.getOboUser(),
            		   threadContext.getOboTenantId()), r)).build();
     }
     
     
}



