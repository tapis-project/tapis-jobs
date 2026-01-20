package edu.utexas.tacc.tapis.jobs.api.resources;


import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
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
import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.responses.RespGetJobList;
import edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearch;
import edu.utexas.tacc.tapis.jobs.api.utils.JobListUtils;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.dto.JobListDTO;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobListType;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;

@Path("/list")
public class JobListingResource 
extends AbstractResource
{
	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(JobListingResource.class);
	private final String className = getClass().getSimpleName();
	private static final int DEFAULT_TOTAL_COUNT = -1;
	private final String UUID_ATTR = "uuid";
	private final String SEARCH_OPERATOR = "IN";
	private final boolean SHARED = true;

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
	/* getJobList:                                                                  */
	/* ---------------------------------------------------------------------------- */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJobList(
			@QueryParam("limit") int limit, 
			@QueryParam("skip") int skip,
			@QueryParam("startAfter") int startAfter,
			@QueryParam("orderBy") String orderBy,
			@QueryParam("computeTotal")  boolean computeTotal,
			@DefaultValue("MY_JOBS") @QueryParam("listType") String listType)

	{
      String opName = "getJobList";
      // ------------------------- Retrieve and validate thread context -------------------------
      Response resp = JobsApiUtils.checkContext(TapisThreadLocal.tapisThreadContext.get());
      if (resp != null) return resp;

      // Create a user that collects together tenant, user and request information needed by the service call
      ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) _securityContext.getUserPrincipal());
      // Trace this request.
      if (_log.isTraceEnabled())
        JobsApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
                                "limit="+listType,"skip="+skip,"startAfter="+startAfter,"orderBy="+orderBy,
                                "computTotal="+computeTotal,"listType="+listType);

      JobsApiUtils.checkRestrictedSvcs(_securityContext);

		// ------------------------- Create Context ---------------------------
		TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
		// ------ Set default values for the reserved query parameters ------------
		// orderBy is of the format - fname1(desc), fname2, fname3(asc), ...
		// ThreadContext designed to never return null for SearchParameters
		SearchParameters srchParms = threadContext.getSearchParameters();

		if(srchParms.getLimit() == null) {srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);}
		int totalCount = DEFAULT_TOTAL_COUNT;

		computeTotal = srchParms.getComputeTotal(); 

		// Get the listType. Default Type is  MY_JOBS  
		// Validate the listType query parameter
		if(!EnumUtils.isValidEnum(JobListType.class, listType)) {
			String msg = JobsApiUtils.getMsgAuth("JOBSAPI_LISTTYPE_INVALID", rUser, listType);
			_log.error(msg);
			return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
		}

		boolean sharedWithMe = false;
		if(listType.equals(JobListType.SHARED_JOBS.name()) || listType.equals(JobListType.ALL_JOBS.name() )){
			sharedWithMe = true;
		}

		List<String> searchList = new ArrayList<String>();
		List<String> sharedSearchList = new ArrayList<String>();

		// summary attributes
		List<JobListDTO> jobSharedSummaryList = new ArrayList<JobListDTO>();

		// Get UUIDs of all jobs shared with the user
		// Note that if sharedWithMe is false, then sharedJobUuidsList will be empty

		List<String> sharedJobUuidsList = new ArrayList<String>();
		try {
			sharedJobUuidsList = JobListUtils.getSharedJobUuids(sharedWithMe,threadContext.getOboUser(), 
			                                                    threadContext.getOboTenantId() );
		} catch (TapisImplException e) {
			_log.error(e.getMessage());
			return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
					entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
		}
		String sharedCond = null;
		// Add the shared jobs UUIDs to the shared searchlist
		if(!sharedJobUuidsList.isEmpty()) {
			sharedCond = UUID_ATTR + "." + SEARCH_OPERATOR + "." + String.join(",",sharedJobUuidsList);
			sharedSearchList.addAll(searchList);
			sharedSearchList.add(sharedCond);
		} 


		// ----------   Compute Total Count --------------
		// If we need the total count and there was a limit then we need to make a call. 
		// Default limit is always greater than zero.
		int totalCountOwner = 0;
		int totalCountShared = 0;

		if (computeTotal){
			//totalCountOwner represents all the jobs that the user is owner, admin or creator of the job.
			if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
				try {
					totalCountOwner = JobListUtils.computeTotalCount(threadContext.getOboUser(), 
							threadContext.getOboTenantId(), searchList, srchParms.getOrderByList(), !SHARED);
				} catch (TapisImplException e) {
					_log.error(e.getMessage());
					return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
							entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
				}
			}
			//totalCountShared represents all the jobs that are shared with the user
			if(sharedWithMe) {
				try {
					totalCountShared = JobListUtils.computeTotalCount(threadContext.getOboUser(), 
							threadContext.getOboTenantId(), sharedSearchList, srchParms.getOrderByList(), SHARED);
				} catch (TapisImplException e) {
					_log.error(e.getMessage());
					return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
							entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
				}
			} 

			if(listType.equals(JobListType.ALL_JOBS.name())){
				totalCount = totalCountOwner +  totalCountShared;
			} else if (listType.equals(JobListType.MY_JOBS.name())){
				totalCount = totalCountOwner;
			} else {
				totalCount = totalCountShared;
			}
		}

		// ------------ Retrieve Job List -----------------------------
		List<JobListDTO> jobList = new ArrayList<JobListDTO>();
		var jobsImpl = JobsImpl.getInstance();
		if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
			try {
				jobList = jobsImpl.getJobSearchListByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), searchList,
						srchParms.getOrderByList(), srchParms.getLimit(),srchParms.getSkip(), !SHARED); // user's owned jobs

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
		}

		int diffLimit = 0;
		int diffSkip = 0;

		// compute limit and skip for shared list
		if(!jobList.isEmpty()) {
			diffLimit = srchParms.getLimit() - jobList.size();
			diffSkip = 0;
		} else {
			diffLimit = srchParms.getLimit();
			// When jobList is empty, then either all the records for jobs when user is the owner have been skipped 
  		    // or the list type is SHARED_JOBS
			try {
				diffSkip = JobListUtils.computeSkip(listType,threadContext.getOboUser(), 
						   threadContext.getOboTenantId(), searchList, srchParms.getOrderByList(), srchParms.getSkip(), !SHARED );
			  } catch (TapisImplException e) {
				  _log.error(e.getMessage());
			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
			                   entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
			  }
		}

		//------- Get the jobs shared with the user--------------------
		// Get the shared jobs
		// Note the sharedSearchList has shared jobs uuids
		if(sharedWithMe && !sharedJobUuidsList.isEmpty()) {
			try {
				jobSharedSummaryList = 
						jobsImpl.getJobSearchListByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), sharedSearchList,
								srchParms.getOrderByList(), diffLimit, diffSkip, SHARED);
			} catch (TapisImplException e) {
				_log.error(e.getMessage());
				return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
						entity(TapisRestUtils.createErrorResponse(e.getMessage())).build();
			} 
			if(!jobSharedSummaryList.isEmpty()) {
				jobList.addAll(jobSharedSummaryList);
			}
		}

		if(jobList.isEmpty()) {
			String msg =  JobUtils.getMsg("JOBS_SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
			RespJobSearch r = new RespJobSearch(jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),-1);
			return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg,r)).build();
		}

		if (computeTotal && srchParms.getLimit() <= 0 && srchParms.getSkip() == 0) totalCount = jobList.size();

		// ------------------------- Process Results --------------------------
		// Success.
		RespGetJobList r = new RespGetJobList(jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);

		return Response.status(Status.OK).entity(TapisRestUtils
				.createSuccessResponse(
						JobUtils.getMsg("JOBS_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), r)).build();
	}
}



