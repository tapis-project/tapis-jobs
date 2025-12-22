package edu.utexas.tacc.tapis.jobs.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.ApplicationPath;

import edu.utexas.tacc.tapis.jobs.api.resources.*;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.ClearThreadLocalRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.ClearThreadLocalResponseFilter;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.QueryParametersRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.providers.ApiExceptionMapper;
import edu.utexas.tacc.tapis.sharedapi.providers.ObjectMapperContextResolver;
import edu.utexas.tacc.tapis.sharedapi.providers.ValidationExceptionMapper;
import edu.utexas.tacc.tapis.sharedapi.servlet.filters.TapisLoggingFilter;
import org.flywaydb.core.Flyway;
import org.glassfish.jersey.server.ResourceConfig;
import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.events.NotificationLiveness;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse). 
@ApplicationPath("/jobs")
public class JobsApplication 
extends ResourceConfig
{
   // List of Tapis services allowed to call this service with a service JWT.
   // No services are allowed.
   public static final Set<String> SVCLIST_TRUSTED = new HashSet<>(Set.of());
   // The table we query to test database connectivity.
   private static final String QUERY_TABLE = "jobs";
   
   public JobsApplication()
   {
       // ------------------ Unrecoverable Errors ------------------
       // Log our existence.
       System.out.println("**** Starting tapis-jobsapi ****");

       // TODO/TBD instead, register specific classes.
       // TODO remove
//       // We specify what packages JAX-RS should recursively scan
//       // to find annotations.  By setting the value to the top-level
//       // tapis directory in all projects, we can use JAX-RS annotations
//       // in any tapis class.  In particular, the filter classes in
//       // tapis-sharedapi will be discovered whenever that project is
//       // included as a maven dependency.
//       packages("edu.utexas.tacc.tapis");

//       packages("edu.utexas.tacc.tapis.jobs.api.resources");

       // Needed for properly returning timestamps
       // Also allows for setting a breakpoint when response is being constructed.
       register(ObjectMapperContextResolver.class);

       // Register classes needed for returning a standard Tapis response for non-Tapis exceptions.
       register(ApiExceptionMapper.class);
       register(ValidationExceptionMapper.class);

       // jax-rs filters
       // NOTE: We deliberately exclude TapisLoggingFilter. In the future, it would be good to enable optional
       // logging of the servlet request and response by updating TapisLoggingFilter to check an env var.
       // register(TapisLoggingFilter.class);
       register(ClearThreadLocalRequestFilter.class);
       register(ClearThreadLocalResponseFilter.class);
       register(JWTValidateRequestFilter.class);
       register(QueryParametersRequestFilter.class);

       //Our APIs
       register(GeneralResource.class);
       register(JobActionResource.class);
       register(JobCancelResource.class);
       register(JobGetResource.class);
       register(JobHistoryResource.class);
       register(JobListingResource.class);
       register(JobOutputDownloadResource.class);
       register(JobOutputListingResource.class);
       register(JobSearchResource.class);
       register(JobShareResource.class);
       register(JobStatusResource.class);
       register(JobSubmitResource.class);
       register(JobSubscriptionResource.class);

       setApplicationName(TapisConstants.SERVICE_NAME_JOBS);
       
       // Initialize our parameters.  A failure here is unrecoverable.
       RuntimeParameters parms = null;
       try {parms = RuntimeParameters.getInstance();}
           catch (Exception e) {
               // We don't depend on the logging subsystem.
               System.out.println("**** FAILURE TO INITIALIZE: tapis-jobsapi RuntimeParameters [ABORTING] ****");
               e.printStackTrace();
               System.exit(1);
           }
       System.out.println("**** SUCCESS:  RuntimeParameters read ****");
       
       // Initialize local error list.
       var errors = new ArrayList<String>(); // cumulative error count
       
       // Enable more detailed SSH logging if the node name is not null.
       SSHConnection.setLocalNodeName(parms.getLocalNodeName());
       
       // ---------------- Initialize Security Filter --------------
       // Required to process any requests.
       JWTValidateRequestFilter.setService(TapisConstants.SERVICE_NAME_JOBS);
       JWTValidateRequestFilter.setSiteId(parms.getSiteId());
       
       // ------------------- Recoverable Errors -------------------
       // ----- Tenant Map Initialization
       // Force runtime initialization of the tenant manager.  This creates the
       // singleton instance of the TenantManager that can then be accessed by
       // all subsequent application code--including filters--without reference
       // to the tenant service base url parameter.
       Map<String,Tenant> tenantMap = null;
       try {
           // The base url of the tenants service is a required input parameter.
           // We actually retrieve the tenant list from the tenant service now
           // to fail fast if we can't access the list.
           String url = parms.getTenantBaseUrl();
           tenantMap = TenantManager.getInstance(url).getTenants();
       } catch (Exception e) {
           // We don't depend on the logging subsystem.
           errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi TenantManager ****\n" + e.getMessage());
           e.printStackTrace();
       }
       if (tenantMap != null) {
           System.out.println("**** SUCCESS:  " + tenantMap.size() + " tenants retrieved ****");
           String s = "Tenants:\n";
           for (String tenant : tenantMap.keySet()) s += "  " + tenant + "\n";
           System.out.println(s);
       } else 
    	   System.out.println("**** FAILURE TO INITIALIZE: tapis-jobsapi TenantManager - No Tenants ****");
       
       // ----- Service JWT Initialization
       ServiceContext serviceCxt = ServiceContext.getInstance();
       try {
                serviceCxt.initServiceJWT(parms.getSiteId(), TapisConstants.SERVICE_NAME_JOBS, 
    	    	                          parms.getServicePassword());
    	}
       	catch (Exception e) {
            errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi ServiceContext ****\n" + e.getMessage());
            e.printStackTrace();
       	}
       if (serviceCxt.getServiceJWT() != null) {
    	   var targetSites = serviceCxt.getServiceJWT().getTargetSites();
    	   int targetSiteCnt = targetSites != null ? targetSites.size() : 0;
    	   System.out.println("**** SUCCESS:  " + targetSiteCnt + " target sites retrieved ****");
    	   if (targetSites != null) {
    		   String s = "Target sites:\n";
    		   for (String site : targetSites) s += "  " + site + "\n";
    		   System.out.println(s);
    	   }
       }
       
     // ----- Database Initialization
     // Use flyway to update the DB schema
     try { migrateDB(); }
     catch (Exception e) {
       errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi MigrateDB ****\n" + e.getMessage());
       e.printStackTrace();
     }

     // Check DB
     try {JobsImpl.getInstance().ensureDefaultQueueIsDefined();}
	    catch (Exception e) {
            errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi Database ****\n" + e.getMessage());
	    	e.printStackTrace();
	    }
       
       // ------ Queue Initialization 
       // By getting the singleton instance of the queue manager
       // we also cause all job queues and exchanges to be initialized.
       // There is some redundancy here since each front-end and
       // each worker initializes all queue artifacts.  Not a problem, 
       // but there's room for improvement.
       try {initializeJobQueueManager();} // called for side effect
        catch (Exception e) {
            errors.add("**** FAILURE TO INITIALIZE: tapis-jobsapi JobQueueManager ****\n" + e.getMessage());
            e.printStackTrace();
        }

       // We're done.
       System.out.println("\n**********************************************");
       System.out.println("**** tapis-jobsapi Initialized [errors=" + errors.size() + "] ****");
       System.out.println("**********************************************\n");
       
       // This is an effective but somewhat crude way to abort.
       if (!errors.isEmpty()) {
           System.out.println("\n");
           for (var s : errors) System.out.println(s);
           System.exit(1);
       }
       
       // ----- Database Migration for release 1.3.1 onwards
       // This code will be removed or made optional after some time.
       if(parms.getJobsRunDBMigration()) {
    	   runMigration();
       } else {
    	   System.out.println("****  Not running DB migration ****. \n");
       }
       
       // ----- Start the notification liveness thread.
       startNotificationLiveness();  
   }
   
   /** Initialize rabbitmq vhost and our standard queues and exchanges.  VHost initialization
    * requires the overall administrator's credentials to create the vhost and its user if
    * they don't already exist.
    */
   private void initializeJobQueueManager()
   {
       // This can throw a runtime exception.
       JobQueueManager.getInstance(JobQueueManager.initParmsFromRuntime());
   }
   
   /** Attempt to migrate the jobs table data after the V014__UpdateSharedAppCtx.sql Flyway
    * migration script was run.  That script changes the shared_app_ctx column from type 
    * boolean to string, which change boolean values into their string representation,
    * but doesn't implement the shared context semantics.  Those semantic require consultation
    * with the Apps service, something most easily done in from a service runtime environment.
    * Normally, we'd have Flyway run the script, but the security setup and other configuration
    * make that burdensome.
    */
   private void runMigration() 
   {
       try {
    	   // Run the migration which will only have an effect the first time it runs
    	   // successfully against a database.  After the first time, it's a no-op.
    	   DBMigrationSharedAppCtx ctx = new DBMigrationSharedAppCtx(JWTValidateRequestFilter.getSiteId());
    	   ctx.migrate();
	   } catch (Exception e) {
		   // We ignore migration errors and simply print an error message. 
		   System.out.println("**** FAILURE TO RUN DB MIGRATION: jobs.sharedAppCtx not updated ****\n" + e.getMessage());
	   }
   }
   
   /** Start the notification liveness threads to continuously test whether events
    * get delivered to Notifications and that notifications are processed. 
    */
   private void startNotificationLiveness() 
   {
	   NotificationLiveness.getInstance();
   }

  /*
   * migrateDB
   * Use Flyway to make sure DB schema is at the latest version
   */
  private void migrateDB() throws TapisException
  {
    Flyway flyway = Flyway.configure().dataSource(JobsDao.getDataSource()).load();
    // Note: Can use repair() as workaround to avoid checksum error during develop/deploy of SNAPSHOT versions when it
    // is not a true migration.
//    flyway.repair();
    flyway.migrate();
  }
}
