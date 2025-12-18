package edu.utexas.tacc.tapis.jobs.api.utils;

import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import org.apache.commons.lang3.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;

import edu.utexas.tacc.tapis.jobs.api.JobsApplication;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventCategoryFilter;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryMethod;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryTarget;
import edu.utexas.tacc.tapis.notifications.client.gen.model.ReqPostSubscription;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.utils.PathSanitizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobsApiUtils 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobsApiUtils.class);
    // Location of message bundle files
    private static final String MESSAGE_BUNDLE = "edu.utexas.tacc.tapis.jobs.api.JobsApiMessages";

    // The wildcard used in notifications subject filters.
    public static final String TYPE_FILTER_WILDCARD = "*";
    
    // Create a TypeToken to be used by gson for processing of LinkedTreeMap objects.
    private static final Type linkedTreeMapType = new TypeToken<LinkedTreeMap<Object,Object>>(){}.getType();

    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /**
     * Get a localized message using the specified key and parameters. Locale is null.
     * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
     * @param key - Key used to lookup message in properties file.
     * @param parms - Parameters for template variables in message
     * @return Resulting message
     */
    public static String getMsg(String key, Object... parms)
    {
        return getMsg(key, null, parms);
    }

    /**
     * Get a localized message using the specified locale, key and parameters.
     * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
     * @param locale - Locale to use when building message. If null use default locale
     * @param key - Key used to lookup message in properties file.
     * @param parms - Parameters for template variables in message
     * @return Resulting message
     */
    public static String getMsg(String key, Locale locale, Object... parms)
    {
        String msgValue = null;

        if (locale == null) locale = Locale.getDefault();

        ResourceBundle bundle = null;
        try { bundle = ResourceBundle.getBundle(MESSAGE_BUNDLE, locale); }
        catch (Exception e)
        {
            _log.error("Unable to find resource message bundle: " + MESSAGE_BUNDLE, e);
        }
        if (bundle != null) try { msgValue = bundle.getString(key); }
        catch (Exception e)
        {
            _log.error("Unable to find key: " + key + " in resource message bundle: " + MESSAGE_BUNDLE, e);
        }

        if (msgValue != null)
        {
            // No problems. If needed fill in any placeholders in the message.
            if (parms != null && parms.length > 0) msgValue = MessageFormat.format(msgValue, parms);
        }
        else
        {
            // There was a problem. Build a message with as much info as we can give.
            StringBuilder sb = new StringBuilder("Key: ").append(key).append(" not found in bundle: ").append(MESSAGE_BUNDLE);
            if (parms != null && parms.length > 0)
            {
                sb.append("Parameters:[");
                for (Object parm : parms) {sb.append(parm.toString()).append(",");}
                sb.append("]");
            }
            msgValue = sb.toString();
        }
        return msgValue;
    }

    /**
     * Get a localized message using the specified key and parameters. Locale is null.
     * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
     * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
     * @param key - Key used to lookup message in properties file.
     * @param parms - Parameters for template variables in message
     * @return Resulting message
     */
    public static String getMsgAuth(String key, ResourceRequestUser rUser, Object... parms)
    {
        // Construct new array of parms. This appears to be most straightforward approach to modify and pass on varargs.
        var newParms = new Object[4 + parms.length];
        newParms[0] = rUser.getJwtTenantId();
        newParms[1] = rUser.getJwtUserId();
        newParms[2] = rUser.getOboTenantId();
        newParms[3] = rUser.getOboUserId();
        System.arraycopy(parms, 0, newParms, 4, parms.length);
        return getMsg(key, newParms);
    }

    /**
     * Trace the incoming request, include info about requesting user, op name and request URL
     * @param rUser resource user
     * @param opName name of operation
     */
    public static void logRequest(ResourceRequestUser rUser, String className, String opName, String reqUrl, String... strParms)
    {
        // Build list of args passed in
        String argListStr = "";
        if (strParms != null && strParms.length > 0) argListStr = String.join(",", strParms);
        String msg = getMsgAuth("JOBSAPI_TRACE_REQUEST", rUser, className, opName, reqUrl, argListStr);
        _log.trace(msg);
    }

    /* ---------------------------------------------------------------------------- */
    /* toHttpStatus:                                                                */
    /* ---------------------------------------------------------------------------- */
    public static Status toHttpStatus(Condition condition)
    {
        // Conditions are expected to have the exact same names as statuses.
        try {return Status.valueOf(condition.name());}
        catch (Exception e) {return Status.INTERNAL_SERVER_ERROR;}     
    }
    
    /* ---------------------------------------------------------------------------- */
    /* constructTenantURL:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Construct a path from the base url of the specified tenant and path.  We 
     * prevent double slashes from appearing between each of the components (url, path 
     * and pathSuffix) that comprise the final string.  We also guarentee that a 
     * single slash separates each of the components.
     * 
     * Exceptions are never thrown. 
     * 
     * The path with the optional suffix appended will be returned if the tenant's 
     * base url could not be found.  If the optional pathSuffix is provided, it will 
     * be appended to the constructed url with a preceding slash if necessary.
     * 
     * @param tenantId the tenantId whose base url will be retrieved
     * @param path the path to append to the tenant's base url
     * @param pathSuffix optional suffix to append to the path
     * @return the tenant's base url with the path and suffix appended or just the 
     * 			path and suffix if the tenant is not found 
     */
    public static String constructTenantURL(String tenantId, String path, String pathSuffix)
    {
    	 // Append the optional suffix to the path to allow for early exit..
    	 if (!StringUtils.isBlank(pathSuffix)) {
    		 // Make sure there's exactly 1 slash between the path and suffix.
    		 if (path.endsWith("/") && pathSuffix.startsWith("/")) 
    			 pathSuffix = pathSuffix.substring(1);
    		 else if (!path.endsWith("/") && !pathSuffix.startsWith("/"))
    			 pathSuffix = "/" + pathSuffix;
    		 path += pathSuffix;
    	 }
    	 
    	 // Get the tenant object. TenantManager throws an exception if 
    	 // the tenant cannot be resolved.
    	 Tenant tenant;
    	 try {tenant = TenantManager.getInstance().getTenant(tenantId);}
    	 catch (Exception e) {return path;} // the error is already logged
    	 
    	 // Get the tenant record.
    	 String url = tenant.getBaseUrl();
    	 if (url == null) return path;
    	 
    	 // Construct the url with path separated by a slash.
		 if (url.endsWith("/") && path.startsWith("/")) 
			 path = path.substring(1);
		 else if (!url.endsWith("/") && !path.startsWith("/"))
			 path = "/" + path;
    	 return url + path;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* postSubscriptionRequest:                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Convert a subscribe request into a Notification ReqPostSubscription object
     * and pass that to Notifications to create a subscription.
     * 
     * @param reqSubscribe an incoming subscription request
     * @param user the owner of the subscription    
     * @param tenant the owner's tenant
     * @param jobUuid the target job's uuid
     * @return the new subscription's url returned by Notifications
     * @throws TapisClientException
     * @throws RuntimeException
     * @throws TapisException
     * @throws ExecutionException
     */
    public static String postSubscriptionRequest(ReqSubscribe reqSubscribe, String user,
                                                 String tenant, String jobUuid) 
     throws TapisClientException, RuntimeException, TapisException, ExecutionException
    {
        // Populate the request object.  The subjectFilter is always the jobEventType.
        var notifReq = new ReqPostSubscription();
        notifReq.setDescription(reqSubscribe.getDescription());
        notifReq.setEnabled(reqSubscribe.getEnabled());
        notifReq.setTtlMinutes(reqSubscribe.getTTLMinutes());
        notifReq.setSubjectFilter(jobUuid);
        
        // Set the targets.
        var notifTargets = new ArrayList<DeliveryTarget>();
        String lastDeliveryMethod = null; // external to try block for error handling.
        try {
            // Convert each request target into a notification target.
            for (var reqTarget : reqSubscribe.getDeliveryTargets()) {
                var notifTarget = new DeliveryTarget();
                notifTarget.setDeliveryAddress(reqTarget.getDeliveryAddress());
                lastDeliveryMethod = reqTarget.getDeliveryMethod().name();
                var notifMethod = DeliveryMethod.valueOf(lastDeliveryMethod);
                notifTarget.setDeliveryMethod(notifMethod);
                notifTargets.add(notifTarget);
            } 
        } catch (Exception e) {
            // The only possible exception is the string to enum conversion.
            var msg = JobUtils.getMsg("JOBS_UNKNOWN_ENUM", "DeliveryMethod", lastDeliveryMethod, jobUuid);
            throw new JobException(msg, e);
        }
        notifReq.setDeliveryTargets(notifTargets);
        
        // Fill in the required, non-payload request values. We leave the name and
        // tenant unassigned allowing Notifications to assign them.
        notifReq.setTypeFilter(getNotifTypeFilter(reqSubscribe.getEventCategoryFilter(), TYPE_FILTER_WILDCARD));
        notifReq.setOwner(user);
        
        // Send request to Notifications.
        var jobsImpl = JobsImpl.getInstance();
        return jobsImpl.postSubscription(notifReq, user, tenant);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getNotifTypeFilter:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Create a notification type filter with the following format:
     * 
     *     service.category.eventDetail
     * 
     * which for job subscriptions always looks like this:
     * 
     *     jobs.<jobEventType>.*
     *     
     * See EventReaders.makeNotifEventType() for the schema to which all Job events
     * conform.     
     * 
     * @param filter the 2nd component in a job subscription type filter
     * @param eventDetail specific event or the wildcard character
     * @return the 3 part type filter string
     */
    public static String getNotifTypeFilter(JobEventCategoryFilter filter, String eventDetail)
    {
        return JobUtils.makeNotifTypeFilter(filter, eventDetail);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* convertInputObjectToString:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** This specialized method allows user input defined with Java type Object and
     * json schema type object to be processed and validated.  It accepts the known 
     * concrete types of String and LinkedTreeMap, all other types throw an exception.  
     * The exceptions thrown by this method will abort any in progress API request. 
     * 
     * @param obj a json input object as some expected type
     * @return a validated json string
     * @throws TapisImplException when object to string conversion fails
     */
    public static String convertInputObjectToString(Object obj)
     throws TapisImplException
    {
        // Caller should customize null case if default isn't applicable.
        if (obj == null) return Job.EMPTY_JSON;
        
        // Input objects originating from apps or systems are strings.
        if (obj instanceof String) {
            String objStr = (String) obj;
            if (StringUtils.isBlank(objStr)) {
                String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "convertInputObjectToString", "obj");
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            
            // Make sure we have a well-formed json object.
            try {TapisGsonUtils.getGson().fromJson(objStr, JsonObject.class);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_JSON_PARSE_ERROR", "convertInputObjectToString",
                                                 objStr, e.getMessage());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
            
            // Return the serialized json object.
            return objStr;
        }
        
        // Input objects originating from a job interface are gson LinkedTreeMaps.
        if (obj instanceof LinkedTreeMap<?,?>) {
            // Convert obj to a string.
            String objStr = null;
            try {objStr = TapisGsonUtils.getGson().toJson(obj, linkedTreeMapType);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_JSON_SERIALIZATION_ERROR", 
                                                 obj.getClass().getSimpleName(), e.getMessage());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
            
            // Make sure we got something.
            if (StringUtils.isBlank(objStr)) {
                String msg = MsgUtils.getMsg("TAPIS_JSON_SERIALIZATION_ERROR", 
                                             obj.getClass().getSimpleName(), "null");
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            
            // Return the serialized json object.
            return objStr;
        } else {
            // Not a gson LinkedTreeMap.
            String msg = MsgUtils.getMsg("TAPIS_JSON_UNEXPECTED_OBJECT_TYPE", "obj", 
                                         obj.getClass().getSimpleName(), "LinkedTreeMap");
            throw new TapisImplException(msg, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    /* ---------------------------------------------------------------------------- */
    /* detectControlCharacters:                                                     */
    /* ---------------------------------------------------------------------------- */
    /** This method calls the sanitizer's control character detector and throw an
     * exception if one is detected.  This method does not check for characters that
     * need to be double quoted if they appear on the command line.
     * 
     * @param objectName the input containing object
     * @param fieldName the input field whose value is being inspected
     * @param value the value being inspected
     * @throws TapisImplException if a control character is found
     */
    public static void detectControlCharacters(String objectName, String fieldName, String value) 
     throws TapisImplException
    {
    	// We only check for control characters which are never appropriate and allow 
    	// the command line dangerous characters because they will be double quoted if
    	// they actually appear on the command line.
    	try {PathSanitizer.detectControlChars(value);}
        catch (Exception e) {
        	var sanitized = PathSanitizer.replaceControlChars(value, '?');
        	var msg = JobUtils.getMsg("JOBS_INVALID_CHAR_DETECTED", objectName, fieldName, 
        			                  sanitized, e.getMessage());
        	throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* hasDangerousCharacters:                                                      */
    /* ---------------------------------------------------------------------------- */
    /** This method detects control characters like \t, \n, \x0B, \f, \r and also checks 
     * for unsafe command line parameters like &, >, <, |, ;, `.
     * 
     * @param objectName the input containing object
     * @param fieldName the input field whose value is being inspected
     * @param value the value being inspected
     * @throws TapisImplException if a control or unsafe character is found
     */
    public static void hasDangerousCharacters(String objectName, String fieldName, String value)
     throws TapisImplException
    {
    	// Detect control characters and command line dangerous characters.
        if (!PathSanitizer.hasDangerousChars(value)) return; // no problems found
       
        // Invalid character found.
        var sanitized = PathSanitizer.replaceControlChars(value, '?');
        var msg = JobUtils.getMsg("JOBS_INVALID_INPUT_CHARACTERS", objectName, fieldName, sanitized);
        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    }

  /**
   * Validate call checks for tenantId, user and accountType
   * If all OK return null, else return error response.
   * @param threadContext thread context to check
   * @return null if all OK else error response
   */
  public static Response checkContext(TapisThreadContext threadContext)
  {
      if (threadContext.validate()) return null;
      String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg)).build();
  }

  // Simple wrapper for checking restricted svc permissions
  public static void checkRestrictedSvcs(SecurityContext securityContext)
  {
    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    TapisRestUtils.checkServiceRestrictions(TapisConstants.SERVICE_NAME_JOBS, JobsApplication.SVCLIST_TRUSTED, rUser);
  }
}
