package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppArgSpec;
import edu.utexas.tacc.tapis.apps.client.gen.model.ArgInputModeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSetArchiveFilter;
import edu.utexas.tacc.tapis.jobs.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.JobArgSpec;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.KeyValuePair;

public final class JobParmSetMarshaller 
{
    /* **************************************************************************** */
    /*                                 Constants                                    */
    /* **************************************************************************** */
    // Environment variable names that start with this prefix are reserved for Tapis.
    private static final String TAPIS_ENV_VAR_PREFIX = Job.TAPIS_ENV_VAR_PREFIX;
    
    /* **************************************************************************** */
    /*                                   Enums                                      */
    /* **************************************************************************** */
    // Tags that indicate the type of arguments being processed.
    public enum ArgTypeEnum {APP_ARGS, SCHEDULER_OPTIONS, CONTAINER_ARGS}
    
    /* **************************************************************************** */
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* mergeTapisProfileFromSystem:                                                 */
    /* ---------------------------------------------------------------------------- */
    /** This method inserts a tapis-profile scheduler option in the request 
     * schedulerOptions list if (1) the option isn't already present and (2) it was 
     * specified in the execution system definition.
     * 
     * @param schedulerOptions the request scheduler option AFTER merging with app
     *                         scheduler options.
     * @param batchSchedulerProfile the tapis-profile specified by the execution system
     */
    public void mergeTapisProfileFromSystem(List<JobArgSpec> schedulerOptions,
                                            String batchSchedulerProfile)
    {
        // Maybe there's nothing to merge.
        if (StringUtils.isBlank(batchSchedulerProfile)) return;
        final String key = Job.TAPIS_PROFILE_KEY + " ";
        
        // See if tapis-profile is already specified as a job request option.
        // The scheduler option list is never null.  If tapis-profile is found, 
        // we ignore the value defined in the system and immediately return.
        for (var opt : schedulerOptions) 
            if (opt.getArg().startsWith(key)) return;
        
        // If we get here then a tapis-profile option was not specified in
        // neither the app definition nor the job request, so the one in 
        // system wins the day.
        var spec = new JobArgSpec();
        spec.setArg(key + batchSchedulerProfile);
        spec.setDescription("The tapis-profile value set in execution system.");
        spec.setName("synthetic_tapis_profile");
        spec.setInclude(Boolean.TRUE);
        schedulerOptions.add(spec);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* marshalAppParmSet:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Populate the standard sharedlib version of ParameterSet with the generated
     * data passed by the apps and systems services.  
     * 
     * Note that we trust the app and systems inputs to conform to the schema 
     * defined in TapisDefinitions.json.
     * 
     * @param appParmSet the parameterSet retrieved from the app definition.
     * @param sysEnv the environment variable list from systems
     * @return the populate sharedlib parameterSet object, never null
     * @throws TapisImplException 
     */
//    public JobParameterSet marshalAppParmSet(
//        edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSet appParmSet,
//        List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> sysEnv) 
//     throws TapisImplException
//    {
//        // Always create a new, uninitialized parameter set.
//        var parmSet = new JobParameterSet(false);
//        if (appParmSet == null) {
//            // The system may define environment variables.
//            parmSet.setEnvVariables(marshalAppKvList(null, sysEnv));
//            return parmSet;
//        }
//        
//        // Null can be returned from the marshal method.
//        var appEnvVariables = appParmSet.getEnvVariables();
//        parmSet.setEnvVariables(marshalAppKvList(appEnvVariables, sysEnv));
//        
//        // Null can be returned from the marshal method.
//        var appArchiveFilter = appParmSet.getArchiveFilter();
//        parmSet.setArchiveFilter(marshalAppAchiveFilter(appArchiveFilter));
//        
//        return parmSet;
//    }

    /* ---------------------------------------------------------------------------- */
    /* marshalAppParmSet:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Populate the standard sharedlib version of ParameterSet with the generated
     * data passed by the apps and systems services.  
     * 
     * Note that we trust the app and systems inputs to conform to the schema 
     * defined in TapisDefinitions.json.
     * 
     * @param appParmSet the parameterSet retrieved from the app definition.
     * @param sysEnv the environment variable list from systems
     * @return the populate sharedlib parameterSet object, never null
     * @throws TapisImplException 
     */
//    public JobParameterSet marshalAppArchiveFillter(
//        edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSet appParmSet,
//        List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> sysEnv) 
//     throws TapisImplException
//    {
//        // Always create a new, uninitialized parameter set.
//        var parmSet = new JobParameterSet(false);
//        if (appParmSet == null) {
//            // The system may define environment variables.
//            parmSet.setEnvVariables(marshalAppKvList(null, sysEnv));
//            return parmSet;
//        }
//        
//        // Null can be returned from the marshal method.
//        var appEnvVariables = appParmSet.getEnvVariables();
//        parmSet.setEnvVariables(marshalAppKvList(appEnvVariables, sysEnv));
//        
//        // Null can be returned from the marshal method.
//        var appArchiveFilter = appParmSet.getArchiveFilter();
//        parmSet.setArchiveFilter(marshalAppAchiveFilter(appArchiveFilter));
//        
//        return parmSet;
//    }

    /* ---------------------------------------------------------------------------- */
    /* mergeArgSpecList:                                                            */
    /* ---------------------------------------------------------------------------- */
    /** The list of arguments from the submit request, reqList, is never null but could
     * be empty.  This list will be updated with the merged arguments completely 
     * replacing the original contents.
     * 
     * The list of arguments from the application, appList, can be null, empty or 
     * populated. 
     * 
     * @param reqList non-null submit request arguments (input/output)
     * @param appList application arguments (input only, possibly null)
     * @param argType the type of list whose args are being processed
     */
    public void mergeArgSpecList(List<JobArgSpec> reqList, List<AppArgSpec> appList,
                                 ArgTypeEnum argType)
    throws TapisImplException
    {
        // See if there's anything to do.
        int appListSize = (appList != null && appList.size() > 0) ? appList.size() : 0;
        int totalSize = reqList.size() + appListSize;
        if (totalSize == 0) return;
        
        // Create a scratch list of the maximum size and maintain proper ordering.
        var scratchList = new ArrayList<ScratchArgSpec>(totalSize);
        
        // ----------------------- App Args -----------------------
        // Maybe there's nothing to merge.
        if (appListSize > 0) {
            // Make sure there are no duplicate names among the app args.
            detectDuplicateAppArgNames(appList);
            
            // Include each qualifying app argument in the temporary list
            // preserving the original ordering.
            for (var appArg : appList) {
                // Set the input mode to the default if it's not set.
                // Args that originate from the application definition
                // always get a non-null inputMode. 
                var inputMode = appArg.getInputMode();
                if (inputMode == null) inputMode = ArgInputModeEnum.INCLUDE_ON_DEMAND;
                
                // Process the application argument.
                switch (inputMode) {
                    // These always go into the merged list.
                    case REQUIRED:
                    case FIXED:
                        scratchList.add(makeScratchArg(appArg, inputMode));
                        break;
                        
                    case INCLUDE_BY_DEFAULT:
                        if (includeArgByDefault(appArg.getName(), reqList)) 
                            scratchList.add(makeScratchArg(appArg, inputMode));
                        break;
                        
                    case INCLUDE_ON_DEMAND:
                        if (includeArgOnDemand(appArg.getName(), reqList))
                            scratchList.add(makeScratchArg(appArg, inputMode));
                        break;
                }
            }
        }        

        // --------------------- Request Args ---------------------
        // Work through the request list, overriding values on name matches
        // and adding anonymous args to the end of the scratch list.  
        if (reqList.size() > 0) {
            // Make sure there are no duplicate names among the request args.
            detectDuplicateReqArgNames(reqList);
            
            for (var reqArg : reqList) {
                // Get the name of the argument, which can be null.
                var reqName = reqArg.getName();
                
                // All ScratchArgSpecs that originate from an anonymous  
                // request argument have a null inputMode. 
                if (StringUtils.isBlank(reqName)) {
                    scratchList.add(new ScratchArgSpec(reqArg, null));
                    continue;
                }
                
                // Find the named argument in the list. 
                int scratchIndex = indexOfNamedArg(scratchList, reqName);
                if (scratchIndex < 0) {
                    // Does not override an application name.
                    scratchList.add(new ScratchArgSpec(reqArg, null));
                    continue;
                }
                
                // Check that we are not trying to override a FIXED app argument.
                detectFixedArgOverride(reqArg, scratchList.get(scratchIndex), argType);
                
                // Merge the request values into the argument that originated 
                // from the application definition, overriding where necessary 
                // but preserving the original list order.
                mergeJobArgs(reqArg, scratchList.get(scratchIndex)._jobArg);
            }
        }
        
        // ------------- Validation and Assignment ----------------
        // The scratchList now contains all the arguments from both the application
        // and the job request, with the application arguments listed first in their
        // original order, followed by the job request arguments in their original
        // order.  When a request argument overrode an application argument, the
        // values of the former were written to the latter and application argument
        // ordering was preserved.
        validateScratchList(scratchList, argType);
        
        // Save the contents of the scratchList to the reqList, completely replacing
        // the original reqList content.
        assignReqList(scratchList, reqList);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeEnvVariables:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Populate the standard sharedlib version of KeyValuePair with the generated
     * version passed by the apps and systems service.  The computational results 
     * are reflected the updated job request's key/value list.  
     * 
     * Note that we trust the apps and systems inputs to conform to the schema 
     * defined in TapisDefinitions.json.
     * 
     * @param reqKvList the request kv list, not null
     * @param appKvList apps generated kv list or null
     * @param sysKvList systems generated kv list or null
     * @throws TapisImplException 
     */
    public void mergeEnvVariables(List<KeyValuePair> reqKvList,
                                  List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair> appKvList,
                                  List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> sysKvList) 
     throws TapisImplException
    {
    	// ------------------- Job Request Preprocessing
        // Validate the names of request environment variables.
    	validateEnvVariableNames(reqKvList.stream().map(x -> x.getKey()).collect(Collectors.toList()), 
    			                 "job request");
    	
        // The app and systems lists are optional.
        if (appKvList == null && sysKvList == null) return;
    	
    	// ------------------- App/System Merge
        // Merge env variables from systems into env variables from apps
        // and return the resultant list that uses the app list type. This
        // merged list is never null.  The map keys are the env variable 
        // names and its values are the app's KeyValuePairs.
        var mergedAppKvList = mergeSysIntoAppEnvVariables(appKvList, sysKvList);
        var mergedAppKvMap = mergedAppKvList.stream().collect(
        	Collectors.toMap(edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair::getKey,
        			         Function.identity()));
        
        // ------------------- Job Request/App Merge
        // Initialize result list and the set of result key names. 
        var resultList = new ArrayList<KeyValuePair>(reqKvList.size() + mergedAppKvList.size());
        var resultKeys = new HashSet<String>(1 + 2*resultList.size());
        
        // Carry over valid variables from job request.
        for (var reqKv : reqKvList) {
        	
        }
        
        
        // Copy item by item from apps list.
        if (mergedAppKvList != null) {
            for (var appKv : mergedAppKvList) {
                // Set the input mode to the default if it's not set.
                // Args that originate from the application definition
                // always get a non-null inputMode. 
                var inputMode = appKv.getInputMode();
                if (inputMode == null) inputMode = 
                	edu.utexas.tacc.tapis.apps.client.gen.model.KeyValueInputModeEnum.INCLUDE_BY_DEFAULT;
                
                var kv = new KeyValuePair();
                if (resultKeys != null) resultKeys.add(appKv.getKey());
                kv.setKey(appKv.getKey());
                kv.setValue(appKv.getValue());
                kv.setDescription(appKv.getDescription());
                kv.setNotes(appKv.getNotes());
                //kvList.add(kv);
            }
        }
//        return kvList;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeArchiveFilters:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Merge application archive filters into the request archive filters.  The 
     * changes are immediately reflected in the request filter.  
     * 
     * @param reqFilter non-null archive filter from job request, used for output
     * @param appFilter archive filter from application, possibly null, input only
     * @throws TapisImplException
     */
    public void mergeArchiveFilters(IncludeExcludeFilter reqFilter, ParameterSetArchiveFilter appFilter) 
     throws TapisImplException
    {
        // Validate that the request environment variables contains no duplicate keys.
//        var reqEnvVars = reqParmSet.getEnvVariables();
//        HashSet<String> origReqEnvKeys = new HashSet<String>(1 + reqEnvVars.size() * 2);
//        for (var kv : reqEnvVars) {
//            // Reserved keys are not allowed.
//            if (kv.getKey().startsWith(TAPIS_ENV_VAR_PREFIX)) {
//                String msg = MsgUtils.getMsg("JOBS_RESERVED_ENV_VAR", kv.getKey(), 
//                                             TAPIS_ENV_VAR_PREFIX, "job request");
//                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
//            }
//            // Duplicates are not allowed.
//            if (!origReqEnvKeys.add(kv.getKey())) {
//                String msg = MsgUtils.getMsg("JOBS_DUPLICATE_ENV_VAR", "job request", kv.getKey());
//                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
//            }
//        }
//        
//        // Add the environment variables from the app only if they do not already
//        // exist in the request set.  The app list has already been checked for
//        // duplicates and reserved names in the marshalling code.
//        var appEnvVars = appParmSet.getEnvVariables();
//        if (appEnvVars != null && !appEnvVars.isEmpty())
//            for (var kv : appEnvVars) 
//                if (!origReqEnvKeys.contains(kv.getKey())) reqParmSet.getEnvVariables().add(kv);    
    	
    	// Always normalize the request archive filter.
    	reqFilter.initAll(); // assign empty lists if null

    	// Is there anything to merge?
    	if (appFilter == null) return;
    	
    	// Merge the archive files.  The elements of the includes and excludes lists can
    	// be globs or regexes.  The two are distinguished by prefixing regexes with "REGEX:"
    	// whereas globs are written as they would appear on a command line.
    	var appIncludes = appFilter.getIncludes();
    	var appExcludes = appFilter.getExcludes();
    	if (appIncludes != null && !appIncludes.isEmpty()) 
    		reqFilter.getIncludes().addAll(appIncludes);
    	if (appExcludes != null && !appExcludes.isEmpty()) 
    		reqFilter.getExcludes().addAll(appExcludes);
            
        // Assign the launch file inclusion flag.
        if (reqFilter.getIncludeLaunchFiles() == null)
        	reqFilter.setIncludeLaunchFiles(appFilter.getIncludeLaunchFiles());
        if (reqFilter.getIncludeLaunchFiles() == null)
        	reqFilter.setIncludeLaunchFiles(Boolean.TRUE);
    }

    /* **************************************************************************** */
    /*                              Private Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* makeScratchArg:                                                              */
    /* ---------------------------------------------------------------------------- */
    private ScratchArgSpec makeScratchArg(AppArgSpec appArg, ArgInputModeEnum inputMode)
    {
        return new ScratchArgSpec(convertToJobArgSpec(appArg), inputMode);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* convertToJobArgSpec:                                                         */
    /* ---------------------------------------------------------------------------- */
    private JobArgSpec convertToJobArgSpec(AppArgSpec appArg)
    {
        var jobArg = new JobArgSpec();
        jobArg.setName(appArg.getName());
        jobArg.setDescription(appArg.getDescription());
        jobArg.setArg(appArg.getArg());
        if (appArg.getNotes() != null)
            jobArg.setNotes(appArg.getNotes().toString());  
        return jobArg;
    }
    
    // ============================================
    // Truth Table for App Arg Inclusion
    //  
    //  AppArgSpec          JobArgSpec  Meaning
    //  inputMode           include 
    //  -------------------------------------------
    //  INCLUDE_ON_DEMAND   True        include arg
    //  INCLUDE_ON_DEMAND   False       exclude arg
    //  INCLUDE_ON_DEMAND   undefined   include arg
    //  INCLUDE_BY_DEFAULT  True        include arg
    //  INCLUDE_BY_DEFAULT  False       exclude arg
    //  INCLUDE_BY_DEFAULT  undefined   include arg
    // ============================================
    
    /* ---------------------------------------------------------------------------- */
    /* includeArgByDefault:                                                         */
    /* ---------------------------------------------------------------------------- */
    private boolean includeArgByDefault(String argName, List<JobArgSpec> reqList)
    {
        // See if the include-by-default appArg has been 
        // explicitly excluded in the request.
        for (var reqArg : reqList) {
            if (!argName.equals(reqArg.getName())) continue;
            if (reqArg.getInclude() == null || reqArg.getInclude()) return true;
              else return false;
        }
        
        // If the appArg is not referenced in a request arg,
        // the default action is to respect the app definition
        // and include it by default.
        return true;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* includeArgOnDemand:                                                          */
    /* ---------------------------------------------------------------------------- */
    private boolean includeArgOnDemand(String argName, List<JobArgSpec> reqList)
    {
        // See if the include-on-demand appArg should be included in the 
        // request by either being simply referenced or explicitly included.
        for (var reqArg : reqList) {
            if (!argName.equals(reqArg.getName())) continue;
            if (reqArg.getInclude() == null || reqArg.getInclude()) return true;
              else return false;
        }
        
        // If the appArg is not referenced in a request arg,
        // the default action is to respect the app definition
        // and not include it.
        return false;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* detectDuplicateAppArgNames:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Validate that there each argument in list from an application definition has
     * a unique name.
     * 
     * @param appList application definition arg list
     * @throws TapisImplException when duplicates are detected
     */
    private void detectDuplicateAppArgNames(List<AppArgSpec> appList) throws TapisImplException
    {
        var names = new HashSet<String>(2*appList.size()+1);
        for (var appArg : appList)
            if (!names.add(appArg.getName())) {
                String msg = MsgUtils.getMsg("JOBS_DUPLICATE_NAMED_ARG", 
                                             "application definition", appArg.getName());
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* detectDuplicateReqArgNames:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Validate that there each argument in list from a job submission request has
     * a unique name among those that provide names.
     * 
     * @param reqList job request definition arg list
     * @throws TapisImplException when duplicates are detected
     */
    private void detectDuplicateReqArgNames(List<JobArgSpec> reqList) throws TapisImplException
    {
        var names = new HashSet<String>(2*reqList.size()+1);
        for (var reqArg : reqList) {
            var name = reqArg.getName();
            if (StringUtils.isBlank(name)) continue;
            if (!names.add(name)) {
                String msg = MsgUtils.getMsg("JOBS_DUPLICATE_NAMED_ARG", "job request", name);
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* detectFixedArgOverride:                                                      */
    /* ---------------------------------------------------------------------------- */
    /** Detect whether an attempt is being made to change a fixed argument in a 
     * computationally significant way.  If a job request tries to change an argument 
     * defined as fixed in the application definition in any way other than the description,
     * it will cause this method to thrown an exception.
     * 
     * This method is only called then the reqArg's name is non-null and matches that
     * of the application's argument.
     * 
     * @param reqArg the argument redefining an argument
     * @param scratchSpec the original argument 
     * @param argType semantic type of argument
     * @throws TapisImplException when a fixed app argument is changed in a job request
     */
    private void detectFixedArgOverride(JobArgSpec reqArg, ScratchArgSpec scratchSpec,
    		                            ArgTypeEnum argType)
     throws TapisImplException
    {
    	// Is this a fixed argument originating in the application?
    	if (scratchSpec._inputMode != ArgInputModeEnum.FIXED) return;
    	
    	// Make sure the request doesn't materially change the argument's definition.
    	// Returning from here means that at most the description values may have 
    	// changed.  Since those values are additive and are only for human consumption,
    	// we allow it.
    	//
    	// We protect ourselves from npe's when even in the case of argument values
    	// when it's unlikely that they can be null because FIXED is being used.
    	var appArg = scratchSpec._jobArg;
    	if (reqArg.getArg() != null && appArg.getArg() != null 
    		&&
    		reqArg.getArg().equals(appArg.getArg()) 
    		&& 
    		((reqArg.getNotes() == null && appArg.getNotes() == null) 
    			|| (reqArg.getNotes() != null && reqArg.getNotes().equals(appArg.getNotes()))))
    	  return;
    	
    	// Bail out, we detected a change in the actionable part of the argument definition. 
        String msg = MsgUtils.getMsg("JOBS_FIXED_ARG_ERROR", reqArg.getName(), "job", "application", 
        		                     argType.name().toLowerCase());
        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* detectFixedEnvVarOverride:                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Detect whether an attempt is being made to change a fixed env variable value in a 
     * computationally significant way.  If an application definition tries to change an 
     * env variable defined as fixed in the system definition in any way other than the 
     * description, it will cause this method to thrown an exception.
     * 
     * This method assumes it is only called when the appKv tries to override a FIXED 
     * environment variable definition from a system.
     * 
     * @param reqArg the argument redefining an argument
     * @param scratchSpec the original argument 
     * @param argType semantic type of argument
     * @throws TapisImplException when a fixed app argument is changed in a job request
     */
    private void detectFixedEnvVarOverride(
    		           edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair appKv,
    		           edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair fixedSysKv)
     throws TapisImplException
    {
    	// Make sure the request doesn't materially change the env variable's definition.
    	// Returning from here means that at most the description values may have 
    	// changed.  Since those values are additive and are only for human consumption,
    	// we allow it.
    	//
    	// We protect ourselves from npe's when even in the case of env variable values
    	// when it's unlikely that they can be null because FIXED is being used.
    	if (appKv.getValue() != null && fixedSysKv.getValue() != null 
    		&&
    		appKv.getValue().equals(fixedSysKv.getValue()) 
    		&& 
    		((appKv.getNotes() == null && fixedSysKv.getNotes() == null) 
    			|| (appKv.getNotes() != null && appKv.getNotes().equals(fixedSysKv.getNotes()))))
    	  return;
    	
    	// Bail out, we detected a change in the actionable part of the env variable definition. 
        String msg = MsgUtils.getMsg("JOBS_FIXED_ENV_VAR_ERROR", appKv.getKey(), "application", "system");
        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateEnvVariableNames:                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Make sure environment variable don't used the reserved _tapis prefix or 
     * contain duplicates.
     * 
     * @param list list of environment variable names
     * @param source the source to be used in error message
     * @throws TapisImplException when validation fails
     */
    private void validateEnvVariableNames(List<String> list, String source) 
     throws TapisImplException
    {
    	// Easy cases.
    	if (list == null || list.isEmpty()) return;
    
    	// Check for duplicates.
    	var names = new HashSet<String>(1 + list.size() * 2);
    	for (var name : list) {
    		// Reserved keys are not allowed.
    		if (name.startsWith(TAPIS_ENV_VAR_PREFIX)) {
    			String msg = MsgUtils.getMsg("JOBS_RESERVED_ENV_VAR", name, 
                                             TAPIS_ENV_VAR_PREFIX, "job request");
    			throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    		}
    		
    		// Duplicates are not allowed.
    		if (!names.add(name)) {
    			String msg = MsgUtils.getMsg("JOBS_DUPLICATE_ENV_VAR", "job request", name);
    			throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    		}
    	}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* indexOfNamedArg:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Find the argument in the list with the specified name.
     * 
     * @param scratchList non-null scratch list
     * @param name non-empty search name
     * @return the index of the matching element or -1 for no match
     */
    private int indexOfNamedArg(List<ScratchArgSpec> scratchList, String name)
    {
        // See if the named argument already exists in the list.
        for (int i = 0; i < scratchList.size(); i++) {
            var curName = scratchList.get(i)._jobArg.getName();
            if (StringUtils.isBlank(curName)) continue;
            if (curName.equals(name)) return i;
        }
        
        // No match.
        return -1;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeJobArgs:                                                                */
    /* ---------------------------------------------------------------------------- */
    /** The sourceArg is a reqArg and the targetArg is an appArg that has already been
     * converted into a JobArgSpec.  The targetArg is already in the result list and
     * this method performs in-place replacement of appArg values with those from
     * the reqArg.
     * 
     * @param sourceArg reqArg
     * @param targetArg a converted appArg that we may modify
     */
    private void mergeJobArgs(JobArgSpec sourceArg, JobArgSpec targetArg)
    {
        // The request flag always assigns the target flag, which is always null.
        targetArg.setInclude(sourceArg.getInclude());
        
        // Conditional replacement.
        if (!StringUtils.isBlank(sourceArg.getArg())) 
            targetArg.setArg(sourceArg.getArg());
        if (sourceArg.getNotes() != null)
            targetArg.setNotes(sourceArg.getNotes());
        
        // Append a non-empty source description to an existing target description.
        // Otherwise, just assign the target description the non-empty source description.
        if (!StringUtils.isBlank(sourceArg.getDescription()))
            if (StringUtils.isBlank(targetArg.getDescription()))
                targetArg.setDescription(sourceArg.getDescription());
            else 
                targetArg.setDescription(
                    targetArg.getDescription() + "\n\n" + sourceArg.getDescription());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeSysIntoAppEnvVariables:                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Merge the application and system definition environment variables.  Either can
     * be null, though both being null is not expected.  
     * 
     * The result is always a non-null list of merged environment variables that 
     * contains no duplicates.  The appKvList is used for both input and output.  The
     * result is the appKvList with non-overriddent system environment variable appended.
     * 
     * @param appKvList env variables from application definition, can be null
     * @param sysKvList env variables from system definition, can be null
     * @return the non-null merged list of environment variables
     * @throws TapisImplException on bad input
     */
    private List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair> mergeSysIntoAppEnvVariables(
    			List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair> appKvList, 
    			List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair>sysKvList) 
     throws TapisImplException
    {
    	// Normalize input lists.  The appKvList is also used as the output list.
    	if (appKvList == null) appKvList = new ArrayList<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair>(0);
    	if (sysKvList == null) sysKvList = new ArrayList<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair>(0);
    	
    	// Validate the names of environment variables in each list.
    	validateEnvVariableNames(appKvList.stream().map(x -> x.getKey()).collect(Collectors.toList()), 
    			                 "application definition");
    	validateEnvVariableNames(sysKvList.stream().map(x -> x.getKey()).collect(Collectors.toList()), 
                				"system definition");
    	
    	// Get a map of all system keys to values that are FIXED and therefore 
    	// cannot be overwritten. Populate the list of FIXED system key/value pairs 
    	// so that we can later check for attempts to override their values.
    	HashMap<String, edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> fixedSysKVs = new HashMap<>();
        for (var sysKv : sysKvList)
        	if (sysKv.getInputMode() == edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueInputModeEnum.FIXED)
        		fixedSysKVs.put(sysKv.getKey(), sysKv);
        
        // Set of merged application keys.
        HashSet<String> mergedKeys = new HashSet<>();
        
        // Iterate through the elements in the apps list.
        for (var appKv : appKvList) {
            // Make sure we are not improperly overriding a FIXED system env variable.
            var fixedSysKv = fixedSysKVs.get(appKv.getKey());
            if (fixedSysKv != null) detectFixedEnvVarOverride(appKv, fixedSysKv); // possible exceptions here
            
            // Record this key as part of result keys.
            mergedKeys.add(appKv.getKey());
        }
        
        // Add any keys from the system list that are not already in app list.
    	for (var sysKv : sysKvList) {
    		// See if this variable has been overridden by an apps variable.
    		if (mergedKeys.contains(sysKv.getKey())) continue;
    		
    		// Set the input mode to the default if it's not set.
    		// Args that originate from the systems definition should
    		// always have a non-null inputMode, but we double check. 
    		var sysInputMode = sysKv.getInputMode();
    		if (sysInputMode == null) sysInputMode = 
    			edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueInputModeEnum.INCLUDE_BY_DEFAULT;
    		var inputMode = edu.utexas.tacc.tapis.apps.client.gen.model.KeyValueInputModeEnum.valueOf(sysInputMode.name());
          
    		// Convert the system env variable to an app environment variable.
    		var kv = new edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair();
    		kv.setKey(sysKv.getKey());
    		kv.setValue(sysKv.getValue());
    		kv.setDescription(sysKv.getDescription());
    		kv.setInputMode(inputMode);
    		kv.setNotes(sysKv.getNotes());
          
    		// Add to app variables lists.
    		mergedKeys.add(kv.getKey());
    		appKvList.add(kv);
    	}
    	
    	return appKvList;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateScratchList:                                                         */
    /* ---------------------------------------------------------------------------- */
    private void validateScratchList(List<ScratchArgSpec> scratchList, ArgTypeEnum argType)
     throws TapisImplException
    {
        // Final scrubbing of scratch list and argument values.
        var it = scratchList.listIterator();
        while (it.hasNext()) {
            // Make sure all arguments are either complete or able to be removed.  
            // Incomplete arguments that originated in the app are removable if their
            // inputMode is INCLUDE_BY_DEFAULT.  All other incomplete arguments cause
            // an error.  A null input mode indicates the argument originated from 
            // the job request.
            var elem = it.next();
            if (StringUtils.isBlank(elem._jobArg.getArg()))
                if (elem._inputMode == ArgInputModeEnum.INCLUDE_BY_DEFAULT) {
                    it.remove();
                    continue; // no further processing needed for removed args
                }
                else {
                    String msg = MsgUtils.getMsg("JOBS_MISSING_ARG", elem._jobArg.getName(), argType);
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
            
            // Make sure notes field is a well-formed json object and convert it to string.
            // We skip elements without notes.
            if (elem._jobArg.getNotes() != null) {
                var json = JobsApiUtils.convertInputObjectToString(elem._jobArg.getNotes());
                elem._jobArg.setNotes(json);
            }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* assignReqList:                                                               */
    /* ---------------------------------------------------------------------------- */
    private void assignReqList(List<ScratchArgSpec> scratchList, List<JobArgSpec> reqList)
    {
        // Always clear the request list.
        reqList.clear();
        
        // Assign each of the arguments from the scratch list.
        for (var elem : scratchList) reqList.add(elem._jobArg);
    }
    
    /* **************************************************************************** */
    /*                            ScratchArgSpec Class                              */
    /* **************************************************************************** */
    // Simple record for internal use only.
    private static final class ScratchArgSpec
    {
        private ArgInputModeEnum _inputMode;
        private JobArgSpec       _jobArg;
        
        private ScratchArgSpec(JobArgSpec jobArg, ArgInputModeEnum inputMode) 
        {_jobArg = jobArg; _inputMode = inputMode;}
    }
}
