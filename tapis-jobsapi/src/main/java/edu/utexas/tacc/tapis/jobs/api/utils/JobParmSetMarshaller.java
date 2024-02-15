package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppArgSpec;
import edu.utexas.tacc.tapis.apps.client.gen.model.ArgInputModeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.KeyValueInputModeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.ParameterSetArchiveFilter;
import edu.utexas.tacc.tapis.jobs.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.JobArgSpec;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.KeyValuePair;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobParmSetMarshaller 
{
    /* **************************************************************************** */
    /*                                 Constants                                    */
    /* **************************************************************************** */
    // Environment variable names that start with this prefix are reserved for Tapis.
    private static final String TAPIS_ENV_VAR_PREFIX = Job.TAPIS_ENV_VAR_PREFIX;
    
    // The distinguished string value of an environment variable that is interpreted 
    // as the variable being unset (as opposed to set to the empty string, which is 
    // a valid value).
    public static final String TAPIS_ENV_VAR_UNSET = TapisConstants.TAPIS_NOT_SET;
    public static final String TAPIS_ENV_VAR_DEFAULT_VALUE = "";
    
    // Limit environment key names to alphnumerics and "_", starting with an alpha.
    public static final Pattern _envKeyPattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    
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
     * @throws TapisImplException 
     */
    public void mergeTapisProfileFromSystem(List<JobArgSpec> schedulerOptions,
                                            String batchSchedulerProfile) 
     throws TapisImplException
    {
        // Maybe there's nothing to merge.
        if (StringUtils.isBlank(batchSchedulerProfile)) return;
        final String key = Job.TAPIS_PROFILE_KEY + " ";
        
        // See if tapis-profile is already specified as a job request option.
        // The scheduler option list is never null.  If tapis-profile is found, 
        // we ignore the value defined in the system and immediately return.
        for (var opt : schedulerOptions) 
            if (opt.getArg().startsWith(key)) return;
        
        // Validate the exec system's profile before using it.
        JobsApiUtils.detectControlCharacters("schedulerOptions", "batchSchedulerProfile", batchSchedulerProfile);
        
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
    	
    	// Eliminate the possibility of null include and notes fields in the request env variables.
    	for (var reqKv : reqKvList) if (reqKv.getInclude() == null) reqKv.setInclude(Boolean.TRUE);
    	
        // The app and systems lists are optional.
        if (appKvList == null && sysKvList == null) {
        	finalizeEnvVariables(reqKvList);
        	return;
        }
    	
    	// ------------------- App/System Merge
        // Merge env variables from systems into env variables from apps
        // and return the resultant list that uses the app list type. The
        // merged list is never null.
        var mergedAppKvList = mergeSysIntoAppEnvVariables(appKvList, sysKvList);
        
        // ------------------- Filter Merged List
        // Determine which merged env variable to keep for inclusion in the final result list.
        // After this loop runs the mergeAppKvList will contain all the values that should end
        // up in the final result list.
        {
        	// Start a new block to limit scope of iterator.
        	var mergedAppIter = mergedAppKvList.iterator();
        	while (mergedAppIter.hasNext()) {
        		// Include the app/system env variable based on input mode and, possibly, 
        		// the include flag in the corresponding request env variable.
        		var appKv = mergedAppIter.next();
        		var inputMode = appKv.getInputMode();
        		switch (inputMode) {
    				case FIXED:
    				case REQUIRED:
    					break;
    				
    				case INCLUDE_BY_DEFAULT:
    					if (!includeEnvVarByDefault(appKv, reqKvList)) 
    						mergedAppIter.remove();
    					break;
    				
    				case INCLUDE_ON_DEMAND:
    					if (!includeEnvVarOnDemand(appKv.getKey(), reqKvList)) 
    						mergedAppIter.remove();
    					break;
        		}
        	}
        }
        
        // ------------------- Job Request Merge
        // Iterate through the env variables set in the job request, merging them with those 
        // from apps and systems variables where necessary.  Elements in the mergedAppKvList are
        // removed as we process the reqKvList and app/system information is transferred into
        // existing reqKv's.  The residue in mergedAppKvList are those variables assigned in
        // apps or systems definitions that are not overridden or referenced from the reqKvList;
        // those variables will later be appended to the end of the reqKvList. 
        var reqIter = reqKvList.iterator();
        while (reqIter.hasNext()) {
        	// Get the job request key/value.
        	var reqKv = reqIter.next();
        	
        	// Does this key override one already in the merged list?
        	var mergedAppIter = mergedAppKvList.iterator();
        	while (mergedAppIter.hasNext()) {
        		// For merging, we're only interested in matches.
        		var appKv = mergedAppIter.next();
        		if (!reqKv.getKey().equals(appKv.getKey())) continue;
        	
            	// Merge the job request's key/value into the existing result's 
            	// key/value if possible.  Attempts to merge incompatible env
            	// variables will throw an exception.
        		//
        		// Either the merge throws an exception or it extracts some or all
        		// appKv information for insertion into the reqKv.  In the latter
        		// case, we can safely remove the appKv from its list.
        		mergeAppIntoReqEnvVariable(reqKv, appKv);
        		mergedAppIter.remove(); 
        	}
        }
        
        // Update the existing request list with app and system variables leftover in 
        // mergedAppKvList. The resulting list maintains the original request list ordering
        // minus entries that were removed, followed by unreferenced app entries, folllowed
        // by unreferenced system entries.
        var convertedList = mergedAppKvList.stream().map(x -> convertToKeyValuePair(x)).collect(Collectors.toList());
        reqKvList.addAll(convertedList);
        
        // Validate and finalize the list of env variables.
        finalizeEnvVariables(reqKvList);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* finalizeEnvVariables:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** Validate that all env variable objects are well-formed.  Assign all notes 
     * fields valid json strings.  By the time this method is call, the env variable
     * names from systems, apps and the request have been run through 
     * validateEnvVariableNames(), so no further name checking is needed. In particular,
     * dangerous characters have been prohibited by the validation method. 
     * 
     * @param reqKvList the list of candidate env variables
     * @throws TapisImplException on validation failure
     */
    private void finalizeEnvVariables(List<KeyValuePair> reqKvList) 
     throws TapisImplException
    {
    	// Validate and finalize each env variable in the request list.
    	for (var reqKv : reqKvList) {
    		// Incomplete env variables are show stoppers. The value should never
    		// be null, but double checking confirms this.
    		var value = reqKv.getValue();
    		if (value == null || TAPIS_ENV_VAR_UNSET.equals(value)) {
                String msg = MsgUtils.getMsg("JOBS_MISSING_ENV_VALUE", reqKv.getKey(), value);
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    		}

            // Detect control characters in the value.
    		JobsApiUtils.detectControlCharacters("envVariables", reqKv.getKey(), value);
    		
    		// Convert notes objects into json strings and validate. Nulls are 
    		// converted to the empty json object string.
    		reqKv.setNotes(JobsApiUtils.convertInputObjectToString(reqKv.getNotes()));
    	}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* mergeAppIntoReqEnvVariable:                                                  */
    /* ---------------------------------------------------------------------------- */
    private void mergeAppIntoReqEnvVariable(KeyValuePair reqKv, 
    				 edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair appKv) 
     throws TapisImplException
    {
    	// Special input mode processing that can throw an exception.
    	var appInputMode = appKv.getInputMode();
    	if (appInputMode == KeyValueInputModeEnum.FIXED) detectFixedEnvVarOverride(reqKv, appKv); 
    	
    	// Merge the app fields into the request fields.
    	if (reqKv.getValue().equals(TAPIS_ENV_VAR_UNSET)) reqKv.setValue(appKv.getValue());
    	if (reqKv.getNotes() == null) reqKv.setNotes(appKv.getNotes());
    	
    	// Combine descriptions.
    	if (!StringUtils.isBlank(appKv.getDescription()))
    		if (StringUtils.isBlank(reqKv.getDescription()))
    			reqKv.setDescription(appKv.getDescription());
    		else 
    			reqKv.setDescription(reqKv.getDescription() + "\n\n" + appKv.getDescription());
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
    /* convertToKeyValuePair:                                                       */
    /* ---------------------------------------------------------------------------- */
    private KeyValuePair convertToKeyValuePair(
    	edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair appKv)
    {
        var jobKv = new KeyValuePair();
        jobKv.setKey(appKv.getKey());
        jobKv.setDescription(appKv.getDescription());
        jobKv.setValue(appKv.getValue());
        if (appKv.getNotes() != null)
            jobKv.setNotes(appKv.getNotes().toString());  
        return jobKv;
    }
    
    // ==============================================
    // Truth Table for Key/Value Inclusion
    //  
    //  AppArgSpec          KeyValuePair  Meaning
    //  inputMode           include 
    //  ---------------------------------------------
    //  INCLUDE_ON_DEMAND   True          include arg
    //  INCLUDE_ON_DEMAND   False         exclude arg
    //  INCLUDE_ON_DEMAND   undefined     include arg
    //  INCLUDE_BY_DEFAULT  True          include arg
    //  INCLUDE_BY_DEFAULT  False         exclude arg
    //  INCLUDE_BY_DEFAULT  undefined     include arg
    // ==============================================
    
    /* ---------------------------------------------------------------------------- */
    /* includeEnvVarByDefault:                                                      */
    /* ---------------------------------------------------------------------------- */
    /** Determine whether an app/system variable should be merged into the request
     * variable list by enforcing the include setting from a referencing request
     * variable if one exists.
     * 
     * @param appKv the candidate app/system variable with inputMode INCLUDE_BY_DEFAULT  
     * @param reqList the request variable list that might reference the candidate appKv
     * @return true to merge the app/system variable, false to discard it
     */
    private boolean includeEnvVarByDefault(
    	edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair appKv, List<KeyValuePair> reqList)
    {
        // See if the include-by-default appKey has been explicitly excluded in the request.
        for (var reqKv : reqList) {
            if (!appKv.getKey().equals(reqKv.getKey())) continue;
            if (reqKv.getInclude() == null || reqKv.getInclude()) return true;
              else return false;
        }
        
        // If no request variable references the app variable, and the inputMode
        // of the app variable is INCLUDE_BY_DEFAULT, and the value is not set,
        // then instead of allowing an incomplete variable definition to be flagged
        // during finalization, we remove the variable now and thereby allow the
        // job to continue.
        if (TAPIS_ENV_VAR_UNSET.equals(appKv.getValue()) &&
        	appKv.getInputMode() == KeyValueInputModeEnum.INCLUDE_BY_DEFAULT) 
        	return false;
        
        // If the appKey is not referenced in a request env variable, the default
        // action is to respect the app definition and include it.
        return true;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* includeEnvVarOnDemand:                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Determine whether an app/system variable should be merged into the request
     * variable list by enforcing the include setting from a referencing request
     * variable if one exists.
     * 
     * @param appKv the candidate app/system variable key with inputMode INCLUDE_ON_DEMAND 
     * @param reqList the request variable list that might reference the candidate appKv
     * @return true to merge the app/system variable, false to discard it
     */
    private boolean includeEnvVarOnDemand(String appKey, List<KeyValuePair> reqList)
    {
        // See if the include-on-demand appArg should be included in the 
        // request by either being simply referenced or explicitly included.
        for (var reqKv : reqList) {
            if (!appKey.equals(reqKv.getKey())) continue;
            if (reqKv.getInclude() == null || reqKv.getInclude()) return true;
              else return false;
        }
        
        // If the appArg is not referenced in a request env variable, the default
        // action is to respect the app definition and not include it.
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
    	
    	// Get the original arg spec that we cannot override.
    	var appArg = scratchSpec._jobArg;

    	// Canonicalize the notes fields by equating null, the empty string and
    	// the empty json object to be the same.
    	var reqNotes = reqArg.getNotes() == null || "".equals(reqArg.getNotes()) ? 
    			TapisConstants.EMPTY_JSON : reqArg.getNotes();
    	var appNotes = appArg.getNotes() == null || "".equals(appArg.getNotes()) ? 
    			TapisConstants.EMPTY_JSON : appArg.getNotes();
    	
    	// Make sure the request doesn't materially change the argument's definition.
    	// Returning from here means that at most the description values may have 
    	// changed.  Since those values are additive and are only for human consumption,
    	// we allow it.
    	//
    	// We protect ourselves from npe's when even in the case of argument values
    	// when it's unlikely that they can be null because FIXED is being used.
    	if (reqArg.getArg() != null && appArg.getArg() != null 
    		&&
    		reqArg.getArg().equals(appArg.getArg()) 
    		&& 
    		reqNotes.equals(appNotes))
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
     * description, it will cause this method to throw an exception.
     * 
     * This method assumes it is only called when the appKv tries to override a FIXED 
     * environment variable definition from a system. It also assumes neither parameter 
     * can be null and normalizeEnvVariableLists() has been called. 
     * 
     * @param appKv non-null env variable from application definition
     * @param fixedSysKv non-null env variable same key and inputMode set to FIXED 
     * @throws TapisImplException when an app env variable tries to change a FIXED system variable
     */
    private void detectFixedEnvVarOverride(
    		           edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair appKv,
    		           edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair fixedSysKv)
     throws TapisImplException
    {
    	// Canonicalize the notes fields by equating null, the empty string and
    	// the empty json object to be the same.
    	var appNotes = appKv.getNotes() == null || "".equals(appKv.getNotes()) ? 
    			TapisConstants.EMPTY_JSON : appKv.getNotes();
    	var fixNotes = fixedSysKv.getNotes() == null || "".equals(fixedSysKv.getNotes()) ? 
    			TapisConstants.EMPTY_JSON : fixedSysKv.getNotes();
    	
    	// Make sure the app doesn't materially change the system env variable's definition.
    	// Returning from here means that at most the description values may be different.  
    	// Since those values are additive and are only for human consumption, we allow it.
    	//
    	// We protect ourselves from npe's when dealing with notes fields, which may not be set.
    	if (appKv.getInputMode() == KeyValueInputModeEnum.FIXED 
    		&&
    		appKv.getValue().equals(fixedSysKv.getValue())
    		&&
    		appNotes.equals(fixNotes))
    	  return;
    	
    	// Bail out, we detected a change in the actionable part of the system env variable definition. 
        String msg = MsgUtils.getMsg("JOBS_FIXED_ENV_VAR_ERROR", appKv.getKey(), "application", "system");
        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* detectFixedEnvVarOverride:                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Detect whether an attempt is being made to change a fixed env variable value in a 
     * computationally significant way.  If a job request tries to change an env variable 
     * defined as fixed in the app or system definition in any way other than the 
     * description, it will cause this method to thrown an exception.
     * 
     * This method assumes it is only called when the appKv tries to override a FIXED 
     * environment variable definition from an app or system. Neither parameter can be null.
     * 
     * @param reqKv non-null env variable from a job request
     * @param fixedAppKv non-null FIXED env variable from application or system definition
     * @throws TapisImplException when a job request env variable tries to change a FIXED app/system variable
     */
    private void detectFixedEnvVarOverride(
    				   KeyValuePair reqKv,
    		           edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair fixedAppKv)
     throws TapisImplException
    {
    	// Canonicalize the notes fields by equating null, the empty string and
    	// the empty json object to be the same.
    	var reqNotes = reqKv.getNotes() == null || "".equals(reqKv.getNotes()) ? 
    			TapisConstants.EMPTY_JSON : reqKv.getNotes();
    	var fixNotes = fixedAppKv.getNotes() == null || "".equals(fixedAppKv.getNotes()) ? 
    			TapisConstants.EMPTY_JSON : fixedAppKv.getNotes();
    	
    	// Make sure the request doesn't materially change the app env variable's definition.
    	// Returning from here means that at most the description values may be different.  
    	// Since those values are additive and are only for human consumption, we allow it.
    	//
    	// We protect ourselves from npe's when dealing with notes fields, which may not be set.
    	if (reqKv.getValue().equals(fixedAppKv.getValue()) 
    		&&
    		reqNotes.equals(fixNotes))
    	  return;
    	
    	// Bail out, we detected a change in the actionable part of the system env variable definition. 
        String msg = MsgUtils.getMsg("JOBS_FIXED_ENV_VAR_ERROR", fixedAppKv.getKey(), "job request", "application");
        throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    }
    
    /* ---------------------------------------------------------------------------- */
    /* normalizeEnvVariableLists:                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Set default for fields that are null.
     * 
     * @param appKvList the original apps env variable list
     * @param sysKvList the original systems env variable list
     */
    private void normalizeEnvVariableLists(
    	List<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair> appKvList,
    	List<edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> sysKvList)
    {
    	// Set apps list defaults.
    	for (var kv : appKvList) {
    		if (kv.getInputMode() == null) kv.setInputMode(KeyValueInputModeEnum.INCLUDE_BY_DEFAULT);
    		if (kv.getValue() == null) kv.setValue(TAPIS_ENV_VAR_DEFAULT_VALUE);
    	}

    	// Set systems list defaults.
    	for (var kv : sysKvList) {
    		if (kv.getInputMode() == null)
    			kv.setInputMode(edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueInputModeEnum.INCLUDE_BY_DEFAULT);
    		if (kv.getValue() == null) kv.setValue(TAPIS_ENV_VAR_DEFAULT_VALUE);
    	}
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
    		
    		// Make sure the name has the right format using the right charset.
    		if (!_envKeyPattern.matcher(name).matches()) {
    			String msg = MsgUtils.getMsg("JOBS_INVALID_ENV_VAR_CHAR", name);
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
    /* validateSystemEnvVariables:                                                  */
    /* ---------------------------------------------------------------------------- */
    private void validateSystemEnvVariables(edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair sysKv)
     throws TapisImplException
    {
    	// FIXED variables must specify a concrete value.
    	if (sysKv.getInputMode() == edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueInputModeEnum.FIXED) {
    		if (TAPIS_ENV_VAR_UNSET.equals(sysKv.getValue())) {
    			String msg = MsgUtils.getMsg("JOBS_INVALID_FIXED_ENV_VAR", "system", sysKv.getKey(),
    					                     sysKv.getValue(), "concrete");
    			throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    		}
    	}
    	
    	// REQUIRED variables must specify a concrete value.
    	if (sysKv.getInputMode() == edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueInputModeEnum.REQUIRED) {
    		if (!TAPIS_ENV_VAR_UNSET.equals(sysKv.getValue())) {
    			String msg = MsgUtils.getMsg("JOBS_INVALID_REQUIRED_ENV_VAR", "system", sysKv.getKey(),
    					                     sysKv.getValue(), TAPIS_ENV_VAR_UNSET);
    			throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
    		}
    	}
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateFixedValue:                                                          */
    /* ---------------------------------------------------------------------------- */
    private void validateFixedValue(edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair appKv)
     throws TapisImplException
    {
    	// FIXED variables must specify a concrete value.
    	if (appKv.getInputMode() == edu.utexas.tacc.tapis.apps.client.gen.model.KeyValueInputModeEnum.FIXED) {
    		if (TAPIS_ENV_VAR_UNSET.equals(appKv.getValue())) {
    			String msg = MsgUtils.getMsg("JOBS_INVALID_FIXED_ENV_VAR", "application", appKv.getKey(),
    					                     appKv.getValue(), "concrete");
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
     * be null, in which case they are created on demand.  Both being null is not expected.  
     * 
     * Items in both lists can have their fields modified, the appKvList can also have 
     * items added to it before it is returned.   
     * 
     * The result is always a non-null list of merged environment variables that 
     * contains no duplicates.  The appKvList is used for both input and output.  The
     * result is the appKvList with non-overridden system environment variables appended.
     * 
     * @param appKvList env variables from application definition, can be null, can be modified
     * @param sysKvList env variables from system definition, can be null, read only
     * @return the non-null, merged appKvList of environment variables
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
    	normalizeEnvVariableLists(appKvList, sysKvList);
    	
    	// Validate the names of environment variables in each list.
    	validateEnvVariableNames(appKvList.stream().map(x -> x.getKey()).collect(Collectors.toList()), 
    			                 "application definition");
    	validateEnvVariableNames(sysKvList.stream().map(x -> x.getKey()).collect(Collectors.toList()), 
                				 "system definition");
    	
    	// Get a map of all system keys to values that are FIXED and therefore 
    	// cannot be overwritten. Populate the list of FIXED system key/value pairs 
    	// so that we can later check for attempts to override their values.
    	HashMap<String, edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair> fixedSysKVs = new HashMap<>();
        for (var sysKv : sysKvList) {
        	validateSystemEnvVariables(sysKv); // possible exceptions here
        	if (sysKv.getInputMode() == edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueInputModeEnum.FIXED)
        		fixedSysKVs.put(sysKv.getKey(), sysKv);
        }
        
        // Set of merged application keys.
        final HashSet<String> mergedKeys = new HashSet<>();
        
        // Iterate through the elements in the apps list.
        for (var appKv : appKvList) {
        	// Valide fixed variables are assigned a concrete value.
        	validateFixedValue(appKv); // possible exceptions here
        	
            // Make sure we are not improperly overriding a FIXED system env variable.
            var fixedSysKv = fixedSysKVs.get(appKv.getKey());
            if (fixedSysKv != null) detectFixedEnvVarOverride(appKv, fixedSysKv); // possible exceptions here
            
            // Check system list for same named env variable. Linear search is fine since
            // we don't expect many environment variables to be set in system definitions.
            edu.utexas.tacc.tapis.systems.client.gen.model.KeyValuePair sysKv = null;
            for (var kv : sysKvList) 
            	if (appKv.getKey().equals(kv.getKey())) {
            		sysKv = kv;
            		break;
            	}
            
            // Combine app and system definitions when possible. Note almost always the apps fields
            // override the systems fields, including cases where a concrete system value is unset
            // because the apps value is TAPIS_ENV_VAR_UNSET. See below for the one exception to
            // this involving a REQUIRED system env variable.  We concatenate the apps and systems 
            // descriptions when both exist.
            if (sysKv != null) {
            	// The only case where information from the system definition overrides 
            	// information in the apps definition is when the system's inputMode mode 
            	// is REQUIRED and the app's is INCLUDE_*.
            	if (sysKv.getInputMode() == edu.utexas.tacc.tapis.systems.client.gen.model.KeyValueInputModeEnum.REQUIRED
            	    && 
            	    (appKv.getInputMode() == KeyValueInputModeEnum.INCLUDE_BY_DEFAULT ||
            	     appKv.getInputMode() == KeyValueInputModeEnum.INCLUDE_ON_DEMAND))
            		appKv.setInputMode(KeyValueInputModeEnum.REQUIRED); // inputMode promotion
            	
            	// Combine descriptions if both exist.
            	if (!StringUtils.isBlank(sysKv.getDescription()))
            		if (StringUtils.isBlank(appKv.getDescription()))
            			appKv.setDescription(sysKv.getDescription());
            		else 
            			appKv.setDescription(appKv.getDescription() + "\n\n" + sysKv.getDescription());
            }
            
            // Record this key as part of result keys so that
            // system keys with the same name are skipped below.
            mergedKeys.add(appKv.getKey());
        }
        
        // Add any keys from the system list that are not already in app list.
    	for (var sysKv : sysKvList) {
    		// See if this variable has been overridden by an apps variable.
    		if (mergedKeys.contains(sysKv.getKey())) continue;

    		// Convert that non-null system inputMode to an app inputMode.
    		var sysInputMode = sysKv.getInputMode(); // normalized to not be null
    		var inputMode = KeyValueInputModeEnum.valueOf(sysInputMode.name());
          
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
    	
    	// Return the possibly modified list of app env variables.
    	return appKvList;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* validateScratchList:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** We do basic validation on each of the 3 list types, but we let SubmitContext
     * perform control character detection after all macro substitution has occurred.
     * 
     * @param scratchList a list of arg specs
     * @param argType one of the 3 list types processed by this method
     * @throws TapisImplException on validation error
     */
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
            
            // Check that the key doesn't contain prohibited characters.
            // This check means we don't have to check the argument key
            // after this for app, container or scheduler arguments. 
            if (argType == ArgTypeEnum.APP_ARGS) {
            	var arg = elem._jobArg.getArg();
            	var argName = elem._jobArg.getName();
            	if (StringUtils.isBlank(argName)) argName = "unnamed";
            	JobsApiUtils.hasDangerousCharacters(argType.name(), argName, arg); 
            } 
            else {
            	var parts = TapisUtils.splitIntoKeyValue(elem._jobArg.getArg());
            	if (parts.length > 0)
            		JobsApiUtils.hasDangerousCharacters(argType.name(), parts[0], parts[0]); 
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
