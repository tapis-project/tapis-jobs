package edu.utexas.tacc.tapis.jobs.utils;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.recoverable.JobRecoverableException;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventCategoryFilter;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobRecoverMsg;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.notifications.client.NotificationsClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public final class JobUtils 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobUtils.class);
    
    // Job subscription category wildcard character.
    public static final String EVENT_CATEGORY_WILDCARD = "*";
    
    // Initialize the regex pattern that extracts the ID slurm assigned to the job.
    // The regex ignores leading and trailing whitespace and groups the numeric ID.
    private static final Pattern SLURM_RESULT_PATTERN = Pattern.compile("\\s*Submitted batch job (\\d+)\\s*");
    private static final Pattern LINE_PATTERN = Pattern.compile("\n");
    
    /* **************************************************************************** */
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* tapisify:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Wrap non-tapis exceptions in a TapisException keeping the same error message
     * in the wrapped exception.  Recoverable exception types are handled so that
     * their type is preserved. 
     * 
     * @param e any throwable that we might wrap in a tapis exception
     * @return a TapisException
     */
    public static TapisException tapisify(Throwable e)
    {
        return tapisify(e, null);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* tapisify:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Wrap non-tapis exceptions in a TapisException keeping the same error message
     * in the wrapped exception. Recoverable exception types are handled so that
     * their type is preserved. 
     * 
     * Note that JobAsyncCmdExceptions are unchecked exceptions that do not inherit
     * from TapisException and are designed to stop or pause job processing.  As 
     * such they should NOT be passed into this method.  If one is passed in, we
     * simply rethrow it so that its effect on job processing will still occur.
     * 
     * @param e any throwable that we might wrap in a tapis exception
     * @return a TapisException
     */
    public static TapisException tapisify(Throwable e, String msg)
    {
        // Dynamic binding is not used for static methods or in
        // overloaded methods, so we have to explicitly select 
        // the recoverable exception type method here.
        //
        // JobAsyncCmdExceptions should NOT be sent here but we 
        // handle that case anyway. 
        if (e instanceof JobRecoverableException) 
            return JobUtils.tapisify((JobRecoverableException)e, msg);
        else if (e instanceof JobAsyncCmdException) throw (JobAsyncCmdException)e;
        else if (e instanceof TapisDBConnectionException) return (TapisDBConnectionException)e;
        else return TapisUtils.tapisify(e, msg);
    }

    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* tapisify:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Wrap JobRecoverableException exceptions in a new version of themselves if
     * msg is non-null.  Otherwise, just return the original exception.
     * 
     * @param e any non-null JobRecoverableException that we might wrap in
     * @param msg the new message or null
     * @return a JobRecoverableException
     */
    private static JobRecoverableException tapisify(JobRecoverableException e, String msg)
    {
        // No need to change anything.  Note this method
        // must only be called when e is not null.
        if (msg == null) return e;
        
        // The result exception.
        JobRecoverableException recoveryException = null;
        
        // Create a new instance of the same tapis exeception type.
        Class<?> cls = e.getClass();
                  
        // Get the two argument (recoveryMsg, msg, cause) constructor that all 
        // JobRecoverableException subtypes implement.
        Class<?>[] parameterTypes = {JobRecoverMsg.class, String.class, Throwable.class};
        Constructor<?> cons = null;
        try {cons = cls.getConstructor(parameterTypes);}
            catch (Exception e2) {
                String msg2 = MsgUtils.getMsg("TAPIS_REFLECTION_ERROR", cls.getName(), 
                                              "getConstructor", e.getMessage());
                _log.error(msg2, e2);
            }
                  
        // Use the constructor to assign the result variable.
        if (cons != null) {
            try {recoveryException = (JobRecoverableException) cons.newInstance(
                                        e.jobRecoverMsg, msg == null ? e.getMessage() : msg, e);}
            catch (Exception e2) {
                String msg2 = MsgUtils.getMsg("TAPIS_REFLECTION_ERROR", cls.getName(), 
                                              "newInstance", e.getMessage());
                _log.error(msg2, e2);
            }
        } 
                  
        // If unable to create a wrapper exception just return the original one since
        // JobRecoverableException cannot be used as a fallback since it's abstract. 
        if (recoveryException == null) recoveryException = e;
        
        // Never null.
        return recoveryException;
    }
    
    /* **************************************************************************** */
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getSlurmId:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Extract the slurm id from the output of an sbatch command.  If unable to find
     * the id, this method throws an exception.  The slurm id is usually the last line
     * of output, but to accommodate installations the write other information after 
     * the sbatch ouput, we do a reverse search on the output lines.  The first line
     * that matches sbatch output text is the one we run with.  
     * 
     * @param job the job issuing the sbatch command
     * @param output the sbatch stdout text
     * @param cmd the actual sbatch command
     * @return the id slurm assigned to this job
     * @throws JobException if the slurm id cannot be recovered
     */
    public static String getSlurmId(Job job, String output, String cmd) 
     throws JobException
    {
        // We have a problem if the result is not the slurm id.
        if (StringUtils.isBlank(output)) {
            String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_NO_RESULT",  
                                         job.getUuid(), cmd);
            throw new JobException(msg);
        }
        
        // There may be banner information in the remote result, which we'll
        // harmlessly inspect in only error cases.  We strip whitespace 
        // from the output and break it up into individual lines.
        output = output.strip();
        String[] lines = LINE_PATTERN.split(output);
        
        // Iterate in reverse order through the lines of output
        // looking for the first slurm result match.
        String slurmId = null;
        for (int i = lines.length - 1; i >= 0; i--) {
        	// Get the current candidate.
        	var line = lines[i];
        	
        	// Look for the success message
        	Matcher m = SLURM_RESULT_PATTERN.matcher(line);
        	if (!m.matches()) continue; // not found
        
        	// Grab the slurm id.
        	int groupCount = m.groupCount();
        	if (groupCount < 1) {
        		String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_INVALID_RESULT",  
                                         	 job.getUuid(), output);
        		throw new JobException(msg);
        	} 
        	
        	// Group 1 contains the slurm ID.
       		slurmId = m.group(1);
       		break;
        }
        
        // Did we find the result line?
    	if (slurmId == null) {
    		String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_INVALID_RESULT",  
                                     	 job.getUuid(), output);
    		throw new JobException(msg);
    	}
    	
        return slurmId;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getLastLine:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Get all characters after the last newline character is a string.  The string
     * must be non-null and must already be trimmed of leading and trailing whitespace.
     * This method is useful in stripping the banner information from the output of
     * remote commands. 
     * 
     * @param s the remote result string
     * @return the last line of the string
     */
    public static String getLastLine(String s)
    {
        // The input is a non-null, trimmed string so
        // a non-negative index must be at least one
        // character from the end of the string.
        int index = s.lastIndexOf('\n');
        if (index < 0) return s;
        return s.substring(index + 1);
    }


    /* ---------------------------------------------------------------------------- */
    /* getNotificationsClient:                                                      */
    /* ---------------------------------------------------------------------------- */
    /** Get a new or cached Notifications service client.  The input parameter is
     * the tenant id of the administrator tenant at the local site.    
     * 
     * @param jobTenantId - the tenant id of the job
     * @return the client
     * @throws TapisImplException
     */
    public static NotificationsClient getNotificationsClient(String jobTenantId) 
     throws TapisException
    {
        // Get the application client for this user@tenant.
        NotificationsClient client = null;
        var user = TapisConstants.SERVICE_NAME_JOBS;
        try {
            client = ServiceClients.getInstance().getClient(
                     user, jobTenantId, NotificationsClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Notifications", 
                                         jobTenantId, user);
            throw new TapisException(msg, e);
        }

        return client;
    }

    /* ---------------------------------------------------------------------------- */
    /* makeNotifTypeFilter:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Create a token that can be used as an event type or a subscription typeFilter.
     * The generic token contains 3 components:
     * 
     *     service.category.eventDetail
     * 
     * For job subscriptions, concrete tokens always look like one of these:
     * 
     *     jobs.<jobEventCategoryFilter>.*
     *     jobs.*.*   // when ALL category is used
     *     
     * @param jobEventType the 2nd component in a job subscription type filter
     * @return the 3 part type filter string
     */
    public static String makeNotifTypeFilter(JobEventCategoryFilter filter, String detail)
    {
        String f = filter.name();
        if (f.equals(JobEventCategoryFilter.ALL.name())) f = EVENT_CATEGORY_WILDCARD; 
        return TapisConstants.SERVICE_NAME_JOBS  + "." + f + "." + detail;
    }

    /* ---------------------------------------------------------------------------- */
    /* makeNotifTypeToken:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Create a token that can be used as an event type or a subscription typeFilter.
     * The generic token contains 3 components:
     * 
     *     service.category.eventDetail
     * 
     * For job events, the tokens always look like this:
     * 
     *     jobs.<jobEventType>.<eventDetail>
     *     
     * @param jobEventType the 2nd component in a job subscription type filter
     * @return the 3 part type filter string
     */
    public static String makeNotifTypeToken(JobEventType jobEventType, String detail)
    {
        return TapisConstants.SERVICE_NAME_JOBS + "." + jobEventType.name() + "." + detail;
    }

    /* ---------------------------------------------------------------------------- */
    /*  generateEnvVarFileContentForZip                                             */
    /* ---------------------------------------------------------------------------- */
    /** Generate file content of env variables runtime type of ZIP
     *  Include export and double quotes around value so whitespace is correctly handled
     */
    public static String generateEnvVarFileContentForZip(List<Pair<String, String>> env)
    {
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);

        // Write each assignment to the buffer.
        for (var pair : env) {
            // Always use export <key>= to start.
            buf.append("export ").append(pair.getKey()).append("=");
            // Only append the value if it is set.
            var value = pair.getValue();
            if (value != null && !value.isEmpty()) {
                buf.append(TapisUtils.conditionalQuote(value));
            }
            buf.append("\n");
        }

        return buf.toString();
    }

    /* ---------------------------------------------------------------------------- */
    /*  generateEnvVarFileContentForDocker                                          */
    /* ---------------------------------------------------------------------------- */
    /** Generate file content of env variables runtime type of ZIP
     */
    public static String generateEnvVarFileContentForDocker(List<Pair<String, String>> env)
    {
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);

        // Write each assignment to the buffer.
        for (var pair : env) {
            // The key always starts the line.
            buf.append(pair.getKey());

            // Are we going to use the short or long form?
            // The short form is just the name of an environment variable
            // that docker will import into the container ONLY IF it exists
            // in the environment from which docker is called.  The long
            // form is key=value.  Note that we don't escape characters in
            // the value because we write to a file not the command line.
            var value = pair.getValue();
            if (value != null && !value.isEmpty()) {
                // The long form forces an explicit assignment.
                buf.append("=");
                buf.append(pair.getValue());
            }
            buf.append("\n");
        }

        return buf.toString();
    }

    /* ---------------------------------------------------------------------------- */
    /*  generateEnvVarFileContentForSingularity                                     */
    /* ---------------------------------------------------------------------------- */
    /** Generate file content of env variables runtimes type of Singularity
     *  For FORK (RUN or START) and BATCH.
     *  FORK should use insertExport=false and BATCH should use insertExport=false
     */
    public static String generateEnvVarFileContentForSingularity(List<Pair<String, String>> env,
                                                                 boolean insertExport)
    {
        // This should never happen since tapis variables are always specified.
        if (env.isEmpty()) return null;

        // Create the key=value records, one per line.
        // Get a buffer to accumulate the key/value pairs.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);

        // Create a list of key=value assignment, each followed by a new line.
        for (var v : env) {
            if (insertExport) buf.append("export ");
            buf.append(v.getLeft());
            buf.append("=");
            buf.append(TapisUtils.conditionalQuote(v.getRight()));
            buf.append("\n");
        }
        return buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarCommandLineArgs:                                         */
    /* ---------------------------------------------------------------------- */
    /** Create the string of key=value pairs separated by commas.  It's assumed
     * that the values are NOT already double quoted.
     *
     * NOTE: We always double quote the value whether or not it contains
     *       dangerous characters, which is different that most other cases
     *       of environment variable construction.  Most of the time we use
     *       TapisUtils.conditionalQuote(), which will only double quote when
     *       dangerous or space characters are present.
     *
     * @param pairs NON-EMPTY list of pair values, one per occurrence
     * @return the string that contains all assignments
     */
    public static  String generateEnvVarCommandLineArgs(List<Pair<String,String>> pairs)
    {
        // Get a buffer to accumulate the key/value pairs.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);

        // Create the string " --env key=value[,key=value]".
        boolean first = true;
        for (var v : pairs) {
            if (first) {buf.append(" --env "); first = false;}
            else buf.append(",");
            buf.append(v.getLeft());
            buf.append("=");
            buf.append(TapisUtils.safelyDoubleQuoteString(v.getRight()));
        }
        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* killJob:                                                               */
    /* ---------------------------------------------------------------------- */
    /**
     * Cancel a job using pkill command. Use kill command as a fallback.
     * Future enhancements to consider
     *   1. First check to see if job has completed. If completed set status to FINISHED or ERROR state.
     *   2. Execute gentle kill, kill -15 (15 = SIGTERM, the default if no signal given)
     *         - give process some time to shut down
     *         - check status, if still running, then use kill -9
     *         - either way, set status to CANCELLED
     */
    public static void killJob(TapisRunCommand runCmd, String jobUUID, String jobRemoteJobId,
                               JobExecutionContext jobCtx)
    {
        String msg;
        // Info for log messages.
        // Since these are only for logging, ignore any exceptions. We still want to cancel the job.
        String host = null, execSysId = null;
        try {
            host = jobCtx.getExecutionSystem().getHost();
            execSysId = jobCtx.getExecutionSystem().getId();
        }
        catch (Exception e) { /* Ignoring exceptions */}

        // If job not yet launched then no pid so nothing to do. Log message.
        if (StringUtils.isBlank(jobRemoteJobId)) {
            msg = MsgUtils.getMsg("JOBS_CANCEL_KILL_NO_PID", jobUUID, execSysId, host);
            _log.debug(msg);
            return;
        }

        // Get the initial command to terminate the process
        String cmd = String.format(JobExecutionUtils.PKILL_9_CMD_FMT, jobRemoteJobId);
        // Attempt to stop the process and it's sub-processes
        String result;
        int rc;
        try {
            rc = runCmd.execute(cmd);
            result = runCmd.getOutAsTrimmedString();
            if (rc != 0) {
                // Initial pkill may not have worked. Log a message and try the backup kill command
                msg = MsgUtils.getMsg("JOBS_CANCEL_KILL_ERROR1", jobUUID, execSysId, host, cmd, rc, result);
                _log.debug(msg);
                cmd = String.format(JobExecutionUtils.KILL_9_CMD_FMT, jobRemoteJobId);
                rc = runCmd.execute(cmd);
                result = runCmd.getOutAsTrimmedString();
                // If process has finished then kill will return an error, but that is OK.
                // Message returned by kill command might look something like this:
                //  "bash: line 0: kill: (2264066) - No such process"
                if (rc != 0) {
                    msg = MsgUtils.getMsg("JOBS_CANCEL_KILL_ERROR1", jobUUID, execSysId, host, cmd, rc, result);
                    _log.debug(msg);
                    return;
                }
            }
        }
        catch (Exception e) {
            msg = MsgUtils.getMsg("JOBS_CANCEL_KILL_ERROR2", jobUUID, execSysId, host);
            _log.error(msg, e);
            return;
        }
        // Record the successful cancel of the process.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_CANCEL_KILLED",jobUUID, host, cmd, rc, result));
    }
}
