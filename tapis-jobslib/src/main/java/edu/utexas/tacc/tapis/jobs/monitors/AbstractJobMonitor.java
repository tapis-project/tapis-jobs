package edu.utexas.tacc.tapis.jobs.monitors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisSSHChannelException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisRecoverableException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** This clas implements the main monitoring loop when the job is both in the 
 * QUEUE and RUNNING states.  Connections to the execution system are closed
 * if the policy so dictates or if the remote job has reached a terminal state
 * and no more monitoring will occur. 
 * 
 * @author rcardone
 */
abstract class AbstractJobMonitor
 implements JobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobMonitor.class);

    // Zero is recognized as the application success code.
    protected static final String SUCCESS_RC = "0";
    
    // The number of times we'll try a new connection after a channel error.
    private static final int CHANNEL_ERROR_RETRIES = 1;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    protected final MonitorPolicy       _policy;
    protected final JobExecutionContext _jobCtx;
    protected final Job                 _job;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractJobMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
    {
        _policy = policy;
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* monitorQueuedJob:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitorQueuedJob() throws TapisException
    {
        monitor(JobStatusType.QUEUED);
    }

    /* ---------------------------------------------------------------------- */
    /* monitorRunningJob:                                                     */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitorRunningJob() throws TapisException
    {
        monitor(JobStatusType.RUNNING);
    }

    /* ---------------------------------------------------------------------- */
    /* closeConnection:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public void closeConnection() 
    {
        _jobCtx.closeExecSystemConnection();
    }
    
    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* readExitCodeFile:                                                      */
    /* ---------------------------------------------------------------------- */
    protected String readExitCodeFile(TapisRunCommand runCmd)
    {
        // Initialize output to default to no error.
        String exitcode = SUCCESS_RC;

        // Create the command that returns the exit code contents if the
        // file exists in the job's output directory.  There's not much we
        // can do if we encounter an error here.
        String cmd = null;
        try {
            var fm = _jobCtx.getJobFileManager();
            var filepath = fm.makeAbsExecSysOutputPath(JobExecutionUtils.JOB_OUTPUT_EXITCODE_FILE);
            cmd = "cat " + filepath;
        } catch (Exception e) {
            _log.error(e.getMessage(), e);
            return exitcode;
        }

        // Issue the command.
        String result = null;
        try {
            int rc = runCmd.execute(cmd);
            runCmd.logNonZeroExitCode();
            result = runCmd.getOutAsString();
        }
        catch (Exception e) {
            _log.error(e.getMessage(), e);
            return exitcode;
        }

        // See if we even found the file.
        if (StringUtils.isBlank(result)) return exitcode;
        result = result.trim();
        if (result.isEmpty() || result.startsWith("cat") || result.contains("No such"))
            return exitcode;

        // We assign exitcode as long as the result is an integer.
        try {Integer.valueOf(result); exitcode = result;}
        catch (Exception e) {}

        return exitcode;
    }

    /* ---------------------------------------------------------------------- */
    /* allowEmptyResult:                                                      */
    /* ---------------------------------------------------------------------- */
    /** This method determines whether the result of a monitoring 
     * request can be empty or not.  When true is returned, empty
     * results from a monitor query do not cause an exception to
     * be thrown.  When false is returned, the remote client code
     * considers an empty response to be an error and throws an
     * exception.
     * 
     * @return true to allow empty monitoring results, false otherwise
     */
    protected boolean allowEmptyResult() {return false;}
    
    /* ---------------------------------------------------------------------- */
    /* queryRemoteJob:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Return a non-null remote job status value.  Subclasses must issue the
     * query on the ssh connection to the execution system.  The active flag
     * allows the query to be tailored to the state of the remote job.  Specify 
     * active=true for any state before the job terminates; specify active=false
     * after the job has terminated.
     * 
     * @param active true for pre-termination, false for post-termination
     * @return non-null remote job status
     */
    protected abstract JobRemoteStatus queryRemoteJob(boolean active) throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* cleanUpRemoteJob:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Offer subclasses a way to clean up a job that the higher level monitor
     * loop has decided to end monitoring even though the job has not been 
     * declared as terminal.  The default implementation does nothing.
     */
    protected void cleanUpRemoteJob() {}
    
    /* ---------------------------------------------------------------------- */
    /* monitor:                                                               */
    /* ---------------------------------------------------------------------- */
    /** This is the actual monitor call.  The initial status values determine
     * how a remote status change is detected.  The only two valid initial status 
     * values are QUEUE and RUNNING.  Subclasses implement the abstract methods
     * of this class to issue the actual query commands on the execution system.
     * Subclasses can also override the JobMonitor interface's methods to take
     * control of monitoring before it reaches this method.
     * 
     * The general approach is to issue monitoring queries until the remote job's 
     * status changes.  The frequency and other limits placed on querying are
     * the determined by the policy settings.  When a change is detected monitoring 
     * ceases and control is returned to the caller.  When a limit is exceeded an
     * exception is thrown indicating to the caller that the job should be 
     * considered FAILED.
     * 
     * Depending on the policy settings, long intervals between monitor queries
     * may cause the connection to the execution system to be closed.
     * 
     * Under normal conditions, when a job terminates the remote job outcome
     * and exit code are retrieved and used to update the job in memory and 
     * in the database.  
     */
    protected void monitor(final JobStatusType initialStatus)
     throws TapisException
    {
        // Sanity check.
        if (initialStatus != JobStatusType.QUEUED && initialStatus != JobStatusType.RUNNING)
        {    
        	_job.setCondition(JobConditionCode.JOB_INTERNAL_ERROR);
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "monitor", "initialStatus", initialStatus);
            throw new JobException(msg);
        }
        
        // We put all code inside the try block so that we can guarantee the job 
        // outcome will always be set during this phase.
        boolean exceptionThrown = false;
        boolean recoverableExceptionThrown = false;
        JobRemoteStatus remoteStatus = null;
        try {
            // Monitor the remote job as prescribed by the monitor policy until
            // it reaches a terminal state or a policy limit has been reached.
            boolean lastAttemptFailed = false; // no failed monitoring attempts yet!
            while (true) 
            {
                // ------------------------- Consult Policy --------------------------
                remoteStatus = null; // reset on each iteration.
                Long waitMillis = _policy.millisToWait(lastAttemptFailed);
                if (waitMillis == null) {
                    // Set the job outcome so that archiving is skipped since the job may
                    // still be running or start running at some point in the future.
                    _jobCtx.getJobsDao().setRemoteOutcome(_job, JobRemoteOutcome.FAILED_SKIP_ARCHIVE);
                    
                    // We want to update the finalMessage field in the jobCtx, which will be used to update the lastMessage field in the db. 
                    String finalMessage = MsgUtils.getMsg("JOBS_EARLY_TERMINATION", _policy.getReasonCode().name());
                    _jobCtx.setFinalMessage(finalMessage);
                
                    // Signal that this job is kaput.
                    _job.setCondition(JobConditionCode.JOB_EXECUTION_MONITORING_TIMEOUT);
                    String msg = MsgUtils.getMsg("JOBS_MONITOR_EARLY_TERMINATION", getClass().getSimpleName(),
                                                 _job.getUuid(), _policy.getReasonCode().name(),
                                                 _job.getRemoteOutcome().name());
                    throw new JobException(msg);
                }
                
                // *** Async command check ***
                _jobCtx.checkCmdMsg();
            
                // Wait the policy-determined number of milliseconds; exceptions are logged.
                try {Thread.sleep(waitMillis);} 
                    catch (InterruptedException e) {
                        if (_log.isDebugEnabled()) {
                            String msg = MsgUtils.getMsg("JOBS_MONITOR_INTERRUPTED", _job.getUuid(), 
                                                         getClass().getSimpleName());
                            _log.debug(msg);
                        }
                    }
            
                // *** Async command check ***
                _jobCtx.checkCmdMsg();
            
                // ------------------------- Request Status --------------------------
                // The query method never returns null.  The call is first made assuming the job
                // is active.  If necessary, a second call is made assuming that the job has 
                // terminated.  The implementing subclass chooses how to support each of the calls.
                remoteStatus = queryRemoteJob(true);
                if (remoteStatus == JobRemoteStatus.NULL || remoteStatus == JobRemoteStatus.EMPTY)
                    remoteStatus = queryRemoteJob(false);
                
                // We keep the connection open if we might use it again soon.
                if (!_policy.keepConnection()) closeConnection();
                
                // --------------------- Process Failed Attempts ---------------------
                // Detect a possible initial queuing race condition and
                // let the policy determine whether we should retry.
                if (remoteStatus == JobRemoteStatus.EMPTY || remoteStatus == JobRemoteStatus.NULL) 
                    if (_policy.retryForInitialQueuing()) continue;
                
                // If the status problem hasn't cleared up by now, we assume that the problem
                // retrieving the status is not due to an initial race condition but some
                // other issue.  This code saves the attempt information in the database.
                if (remoteStatus == JobRemoteStatus.EMPTY || remoteStatus == JobRemoteStatus.NULL) 
                {
                    // Let's record this failure attempt.
                    lastAttemptFailed = true;
                    
                    // Update the job monitoring counter and its persistent 
                    // record in the database. An exception can be thrown here.
                    final boolean success = false;
                    _jobCtx.getJobsDao().incrementRemoteStatusCheck(_job, success);
                    
                    // Try again.
                    continue;
                }
                
                // The monitoring command did not fail, so we can update the job monitoring counter
                // and its persistent record in the database now. An exception can be thrown here.
                final boolean success = true;
                _jobCtx.getJobsDao().incrementRemoteStatusCheck(_job, success);
                
                // --------------------- Process No-Change ---------------------------
                // Is the remote job's status still compatible with our initial status? 
                boolean noChange;
                if (initialStatus == JobStatusType.QUEUED) noChange = remoteStatus == JobRemoteStatus.QUEUED;
                  else noChange = remoteStatus == JobRemoteStatus.ACTIVE;
                if (noChange) {
                    // Clear any failure history and continue normally.
                    lastAttemptFailed = false;
                    continue;
                }
                
                // --------------------- Process Advancement -------------------------
                // Has the remote job moved off the queue and into an active execution state?
                if (initialStatus == JobStatusType.QUEUED && remoteStatus == JobRemoteStatus.ACTIVE) 
                    break;
                
                // --------------------- Process Termination -------------------------
                // Are we in a terminal state?
                if (remoteStatus == JobRemoteStatus.DONE || remoteStatus == JobRemoteStatus.FAILED) 
                {
                    // The exit code is always set.
                    var code = getExitCode();
                    
                    // Set the job outcome. Finished is our success code. If the job failed,
                    // then we skip archiving unless the user explicitly specified that 
                    // archiving should be performed even on failures.
                    if (remoteStatus == JobRemoteStatus.DONE) 
                        _jobCtx.getJobsDao().setRemoteOutcomeAndResult(_job, JobRemoteOutcome.FINISHED, code);
                    else if (_job.isArchiveOnAppError())
                        _jobCtx.getJobsDao().setRemoteOutcomeAndResult(_job, JobRemoteOutcome.FAILED, code);
                    else _jobCtx.getJobsDao().setRemoteOutcomeAndResult(_job, JobRemoteOutcome.FAILED_SKIP_ARCHIVE, code);
                    
                    // Record the outcome.  
                    if (_log.isDebugEnabled()) {
                        String msg = MsgUtils.getMsg("JOBS_MONITOR_FINISHED", getClass().getSimpleName(),
                                                     _job.getUuid(), remoteStatus.name(),
                                                     _job.getRemoteOutcome().name(), code);
                        _log.debug(msg);
                    }
                    
                    // We're done monitoring.
                    break;
                }
            }
        }
        catch (Exception e) {
            // We need to do two things in this catch clause:
            //   
            //  1. Record that an exception happened.
            //  2. Record whether the exception is recoverable or not.
            _log.error(e.getMessage(), e);
                
            // Are we dealing with a recoverable condition?  Connection problems are always
            // treated as recoverable, see the recovery code in TenantQueueProcessor.
            exceptionThrown = true;
            
            // See if a recoverable exception was thrown.
            var found = TapisUtils.findInChain(e, TapisRecoverableException.class);
            if (found != null) 
            {
                // Do not set the outcome when monitoring will resume in the future.
                recoverableExceptionThrown = true;
            } 
            
            throw e;
        }
        finally {
            // Make sure the job outcome is set.  If we got here via an exception,
            // the outcome is not set.  We set it so that archiving is not performed
            // since the timing of the archiving cannot be coordinated with the job 
            // if it is or will be executing.  
            if (exceptionThrown && !recoverableExceptionThrown && _job.getRemoteOutcome() == null) {
                // An exception could be thrown from here.
                try {_jobCtx.getJobsDao().setRemoteOutcome(_job, JobRemoteOutcome.FAILED_SKIP_ARCHIVE);}
                    catch (Exception e) {
                        // Log error and continue.
                        _log.error(e.getMessage(), e);
                    }
                
                // Record the outcome. The remote status parameter reflects the last value set, which could be null.
                if (_log.isDebugEnabled()) {
                    String outcome = _job.getRemoteOutcome() == null ? "null" : _job.getRemoteOutcome().name();
                    String msg = MsgUtils.getMsg("JOBS_MONITOR_FINISHED", getClass().getSimpleName(),
                                                 _job.getUuid(), remoteStatus, outcome, null);
                    _log.debug(msg);
                }
            }
            
            // Close the connection if the job has terminated.
            if (_job.getRemoteOutcome() != null) closeConnection();
            
            // Give the specific monitor a chance to clean up.
            if (exceptionThrown || initialStatus == JobStatusType.RUNNING) cleanUpRemoteJob();
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* runJobMonitorCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Execute the job monitoring command with one reconnection try if we get
     * an error on the channel.
     * 
     * @param runCmd the run command object
     * @param cmd the actual command string to run
     * @return the result package
     * @throws TapisException if unable to run the command
     */
    protected JobMonitorCmdResponse runJobMonitorCmd(TapisRunCommand runCmd, String cmd)
      throws TapisException
    {
    	// We try to reconnect once if we encounter a channel error.
    	JobMonitorCmdResponse resp = null;
    	for (int i = 0; i <= CHANNEL_ERROR_RETRIES; i++) 
    	{
    		// Query the container.
    		resp = new JobMonitorCmdResponse();
    		try {
    			// Issue the command and get the result.
    			resp.rc = runCmd.execute(cmd);
    			runCmd.logNonZeroExitCode();
    			resp.result = runCmd.getOutAsString();
    		}
    		// Retry monitor command after reconnecting 
    		catch (TapisSSHChannelException e) {
    			_log.error(e.getMessage(), e);
    			
    			// Have we maxed out the retries?
    			if (i >= CHANNEL_ERROR_RETRIES) throw e;
    			
    			// Log intention to retry.
    			_log.debug(MsgUtils.getMsg("JOBS_MONITOR_RECONNECTING", 
    					                   _job.getUuid(), _job.getExecSystemId()));
    			
    			// Close the current connection and try to reconnect.
    			// We can do this here for monitoring because we don't share
    			// the connection outside of this class and its subclasses.
    			// Even if two jobs for the same user are running on the same 
    			// machine concurrently they will have different connections
    			// for monitoring--not the most efficient but convenient in 
    			// this case.
    			_jobCtx.closeExecSystemConnection();
    			try {_jobCtx.getExecSystemTapisSSH();}
    				catch (Exception e1) {
    					_log.error(e.getMessage(), e1);
    					throw e1;
    				}
    			
    			// Run the command again.
    			continue;
    		}
    		catch (Exception e) {
    			_log.error(e.getMessage(), e);
    			throw e;
    		}
    	}
    	
    	// Response will never be null.
    	return resp;
    }
    
    /* ********************************************************************** */
    /*                          class JobMonitorCmdResponse                   */
    /* ********************************************************************** */
    protected static final class JobMonitorCmdResponse
    {
    	public int rc;
    	public String result;
    }
}
