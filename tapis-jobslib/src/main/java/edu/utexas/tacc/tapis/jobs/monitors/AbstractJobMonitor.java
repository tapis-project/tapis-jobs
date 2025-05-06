package edu.utexas.tacc.tapis.jobs.monitors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy.ReasonCode;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobCancelMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.recover.JobCancelRecoverMsg;
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
    /**
     * This is the actual monitor call. The initial status values determine how a remote status change is detected.
     * The only two valid initial status values are QUEUE and RUNNING.
     * Subclasses implement the abstract methods of this class to issue the actual query commands
     * on the execution system. Subclasses can also override the JobMonitor interface methods to take control of
     * monitoring before it reaches this method.
     * 
     * The general approach is to issue monitoring queries until the remote job's status changes.
     * The frequency and other limits placed on querying are determined by the policy settings.
     * When a change is detected monitoring ceases and control is returned to the caller.
     * When a limit is exceeded an exception is thrown to indicate that the job should be considered FAILED.
     * 
     * Depending on the policy settings, long intervals between monitor queries may cause the connection to
     * the execution system to be closed.
     * 
     * Under normal conditions, when a job terminates the remote job outcome and exit code are retrieved and used to
     * update the job in memory and in the database.
     */
    protected void monitor(final JobStatusType initialStatus) throws TapisException
    {
        // Sanity check.
        if (initialStatus != JobStatusType.QUEUED && initialStatus != JobStatusType.RUNNING)
        {
          _job.setCondition(JobConditionCode.JOB_INTERNAL_ERROR);
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "monitor", "initialStatus", initialStatus);
          throw new JobException(msg);
        }
        
        // We put all code in a try block so we can guarantee the job outcome will always be set during this phase.
        boolean exceptionThrown = false;
        boolean recoverableExceptionThrown = false;
        JobRemoteStatus remoteStatus = null;
        try
        {
            // Monitor the remote job as prescribed by the monitor policy until it reaches a terminal state or a
            // policy limit has been reached.
            boolean lastAttemptFailed = false; // no failed monitoring attempts yet!
            Long waitMillis;
            while (true)
            {
                remoteStatus = null; // reset on each iteration.
                // Before anything else check for async command. If PAUSE or CANCEL request has been received
                // then no need to monitor. Job state is changed and an exception is thrown.
                _jobCtx.checkCmdMsg();

// TODO/TBD                // Before waiting determine the remote status. May not need to monitor.
//                // For example, slurm may have timed out the remote job during the last polling interval.
//                remoteStatus = queryRemoteJobStatus();
//                // Check to see if we are ready to break out of our forever monitoring loop due to a change of status.
//                // NOTE that if JobRemoteStatus is DONE or FAILED then job remote outcome and result are recorded.
//                if (monitoringIsDone(initialStatus, remoteStatus)) break;

                // ------------------------- Consult Policy --------------------------
                // TODO/TBD returns null when
                //       ReasonCode.TOO_MANY_FAILURES - monitoring failed for more than one hour
                //          - NOTE: This does not include when recoverable exceptions are thrown, e.g. TapisSSHTimeoutException
                //                  In the case of recoverable exceptions the job goes into BLOCKED status and a message
                //                  is placed in the recovery queue.
                //            ????????? But queryRemoteJobStatus does not throw exception (including recoverable exceptions),
                //                      so it does not go to blocked?
                //          - NOTE: What does count is remoteStatus == JobRemoteStatus.EMPTY or JobRemoteStatus.NULL
                //       ReasonCode.TOO_MANY_ATTEMPTS - monitoring has exceeded allowed number of attempts.
                //         - Should never happen for current (the default) policy which has these for the default steps:
                //                steps.add(Pair.of(1,   1000L));   // 1 second
                //                steps.add(Pair.of(5,   10000L));  // 10 seconds
                //                steps.add(Pair.of(10,  60000L));  // 1 minute
                //                steps.add(Pair.of(100, 180000L)); // 3 minutes
                //                steps.add(Pair.of(100, 300000L)); // 5 minutes
                //                steps.add(Pair.of(-1,  600000L)); // 10 minutes forever
                //       ReasonCode.TIME_EXPIRED - Job has exceeded its max allowed time.
                //         - Only applies when monitoring while in RUNNING state
                //         - This includes an additional 10 minutes (MONITOR_TIMEOUT_EXTENSION_SECS) to try to avoid a
                //           race condition between the slurm timeout and the Tapis timeout.
                // Determine how long to wait before the next monitor attempt.
                waitMillis = _policy.millisToWait(lastAttemptFailed);
                if (waitMillis == null)
                {
                    // Either max time for job has been reached, or we are giving up on monitoring due to errors over a
                    //   long period of time (MonitorPolicy.DEFAULT_CONSECUTIVE_FAILURE_MINUTES = 60 minutes).
                    // The specific reason comes from the policy instance.
                    ReasonCode reasonCode = _policy.getReasonCode();
                    // Set the job outcome to failed.
                    _jobCtx.getJobsDao().setRemoteOutcome(_job, JobRemoteOutcome.FAILED);

                    // We want to update the finalMessage field in the jobCtx, which will be used to update the lastMessage field in the db. 
                    _jobCtx.setFinalMessage(MsgUtils.getMsg("JOBS_EARLY_TERMINATION", reasonCode.name()));
                    
                    // If time has expired, cancel jobs that are not automatically killed by their schedulers.
                    if (ReasonCode.TIME_EXPIRED.equals(reasonCode)) cancelExpiredJob();
                
                    // Signal that this job is kaput.
                    _job.setCondition(JobConditionCode.JOB_EXECUTION_MONITORING_TIMEOUT);
                    String msg = MsgUtils.getMsg("JOBS_MONITOR_EARLY_TERMINATION", getClass().getSimpleName(),
                                                 _job.getUuid(), reasonCode.name(),
                                                 _job.getRemoteOutcome().name());
                    throw new JobException(msg);
                }
                
                // Check again that we have not received an async PAUSE or CANCEL
                _jobCtx.checkCmdMsg();
            
                // Wait the policy-determined number of milliseconds; exceptions are logged.
                try {Thread.sleep(waitMillis);} 
                catch (InterruptedException e)
                {
                  if (_log.isDebugEnabled())
                  {
                    String msg = MsgUtils.getMsg("JOBS_MONITOR_INTERRUPTED", _job.getUuid(), getClass().getSimpleName());
                    _log.debug(msg);
                  }
                }
            
                // *** Async command check for PAUSE or CANCEL ***
                _jobCtx.checkCmdMsg();
            
                // ------------------------- Request Status --------------------------
                remoteStatus = queryRemoteJobStatus();

                // We keep the connection open if we might use it again soon.
                if (!_policy.keepConnection()) closeConnection();
                
                // --------------------- Process Failed Attempts ---------------------
                if (remoteStatus == JobRemoteStatus.EMPTY || remoteStatus == JobRemoteStatus.NULL)
                {
                  // Detect a possible initial queuing race condition. Let the policy determine whether we should retry.
                  if (_policy.retryForInitialQueuing()) continue;

                  // We were not able to get the status, and we are not attempting to deal with a queueing race condition.
                  // Record the failure.
                  lastAttemptFailed = true;
                  // Update job monitoring counter and persist record in database. An exception can be thrown here.
                  _jobCtx.getJobsDao().incrementRemoteStatusCheck(_job, false);
                  // Try again.
                  continue;
                }
                
                // Update job monitoring record, indicate remote check succeeded. An exception can be thrown here.
                _jobCtx.getJobsDao().incrementRemoteStatusCheck(_job, true);
                
                // --------------------- Process No-Change ---------------------------
                // If the remote job's status did not change then clear failure flag
                boolean noChange;
                if (initialStatus == JobStatusType.QUEUED)
                {
                  noChange = remoteStatus == JobRemoteStatus.QUEUED;
                }
                else
                {
                  noChange = remoteStatus == JobRemoteStatus.ACTIVE;
                }
                if (noChange)
                {
                  // Clear any failure history and continue normally.
                  lastAttemptFailed = false;
                  continue; // TODO remove, check is now done at start of loop
                }

                // --------------------- Process Termination -------------------------
                // Check to see if we are ready to break out of our forever monitoring loop.
                // NOTE that if JobRemoteStatus is DONE or FAILED then job remote outcome and result are recorded.
                if (monitoringIsDone(initialStatus, remoteStatus)) break;
            }
        }
        catch (Exception e)
        {
            // We need to do two things in this catch clause:
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
            // Make sure the job outcome is set. If we got here via a non-recoverable exception and the outcome
            // is not set we FAIL the job. Note that if archiveOnAppErr is true then archiving will be attempted
            // even though the job may still be running.
            if (exceptionThrown && !recoverableExceptionThrown && _job.getRemoteOutcome() == null) {
                // An exception could be thrown from here.
                try {_jobCtx.getJobsDao().setRemoteOutcome(_job, JobRemoteOutcome.FAILED);}
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
    
    /* ---------------------------------------------------------------------- */
    /* cancelExpiredJob:                                                      */
    /* ---------------------------------------------------------------------- */
    /** This method will attempt to asynchronously kill jobs whose maxMinutes 
     * have expired and their scheduler does not automatically kill them.  Any
     * type of FORK job falls into this category.
     * 
     * The actual cancel command is queued to separate threads or processes so
     * this thread will not experience SSH overhead.  We cover the case where
     * a job is in recovery, though there are windows of time when a job 
     * switches between recovery and active where cancel messages will be 
     * missed.  This is a best effort implementation. 
     */
    protected void cancelExpiredJob()
    {
    	// Currently, we only need to kill off FORK jobs because our BATCH
    	// scheduler (Slurm) will kill jobs it considers expired.  When other
    	// BATCH schedulers are introduced, this may need to be updated.
    	if (_job.getJobType() == JobType.BATCH) return;
    	var jobUuid = _job.getUuid();
    	
    	// Best effort attempt to kill job.
        try {
            // get a JobQueueManager instance and prep a JobCancelMsg
            JobQueueManager queueManager = JobQueueManager.getInstance();
            
            // set correlation id and sender
            JobCancelMsg jobCancelMsg = new JobCancelMsg();
            jobCancelMsg.jobuuid = jobUuid;
            jobCancelMsg.correlationId = jobUuid;
            jobCancelMsg.senderId = this.getClass().getSimpleName() + "-expiredMaxMinutes";
          
            // post a cmd to our job to cancel
            queueManager.postCmdToJob(jobCancelMsg, jobUuid);
            
            // Cover recovery queue too.
            JobCancelRecoverMsg jobCancelRecoverMsg = new JobCancelRecoverMsg();
            jobCancelRecoverMsg.jobUuid = jobUuid;
            jobCancelRecoverMsg.tenantId = _job.getTenant();
            jobCancelRecoverMsg.setSenderId(this.getClass().getSimpleName() + "-expiredMaxMinutes");
        
            // Post a cmd to a job that is in recovery
            queueManager.postRecoveryQueue(jobCancelRecoverMsg);

          } catch (JobException e) {
            String msg = MsgUtils.getMsg("JOBS_QMGR_POST_CANCEL", jobUuid);
            _log.error(msg, e);
          }
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /*
     * Determine remote status by calling a primary command and if necessary a secondary fallback command.
     * The implementing subclass chooses how to support each of the calls. Not all subclasses support/need
     * a secondary call. Slurm monitoring makes use of a secondary command.
     */
    private JobRemoteStatus queryRemoteJobStatus() throws TapisException
    {
      // The query method never returns null. The call is first made assuming the job is active.
      // If necessary, a second call is made assuming that the job has terminated.
      JobRemoteStatus remoteStatus = queryRemoteJob(true);
      if (remoteStatus == JobRemoteStatus.NULL || remoteStatus == JobRemoteStatus.EMPTY)
      {
        remoteStatus = queryRemoteJob(false);
      }
      return remoteStatus;
    }

    /*
     * Determine if we should break out of our forever monitoring loop due to a change of status.
     * It is time to break out if:
     *   Job was QUEUED and is now active or
     *   Job is DONE or FAILED
     * If job is DONE or FAILED then update remote outcome and result
     */
    private boolean monitoringIsDone(JobStatusType initialStatus, JobRemoteStatus remoteStatus) throws JobException
    {
      // Has the remote job moved off the queue and into an active execution state? If yes we are done.
      if (initialStatus == JobStatusType.QUEUED && remoteStatus == JobRemoteStatus.ACTIVE) return true;

      // --------------------- Process Termination -------------------------
      // Are we in a terminal state? If yes we record outcome and are done.
      if (remoteStatus == JobRemoteStatus.DONE || remoteStatus == JobRemoteStatus.FAILED)
      {
        // We are done monitoring. Record job remote outcome and result
        // The exit code is always set.
        String code = getExitCode();
        // Set the job outcome. Finished is our success code. If the job failed, then we skip archiving unless the user
        // explicitly specified that archiving should be performed even on failures.
        if (remoteStatus == JobRemoteStatus.DONE)
            _jobCtx.getJobsDao().setRemoteOutcomeAndResult(_job, JobRemoteOutcome.FINISHED, code);
        else if (_job.isArchiveOnAppError())
            _jobCtx.getJobsDao().setRemoteOutcomeAndResult(_job, JobRemoteOutcome.FAILED, code);
        else _jobCtx.getJobsDao().setRemoteOutcomeAndResult(_job, JobRemoteOutcome.FAILED_SKIP_ARCHIVE, code);
        if (_log.isDebugEnabled())
        {
          String msg = MsgUtils.getMsg("JOBS_MONITOR_FINISHED", getClass().getSimpleName(), _job.getUuid(),
                                       remoteStatus.name(), _job.getRemoteOutcome().name(), code);
          _log.debug(msg);
        }
        // We are done monitoring.
        return true;
      }
      // Continue monitoring
      return false;
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
