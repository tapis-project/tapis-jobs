package edu.utexas.tacc.tapis.jobs.monitors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/*
 * Provide support for monitoring a ZIP job of type FORK.
 */
public class ZipNativeMonitor
 extends AbstractJobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipNativeMonitor.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The application return code as reported by monitor status command
    private String _exitCode;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipNativeMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
    {super(jobCtx, policy);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getExitCode:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Return the application exit code as reported by docker.  If no exit
     * code has been ascertained, null is returned. 
     * 
     * @return the application exit code or null
     */
    @Override
    public String getExitCode() {return _exitCode;}
    
    /* ---------------------------------------------------------------------- */
    /* monitorQueuedJob:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitorQueuedJob() throws TapisException
    {
        // The queued state is a no-op for forked jobs.
    }

    /* ---------------------------------------------------------------------- */
    /* queryRemoteJob:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Query the status of the job's process.
     * Runs the ps command to determine if the pid associated with the job is still active.
     * Process ID to monitor must be passed in as the only argument
     */
    @Override
    protected JobRemoteStatus queryRemoteJob(boolean active) throws TapisException
    {
        // For a forked ZIP container there is no difference between the active and inactive queries,
        // so there's no point in issuing a second (inactive) query if the first
        // one did not return a result.
        if (!active) return JobRemoteStatus.NULL;

        // Many log messages contain host. Create local variable for convenience and clarity.
        String host = _jobCtx.getExecutionSystem().getHost();

        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();

        // Get the command to query for status.
        String cmd = JobExecutionUtils.getZipStatusCommand(_job.getRemoteJobId());

        // Execute the query with retry capability.
        String result = null;
        int rc;
        try {
        	// Run the command and unpack results.
        	var resp = runJobMonitorCmd(runCmd, cmd);
        	rc = resp.rc;
                result = resp.result;
                if (StringUtils.isBlank(result)) result = "";
                result = result.trim();
        }
        catch (Exception e) {
            // Exception already logged.
            return JobRemoteStatus.NULL;
        }

        // If return code was zero assume job is still running.
        if (rc == 0) return JobRemoteStatus.ACTIVE;


        // Return code was non-zero. Assume process is done.
        // Log the result for debug and get the exitCode
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_ZIP_STATUS_EXIT", _job.getUuid(), host, cmd, rc, result);
            _log.debug(msg);
        }
        _exitCode = readExitCodeFile(runCmd);
        if (!SUCCESS_RC.equals(_exitCode)) return JobRemoteStatus.FAILED;
        else return JobRemoteStatus.DONE;
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
}
