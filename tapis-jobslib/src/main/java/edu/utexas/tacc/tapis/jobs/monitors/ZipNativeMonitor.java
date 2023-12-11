package edu.utexas.tacc.tapis.jobs.monitors;

import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.ZIP_STATUS_DONE;
import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.ZIP_STATUS_RUNNING;

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
     * Runs a shell script that outputs the status of the process.
     * Process ID to monitor must be passed in as the only argument
     * Script returns 0 if it can determine the status and 1 if there is an error
     * Status is echoed to stdout as either "RUNNING" or "DONE <exit_code>"
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

        // Get absolute path to execSystemExecDir. Command will be run from here.
        String execDir = JobExecutionUtils.getExecDir(_jobCtx, _job);

        // Get the command to query for status.
        String cmd = JobExecutionUtils.getZipStatusCommand(execDir, _job.getRemoteJobId());

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

        // If monitor script failed then log error message and return NULL status so retry will happen as per policy.
        if (rc != 0) {
            String msg = MsgUtils.getMsg("JOBS_ZIP_STATUS_EXIT_ERROR", _job.getUuid(), host, cmd, rc, result);
            _log.error(msg);
            return JobRemoteStatus.NULL;
        }

        // Status script succeeded. We should have gotten something.
        if (StringUtils.isBlank(result)) {
            String msg = MsgUtils.getMsg("JOBS_ZIP_STATUS_EXIT_EMPTY", _job.getUuid(), host, cmd);
            _log.error(msg);
            return JobRemoteStatus.EMPTY;
        }

        // The status that will be returned.
        JobRemoteStatus jobRemoteStatus;
        // If running then return status of ACTIVE
        if (result.startsWith(ZIP_STATUS_RUNNING)) {
            jobRemoteStatus = JobRemoteStatus.ACTIVE;
        } else if (!result.startsWith(ZIP_STATUS_DONE)) {
            // We did not recognize the output. This should not happen, so log an error and retry
            String msg = MsgUtils.getMsg("JOBS_ZIP_STATUS_OUTPUT_ERROR",
                    _job.getUuid(), host, cmd, rc, result);
            _log.error(msg);
            jobRemoteStatus = JobRemoteStatus.NULL;
        }
        else {
            // Status command succeeded and output from it seems OK so far.
            // It appears job has finished. Parse output to get exit code
            // We expect a status string that looks like "DONE <exit_code>".
            // Group 1 in the regex match isolates the parenthesized return code.
            var m = JobExecutionUtils.ZIP_STATUS_RESULT_PATTERN.matcher(result);
            if (m.matches()) {
                _exitCode = m.group(1);
                // Make sure the exit code is an integer. If not log a warning and fail the job
                // This can happen if user app places something other than an int as first line
                //   in file tapisjob.exitcode
                try {
                    Integer.parseInt(_exitCode);
                }
                catch (NumberFormatException e) {
                    String msg = MsgUtils.getMsg("JOBS_ZIP_STATUS_NOT_INT", _job.getUuid(), host, cmd, result);
                    _log.warn(msg);
                    return JobRemoteStatus.FAILED;
                }

                if (SUCCESS_RC.equals(_exitCode)) jobRemoteStatus = JobRemoteStatus.DONE;
                else jobRemoteStatus = JobRemoteStatus.FAILED;
            } else {
                String msg = MsgUtils.getMsg("JOBS_ZIP_STATUS_PARSE_ERROR",
                                             _job.getUuid(), host, cmd, result);
                _log.warn(msg);
                _exitCode = SUCCESS_RC;
                jobRemoteStatus = JobRemoteStatus.DONE;
            }
        }
        return jobRemoteStatus;
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
}
