package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipNativeCanceler extends AbstractJobCanceler{

	/* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipNativeCanceler.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipNativeCanceler(JobExecutionContext jobCtx)
    {
        // Set _jobCtx, _job
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* cancel:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
	public void cancel() throws TapisException {
    	
    	var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
    	cancelZipJob(runCmd);
	}

    /* ---------------------------------------------------------------------- */
    /* cancelZipJob:                                                          */
    /* ---------------------------------------------------------------------- */
    /*
     * Future enhancements to consider
     *   1. First check to see if job has completed. If completed set status to FINISHED or ERROR state.
     *   2. Execute gentle kill, kill -15 (15 = SIGTERM, the default if no signal given)
     *         - give process some time to shut down
     *         - check status, if still running, then use kill -9
     *         - either way, set status to CANCELLED
     */
    private void cancelZipJob(TapisRunCommand runCmd)
    {
        // Info for log messages.
        // Since these are only for logging, ignore any exceptions. We still want to cancel the job.
        String host = null, execSysId = null;
        try {
            host = _jobCtx.getExecutionSystem().getHost();
            execSysId = _jobCtx.getExecutionSystem().getId();
        }
        catch (Exception e) { /* Ignoring exceptions */}

        // Get the command text to terminate the app process launched for a ZIP runtime.
        String cmd = String.format(JobExecutionUtils.ZIP_KILL_CMD_FMT, _job.getRemoteJobId());

        // Stop the app process
        String result;
        try {
            int rc = runCmd.execute(cmd);
            result = runCmd.getOutAsString();
            if (StringUtils.isBlank(result)) result = "";
            result = result.trim();
            // If process has finished this will return an error, but that is OK.
            // Message returned by kill command might look something like this:
            //  "bash: line 0: kill: (2264066) - No such process"
            // Simply log a message. This is what other runtime types do.
            if (rc != 0) {
                String msg = MsgUtils.getMsg("JOBS_ZIP_KILL_ERROR1", _job.getUuid(), execSysId, host, result, cmd);
                _log.error(msg);
                return;
            }
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_ZIP_KILL_ERROR2", _job.getUuid(), execSysId, host);
            _log.error(msg, e);
            return;
        }

        // Record the successful cancel of the process.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_ZIP_KILLED",_job.getUuid()));
    }
}
