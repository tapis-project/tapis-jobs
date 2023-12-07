package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
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
	public void cancel() throws JobException, TapisException {
    	
    	var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
    	cancelZipJob(runCmd);
	}

    /* ---------------------------------------------------------------------- */
    /* cancelZipJob:                                                          */
    /* ---------------------------------------------------------------------- */
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
        String cmd = JobExecutionUtils.ZIP_RUN_KILL + _job.getRemoteJobId();

        // Stop the app process
        String result = null;
        try {
            int rc = runCmd.execute(cmd);
            result = runCmd.getOutAsString();
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
