package edu.utexas.tacc.tapis.jobs.launchers;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public final class ZipNativeLauncher
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipNativeLauncher.class);
    // Regex to test that a string contains only characters 0-9 and there is at least one character
    private static final Pattern DIGITS_ONLY = Pattern.compile("\\d+");

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipNativeLauncher(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Set _job and _jobCtx
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* launch:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    public void launch() throws TapisException
    {
        // Throttling adds a randomized delay on heavily used hosts.
        throttleLaunch();
        
        // Subclasses can override default implementation.
        String cmd = getLaunchCommand();
        
        // Log the command we are about to issue.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                       _job.getUuid(), cmd));
        
        // Start the container.
        var runCmd     = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        runCmd.logNonZeroExitCode();
        String result  = runCmd.getOutAsString();
        if (StringUtils.isBlank(result)) result = "";
        result = result.trim();

        // Get the process id
        String pid;
        if (exitStatus == 0) {
            // Remove whitespace and line separator characters
            pid = result.trim().replaceAll("\\n", "");
            if (StringUtils.isBlank(pid)) pid = UNKNOWN_PROCESS_ID;
            if (_log.isDebugEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                             _job.getUuid(), result, exitStatus);
                _log.debug(msg);
            }
            // If PID is not a number then it is an error. A pid must be a positive integer.
            if (!DIGITS_ONLY.matcher(pid).matches())
            {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(),
                        _job.getUuid(), cmd, result, exitStatus);
                throw new TapisException(msg);
            }
        } else {
            // Our one chance at launching the process failed with a non-communication
            // error, which we assume is unrecoverable, so we abort the job now.
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result, exitStatus);
            throw new TapisException(msg);
        }

        // Save the process id or the unknown id string.
        _jobCtx.getJobsDao().setRemoteJobId(_job, pid);
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
}
