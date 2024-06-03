package edu.utexas.tacc.tapis.jobs.launchers;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.schedulers.JobScheduler;
import edu.utexas.tacc.tapis.jobs.schedulers.SlurmScheduler;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

import java.util.regex.Pattern;

public final class ZipLauncher
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipLauncher.class);
    // Regex to test that a string contains only characters 0-9 and there is at least one character
    private static final Pattern DIGITS_ONLY = Pattern.compile("\\d+");

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    private final JobScheduler scheduler;
    private final boolean isBatch;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipLauncher(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType)
     throws TapisException
    {
        // Set _job and _jobCtx
        super(jobCtx);
        // Set the scheduler properties as needed.
        // Currently only slurm supported. In future may move this to a method and use switch
        scheduler = (schedulerType != null) ? new SlurmScheduler(jobCtx) : null;
        isBatch = (schedulerType != null);
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
        
        // Run the command to launch the job
        var runCmd     = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsTrimmedString();
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(),
                                          _job.getUuid(), cmd, result, exitStatus);
            throw new JobException(msg);
        }

        // Note success.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(),
                    _job.getUuid(), result, exitStatus);
            _log.debug(msg);
        }

        // Get either the batch job id or the process id
        String pid;
        if (isBatch) {
            // Extract the batch job id.
            pid = scheduler.getBatchJobIdFromOutput(result, cmd);
        } else {
            // Remove line separator characters
            pid = result.trim().replaceAll("\\n", "");
            if (StringUtils.isBlank(pid)) pid = UNKNOWN_PROCESS_ID;
            if (_log.isDebugEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(),
                        _job.getUuid(), result, exitStatus);
                _log.debug(msg);
            }
            // If PID is not a number then it is an error. A pid must be a positive integer.
            if (!DIGITS_ONLY.matcher(pid).matches()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(),
                        _job.getUuid(), cmd, result, exitStatus);
                throw new TapisException(msg);
            }
        }

        // Save the process id or the unknown id string.
        _jobCtx.getJobsDao().setRemoteJobId(_job, pid);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getLaunchCommand:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getLaunchCommand()
            throws TapisException
    {
        // Create the command that changes the directory to the execution
        // directory and submits the job script.  The directory is expressed
        // as an absolute path on the system.
        String execDir = conditionalQuote(JobExecutionUtils.getExecDir(_jobCtx, _job));
        if (isBatch) {
            return String.format("cd %s;sbatch %s", execDir, JobExecutionUtils.JOB_WRAPPER_SCRIPT);
        }
        else {
            return String.format("cd %s;./%s", execDir, JobExecutionUtils.JOB_WRAPPER_SCRIPT);
        }
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
}
