package edu.utexas.tacc.tapis.jobs.launchers;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.alwaysSingleQuote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Launch a job using singularity run from a wrapper script that returns the
 * PID of the spawned background process.
 * 
 * @author rcardone
 */
public final class SingularityRunSlurmLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmLauncher.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunSlurmLauncher(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Create and populate the docker command.
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
        
        // -------------------- Launch Container --------------------
        // Subclasses can override default implementation.
        String cmd = getLaunchCommand();
        
        // Log the command we are about to issue.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                       _job.getUuid(), cmd));
        
        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        
        // Start the container and retrieve the pid.
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsString();
        
        // Let's see what happened.
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR2", getClass().getSimpleName(),
                                         _job.getUuid(), cmd, result, exitStatus);
            throw new JobException(msg);
        }

        // Note success.
        if (_log.isDebugEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                         _job.getUuid(), result, exitStatus);
            _log.debug(msg);
        }
        
        // -------------------- Get ID ------------------------------
        // Extract the slurm id.
        String id = JobUtils.getSlurmId(_job, result, cmd);
        
        // Save the id.
        _jobCtx.getJobsDao().setRemoteJobId(_job, id);
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
        String execDir = JobExecutionUtils.getExecDir(_jobCtx, _job);
        return String.format("cd %s;sbatch %s", alwaysSingleQuote(execDir), JobExecutionUtils.JOB_WRAPPER_SCRIPT);
    }
}
