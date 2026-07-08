package edu.utexas.tacc.tapis.jobs.cancellers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;

public class SlurmCanceler extends AbstractJobCanceler
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SlurmCanceler.class);

  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  public SlurmCanceler(JobExecutionContext jobCtx)
  {
    super(jobCtx);
  }

  /* ********************************************************************** */
  /*                           Protected Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* cancel:                                                                */
  /* ---------------------------------------------------------------------- */
  @Override
  public void cancel() throws TapisException
  {
    // Best effort, no noise.
    try
    {
      // Get the command object.
      var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
      // Best effort to cancel slurm job
      cancelSlurmJob(runCmd);
    }
    catch (Exception e)
    {
      String msg = JobUtils.getMsg("JOBS_SLURM_CANCEL_ERROR", _job.getUuid(), _job.getRemoteJobId(),"", "", "");
      _log.error(msg, e);
    }
  }
    
  /* ---------------------------------------------------------------------- */
  /* cancelSlurmJob:                                                       */
  /* ---------------------------------------------------------------------- */
  private void cancelSlurmJob(TapisRunCommand runCmd)
  {
   // Get the command text to terminate the slurm job
    String cmd = JobExecutionUtils.SLURM_CANCEL + _job.getRemoteJobId();

    // Stop the instance.
    String result = null;
    try
    {
      int rc = runCmd.execute(cmd);
      runCmd.logNonZeroExitCode();
      result = runCmd.getOutAsString();
    }
    catch (Exception e)
    {
      String execSysId = null;
      try {execSysId = _jobCtx.getExecutionSystem().getId();} catch (Exception e1) {}
      String msg = JobUtils.getMsg("JOBS_SLURM_CANCEL_ERROR", _job.getUuid(), _job.getRemoteJobId(), execSysId, result, cmd);
      _log.error(msg, e);
      return;
    }

    // Record the successful cancel of the slurm job
    if (_log.isDebugEnabled())
       _log.debug(JobUtils.getMsg("JOBS_SLURM_CANCEL",_job.getUuid()));
  }
}
