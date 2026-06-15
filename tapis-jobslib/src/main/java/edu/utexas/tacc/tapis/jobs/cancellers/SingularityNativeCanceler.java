package edu.utexas.tacc.tapis.jobs.cancellers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public class SingularityNativeCanceler extends AbstractJobCanceler
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SingularityNativeCanceler.class);

  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  public SingularityNativeCanceler(JobExecutionContext jobCtx)
  {
    super(jobCtx);
  }

  /* ********************************************************************** */
  /*                           Protected Methods                            */
  /* ********************************************************************** */
  @Override
  public void cancel() throws JobException, TapisException
  {
    // Best effort, no noise.
    try
    {
      // Get the command object.
      var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
      JobUtils.killJob(runCmd, _job.getUuid(), _job.getRemoteJobId(), _jobCtx );
    }
    catch (Exception e)
    {
      String msg = JobUtils.getMsg("JOBS_SINGULARITY_CLEAN_UP_ERROR", _job.getUuid(), _job.getRemoteJobId());
      _log.error(msg, e);
    }
  }
}
