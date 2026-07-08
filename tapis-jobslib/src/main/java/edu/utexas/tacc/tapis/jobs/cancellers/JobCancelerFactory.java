package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;

public class JobCancelerFactory
{
  /**
   * getInstance:
   *  Create a Canceler based on the type of job and its execution environment.
   *  This method either returns the appropriate Canceler or throws an exception.
   *  There are six combinations of jobType and runtimeType:
   *     jobType x runtimeType = (FORK, BATCH) x (DOCKER, SINGULARITY, ZIP)
   *  The only combination that Jobs does not support is BATCH-DOCKER
   *  The only schedulerType supported (for BATCH) is slurm.
   *
   * @param jobCtx job context
   * @return the Canceler designated for the current job type and environment
   * @throws TapisException when no canceler is found or a network error occurs
   */
  public static JobCanceler getInstance(JobExecutionContext jobCtx) throws TapisException
  {
    // Extract required information from app and job.
    TapisApp app = jobCtx.getApp();
    RuntimeEnum runtimeType = app.getRuntime();
    Job job = jobCtx.getJob();
    JobType jobType = job.getJobType();
    TapisSystem system = jobCtx.getExecutionSystem();
    SchedulerTypeEnum schedulerType = system.getBatchScheduler();
    String jobUuid = job.getUuid(); // For logging

    // Avoid potential NPE
    if (runtimeType == null) {throw new JobException(JobUtils.getMsg("TAPIS_NULL_PARAMETER", "JobCanceler.getInstance", "app.runtime"));}

    // Check that we support the cancel operations for this type of job.
    String msg = JobUtils.checkCancelSupported(jobType, runtimeType, schedulerType, jobUuid);
    if (!StringUtils.isBlank(msg)) { throw new JobException(msg); }

    // The result.
    JobCanceler canceler;

    // ------------------------- FORK -------------------------
    if (jobType == JobType.FORK)
    {
      canceler = switch (runtimeType)
      {
        case DOCKER      -> new DockerNativeCanceler(jobCtx);
        case SINGULARITY -> new SingularityNativeCanceler(jobCtx);
        case ZIP         -> new ZipNativeCanceler(jobCtx);
        // NOTE: All cases covered. No need for a default case. If enum added -> compile error
      };
    }

    // ------------------------- BATCH ------------------------
    else if (jobType == JobType.BATCH)
    {
      // Double check that a scheduler is assigned.
      if (schedulerType == null)
      {
        throw new JobException(JobUtils.getMsg("JOBS_SYSTEM_MISSING_SCHEDULER", system.getId(), jobCtx.getJob().getUuid()));
      }

      // Get the canceler for each supported runtime/scheduler combination.
      canceler = switch (runtimeType)
      {
        case DOCKER      -> getBatchDockerCanceler(jobUuid, jobType, runtimeType, schedulerType); // Not yet supported, throws exception
        case SINGULARITY, ZIP -> getBatchSchedulerCanceler(jobCtx, schedulerType, jobUuid);
        // NOTE: All cases covered. No need for a default case. If enum added -> compile error
      };
    }
    else
    {
      // This should have been rejected by checkCancelSupported call. But just in case we get this far.
      msg = JobUtils.getMsg("JOBS_UNSUPPORTED_CANCEL_OP", jobUuid, jobType, runtimeType, schedulerType);
      throw new JobException(msg);
    }
    return canceler;
  }

  /* ---------------------------------------------------------------------- */
  /* getBatchDockerCanceler:                                                */
  /* ---------------------------------------------------------------------- */
  private static JobCanceler getBatchDockerCanceler(String jobUuid, JobType jobType, RuntimeEnum runtimeType,
                                                    SchedulerTypeEnum schedulerType)
     throws JobException
  {
    // This should have been rejected by checkCancelSupported call. But just in case we get this far.
    String msg = JobUtils.getMsg("JOBS_UNSUPPORTED_CANCEL_OP", jobUuid, jobType, runtimeType, schedulerType);
    throw new JobException(msg);
  }

  /* ---------------------------------------------------------------------- */
  /* getBatchSchedulerCanceler:                                             */
  /* ---------------------------------------------------------------------- */
  private static JobCanceler getBatchSchedulerCanceler(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType,
                                                       String jobUuid)
     throws JobException
  {
    // Get the canceler for the scheduler. Only slurm supported.
    if (SchedulerTypeEnum.SLURM.equals(schedulerType))
    {
      return new SlurmCanceler(jobCtx);
    }
    else
    {
      // This should have been rejected at the front-end, but just in case we get this far.
      String msg = JobUtils.getMsg("JOBS_UNSUPPORTED_SCHEDULER_TYPE", jobUuid, schedulerType.name());
      throw new JobException(msg);
    }
  }
}
