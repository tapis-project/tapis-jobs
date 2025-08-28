package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

public class JobCancelerFactory {

	/* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a Canceler based on the type of job and its execution environment.
     * This method either returns the appropriate Canceler or throws an exception.
     * 
     * @param jobCtx job context
     * @return the Canceler designated for the current job type and environment
     * @throws TapisException when no canceler is found or a network error occurs
     */
    public static JobCanceler getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
    	 // Extract required information from app and job.
        var app = jobCtx.getApp();
        var runtime = app.getRuntime();
        var jobType = jobCtx.getJob().getJobType();
        
        // The result.
        JobCanceler canceler = null;
        
        // ------------------------- FORK -------------------------
       if (jobType == JobType.FORK) {
    	   canceler = switch (runtime) {
                case DOCKER      -> new DockerNativeCanceler(jobCtx);
                case SINGULARITY -> new SingularityRunCanceler(jobCtx);
                case ZIP         -> new ZipNativeCanceler(jobCtx);
                default -> {
                    String msg = JobUtils.getMsg("JOBS_UNSUPPORTED_APP_RUNTIME", runtime,
                                                 "JobCancelerFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
        else if (jobType == JobType.BATCH) {
            // Get the scheduler under which containers were launched.
            var system = jobCtx.getExecutionSystem();
            var scheduler = system.getBatchScheduler();
            
            // Double check that a scheduler is assigned.
            if (scheduler == null) {
                String msg = JobUtils.getMsg("JOBS_SYSTEM_MISSING_SCHEDULER", system.getId(), 
                                              jobCtx.getJob().getUuid());
                throw new JobException(msg);
            }
            
            // Get the canceler for each supported runtime/scheduler combination.
            canceler = switch (runtime) {
                case DOCKER      -> getBatchDockerCanceler(jobCtx, scheduler);
                case SINGULARITY -> getBatchSingularityCanceler(jobCtx, scheduler);
                default -> {
                    String msg = JobUtils.getMsg("JOBS_UNSUPPORTED_APP_RUNTIME", runtime,
                                                 "JobCancelerFactory");
                    throw new JobException(msg);
                }
            };
        }
        else {
            String msg = JobUtils.getMsg("JOBS_UNSUPPORTED_APP_TYPE", jobType, "JobCancelerFactory");
            throw new JobException(msg);
        }
		return canceler;
    	
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchDockerCanceler:                                                 */
    /* ---------------------------------------------------------------------- */
    private static JobCanceler getBatchDockerCanceler(JobExecutionContext jobCtx,
                                                      SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's docker canceler. 
    	JobCanceler canceler = switch (scheduler) {
            case SLURM -> null; // not implemented
            
            default -> {
                String msg = JobUtils.getMsg("JOBS_UNSUPPORTED_APP_RUNTIME",
                                             scheduler.name(), "JobCancelerFactory");
                throw new JobException(msg);
            }
        };
        
        return canceler;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityCanceler:                                            */
    /* ---------------------------------------------------------------------- */
    private static JobCanceler getBatchSingularityCanceler(JobExecutionContext jobCtx,
                                                           SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Get the scheduler's slurm canceler. 
        JobCanceler canceler = switch (scheduler) {
            case SLURM -> new SlurmCanceler(jobCtx);
        
            default -> {
                String msg = JobUtils.getMsg("JOBS_UNSUPPORTED_APP_RUNTIME",
                                             scheduler.name(), "JobCancelerFactory");
                throw new JobException(msg);
            }
        };
        
        return canceler;
    }

}
