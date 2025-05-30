package edu.utexas.tacc.tapis.jobs.stagers;

import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeOptionEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.stagers.docker.DockerStager;
import edu.utexas.tacc.tapis.jobs.stagers.singularity.SingularityRunSlurmStager;
import edu.utexas.tacc.tapis.jobs.stagers.singularity.SingularityRunStager;
import edu.utexas.tacc.tapis.jobs.stagers.zip.ZipStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

public final class JobExecStageFactory 
{
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create a stager based on the type of job and its execution environment.
     * This method either returns the appropriate stager or throws an exception.
     *
     * Supported stagers:   FORK for runtimes: Docker, Singularity_Run, ZIP
     *                      BATCH for runtimes: Singularity_Run, ZIP
     * Unsupported stagers: BATCH for Docker
     *
     * @param jobCtx job context
     * @return the stager designated for the current job type and environment
     * @throws TapisException when no stager is found or a network error occurs
     */
    public static JobExecStager getInstance(JobExecutionContext jobCtx) 
     throws TapisException 
    {
        // Extract required information from app and job.
        var app     = jobCtx.getApp();
        var runtime = app.getRuntime();
        var jobType = jobCtx.getJob().getJobType();
        
        // The result.
        JobExecStager stager = null;

        // ------------------------- FORK -------------------------
        if (jobType == JobType.FORK) {
            stager = switch (runtime) {
                case DOCKER      -> new DockerStager(jobCtx);
                case SINGULARITY -> new SingularityRunStager(jobCtx);
                case ZIP         -> new ZipStager(jobCtx, null /*schedulerType*/);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobExecStageFactory");
                    throw new JobException(msg);
                }
            };
        }
        // ------------------------- BATCH ------------------------
        else if (jobType == JobType.BATCH) {
            // Get the scheduler under which containers will be launched.
            var system = jobCtx.getExecutionSystem();
            var scheduler = system.getBatchScheduler();
            
            // Double check that a scheduler is assigned.
            if (scheduler == null) {
                String msg = MsgUtils.getMsg("JOBS_SYSTEM_MISSING_SCHEDULER", system.getId(), 
                                              jobCtx.getJob().getUuid());
                throw new JobException(msg);
            }
            
            // Get the stager for each supported runtime/scheduler combination.
            stager = switch (runtime) {
                case DOCKER      -> getBatchDockerStager(jobCtx, scheduler);
                case SINGULARITY -> getBatchSingularityStager(jobCtx, scheduler);
                case ZIP         -> getBatchZipStager(jobCtx, scheduler);
                default -> {
                    String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", runtime, 
                                                 "JobExecStageFactory");
                    throw new JobException(msg);
                }
            };
        }
        else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_TYPE", jobType, "JobExecStageFactory");
            throw new JobException(msg);
        }
        
        return stager;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchDockerStager:                                                  */
    /* ---------------------------------------------------------------------- */
    private static JobExecStager getBatchDockerStager(JobExecutionContext jobCtx,
                                                      SchedulerTypeEnum scheduler) 
     throws TapisException
    {
        // Not yet supported
        String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME",
                                     scheduler + "(DOCKER)",
                                     "JobExecStageFactory");
        throw new JobException(msg);
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityStager:                                             */
    /* ---------------------------------------------------------------------- */
    private static JobExecStager getBatchSingularityStager(JobExecutionContext jobCtx,
                                                           SchedulerTypeEnum scheduler)
     throws TapisException
    {
        // Get the scheduler's stager.
        JobExecStager stager = switch (scheduler) {
            case SLURM -> new SingularityRunSlurmStager(jobCtx, scheduler);
        
            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", 
                                             scheduler + "(SINGULARITY)", 
                                             "JobExecStageFactory");
                throw new JobException(msg);
            }
        };
        return stager;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchZipStager:                                                     */
    /* ---------------------------------------------------------------------- */
    private static JobExecStager getBatchZipStager(JobExecutionContext jobCtx,
                                                   SchedulerTypeEnum scheduler)
            throws TapisException
    {
        // Get the scheduler's docker stager.
        JobExecStager stager = switch (scheduler) {
            case SLURM -> new ZipStager(jobCtx, scheduler);

            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME",
                                             scheduler + "(ZIP)",
                                             "JobExecStageFactory");
                throw new JobException(msg);
            }
        };

        return stager;
    }
}
