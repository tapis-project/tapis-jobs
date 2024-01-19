package edu.utexas.tacc.tapis.jobs.stagers;

import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.RuntimeOptionEnum;
import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.stagers.docker.DockerStager;
import edu.utexas.tacc.tapis.jobs.stagers.singularity.SingularityRunSlurmStager;
import edu.utexas.tacc.tapis.jobs.stagers.singularity.SingularityRunStager;
import edu.utexas.tacc.tapis.jobs.stagers.singularity.SingularityStartStager;
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
     * Supported stagers:   FORK for runtimes: Docker, Singularity_Start, Singularity_Run, ZIP
     *                      BATCH for runtimes: Singularity_Run, ZIP
     * Unsupported stagers: BATCH for Docker, Singularity_Start
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

        // RuntimeOption
        RuntimeOptionEnum runtimeOption = null;
        if (runtime == RuntimeEnum.SINGULARITY) runtimeOption = getSingularityOption(jobCtx, app);

        // ------------------------- FORK -------------------------
        if (jobType == JobType.FORK) {
            stager = switch (runtime) {
                case DOCKER      -> new DockerStager(jobCtx);
                case SINGULARITY -> getForkSingularityStager(jobCtx, runtimeOption);
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
                case SINGULARITY -> getBatchSingularityStager(jobCtx, scheduler, runtimeOption);
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
    /* getForkSingularityStager:                                              */
    /* ---------------------------------------------------------------------- */
    private static JobExecStager getForkSingularityStager(JobExecutionContext jobCtx,
                                                          RuntimeOptionEnum runtimeOption)
            throws TapisException
    {
        // Get the scheduler's stager.
        JobExecStager stager = switch (runtimeOption) {
            case SINGULARITY_START -> new SingularityStartStager(jobCtx);
            case SINGULARITY_RUN -> new SingularityRunStager(jobCtx);

            default -> {
                String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME",
                        runtimeOption + "(SINGULARITY)",
                        "JobExecStageFactory");
                throw new JobException(msg);
            }
        };

        return stager;
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchSingularityStager:                                             */
    /* ---------------------------------------------------------------------- */
    private static JobExecStager getBatchSingularityStager(JobExecutionContext jobCtx,
                                                           SchedulerTypeEnum scheduler,
                                                           RuntimeOptionEnum runtimeOption)
     throws TapisException
    {
        // Make sure the runtime option is supported.
        if (runtimeOption != RuntimeOptionEnum.SINGULARITY_RUN) {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME",
                                  scheduler + "(SINGULARITY_" + runtimeOption + ")",
                                         "JobExecStageFactory");
            throw new JobException(msg);
        }

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

    /* ---------------------------------------------------------------------- */
    /* getSingularityOption:                                                  */
    /* ---------------------------------------------------------------------- */
    private static RuntimeOptionEnum getSingularityOption(JobExecutionContext jobCtx, TapisApp app)
            throws TapisException
    {
        // We are only interested in the singularity options.
        var opts = app.getRuntimeOptions();
        boolean start = opts.contains(RuntimeOptionEnum.SINGULARITY_START);
        boolean run   = opts.contains(RuntimeOptionEnum.SINGULARITY_RUN);

        // Did we get conflicting information?
        if (start && run) {
            String msg = MsgUtils.getMsg("TAPIS_SINGULARITY_OPTION_CONFLICT",
                    jobCtx.getJob().getUuid(),
                    app.getId(),
                    RuntimeOptionEnum.SINGULARITY_START.name(),
                    RuntimeOptionEnum.SINGULARITY_RUN.name());
            throw new JobException(msg);
        }
        if (!(start || run)) {
            String msg = MsgUtils.getMsg("TAPIS_SINGULARITY_OPTION_MISSING",
                    jobCtx.getJob().getUuid(),
                    app.getId());
            throw new JobException(msg);
        }

        // At this point the option must be start or run
        if (start) return RuntimeOptionEnum.SINGULARITY_START;
        else return RuntimeOptionEnum.SINGULARITY_RUN;
    }
}
