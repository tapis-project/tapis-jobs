package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

public class SingularityRunSlurmStager
  extends AbstractSingularityStager
  implements ISingularityRun
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmStager.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Slurm run command object.
    private final SingularityRunSlurmCmd _slurmRunCmd;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunSlurmStager(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType)
     throws TapisException
    {
        // Set _jobCtx, _job, _cmdBuilder, _isBatch, _jobExecCmd, _scheduler (with slurmOptions)
        super(jobCtx, schedulerType);
        // The specific exec command
        _slurmRunCmd = (SingularityRunSlurmCmd) _jobExecCmd;
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */

    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateWrapperScriptContent() throws TapisException
    {
        // Run as bash batch script.
        initBashBatchScript();

        // Add batch directives and any module load commands.
        _cmdBuilder.append(_jobScheduler.getBatchDirectives());
        _cmdBuilder.append(_jobScheduler.getModuleLoadCalls());

        // Generate the basic single line command text for singularity RUN under slurm
        String cmdText = _slurmRunCmd.generateExecCmd(_job);

        // Add the exec command.
        _cmdBuilder.append(cmdText);

        return _cmdBuilder.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* createJobExecCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Create the JobExecCmd.
     *
     */
    @Override
    public JobExecCmd createJobExecCmd() throws TapisException
    {
        return configureSlurmRunCmd();
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureSlurmRunCmd:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Initialize a new slurm run command with user and tapis specified options.
     * The signularity stager field must be initialized before calling this
     * method.
     * 
     * @return the initialized slurm run command
     * @throws TapisException
     */
    private SingularityRunSlurmCmd configureSlurmRunCmd()
            throws TapisException
    {
        // Create and populate the singularity command.
        var singularityCmd = new SingularityRunSlurmCmd();

        // Set the image.
        singularityCmd.setImage(_jobCtx.getApp().getContainerImage());

        // Set the application arguments.
        singularityCmd.setAppArguments(concatAppArguments());

        // Set all environment variables.
        singularityCmd.setEnv(getEnvVariables());

        // Set the singularity options.
        setSingularityOptions(singularityCmd);
        
        return singularityCmd;
    }
}
