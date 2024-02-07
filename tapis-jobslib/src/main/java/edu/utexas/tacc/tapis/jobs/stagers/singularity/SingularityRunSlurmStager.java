package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

public class SingularityRunSlurmStager
  extends AbstractSingularityStager
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

    /* ---------------------------------------------------------------------- */
    /* setSingularityOptions:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Set the singularity options that we allow the user to modify.
     * 
     * @param singularityCmd the run command to be updated
     */
    private void setSingularityOptions(SingularityRunSlurmCmd singularityCmd)
     throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getContainerArgs();
        if (opts == null || opts.isEmpty()) return;
        
        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "singularity", opt.getArg());
                throw new JobException(msg);
            }
            
            // Get the option and its value if one is provided.
            String option = null;
            String value  = ""; // default value when none provided
            int groupCount = m.groupCount();
            if (groupCount > 0) option = m.group(1);
            if (groupCount > 1) value  = m.group(2);            
            
            // The option should always exist.
            if (StringUtils.isBlank(option)) {
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "singularity", opt.getArg());
                throw new JobException(msg);
            }
            
            // Save the parsed value.
            assignCmd(singularityCmd, option, value);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignCmd:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified singularity run parameter.  If the parameter
     * pertains to run only--that is it's not a paramter also used by start--then
     * it will be set here.  If the parameter is not run only, then the command
     * parameter assignment method in the superclass will be called.
     * 
     * Note that this method overloads but does not override the superclass
     * method with the name.
     * 
     * @param singularityCmd the run command
     * @param option the singularity argument
     * @param value the argument's non-null value
     */
    protected void assignCmd(SingularityRunCmd singularityCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            // Run common options.
            case "--app":
                singularityCmd.setApp(value);
                break;
            case "--ipc":
            case "-i":
                singularityCmd.setIpc(true);
                break;
            case "--nonet":
                singularityCmd.setNoNet(true);
                break;
            case "--pid":
            case "-p":
                singularityCmd.setPid(true);
                break;
            case "--pwd":
                singularityCmd.setPwd(value);
                break;
            case "--vm":
                singularityCmd.setVm(true);
                break;
            case "--vm-cpu":
                singularityCmd.setVmCPU(value);
                break;
            case "--vm-err":
                singularityCmd.setVmErr(true);
                break;
            case "--vm-ip":
                singularityCmd.setVmIP(value);
                break;
            case "--vm-ram":
                singularityCmd.setVmRAM(value);
                break;
        
            // It's either a common option or invalid.
            default: super.assignCmd(singularityCmd, option, value);
        }
    }
}
