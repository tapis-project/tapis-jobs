package edu.utexas.tacc.tapis.jobs.stagers.singularityslurm;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.stagers.singularitynative.AbstractSingularityStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SingularityRunSlurmStager 
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
    
    // Embedded singularity stager.
    private final WrappedSingularityRunStager _wrappedStager;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunSlurmStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        // The singularity stager must initialize before the slurm run command.
        super(jobCtx);
        _wrappedStager = new WrappedSingularityRunStager(jobCtx);
        _slurmRunCmd   = configureSlurmRunCmd();
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    @Override
    public void stageJob() throws TapisException 
    {
        // Create the wrapper script.
        String wrapperScript = generateWrapperScript();
        
        // Create the exported env variable content.
        String envVarFile = generateEnvVarFile();
        
        // Install the wrapper script on the execution system.
        var fm = _jobCtx.getJobFileManager();
        fm.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);
        
        // Install the exported env variable file.
        fm.installExecFile(envVarFile, JobExecutionUtils.JOB_ENV_FILE, JobFileManager.RWRW);
    }
    
    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateWrapperScript() throws TapisException 
    {
        // Initialize the script content in superclass.
        initBashBatchScript();
        
        // Add the batch directives to the script.
        _cmd.append(_slurmRunCmd.generateExecCmd(_job));
        
        // Add zero or more module load commands.
        appendModuleLoadCalls();
        
        // Add the actual singularity run command.
        _cmd.append(_wrappedStager.getCmdTextWithEnvVars());
        _cmd.append("\n");
        
        return _cmd.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Create an exportable env variable definitions. 
     * 
     * @return the string that contains all exported env variable assignments
     */
    @Override
    protected String generateEnvVarFile() throws TapisException 
    {
        // Create a list of key=value assignment, each followed by a new line.
        var pairs = _wrappedStager.getSingularityRunCmd().getEnv();
        return getExportPairListArgs(pairs);
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* appendModuleLoadCalls:                                                 */
    /* ---------------------------------------------------------------------- */
    private void appendModuleLoadCalls()
     throws JobException
    {
        // There's nothing to do unless a profile was specified.
        if (StringUtils.isBlank(_slurmRunCmd.getTapisProfile())) return;
        
        // Make sure we retrieve the profile.
        var profile = _jobCtx.getSchedulerProfile(_slurmRunCmd.getTapisProfile());

        // Get the array of module load specs.
        var specs = profile.getModuleLoads();
        if (specs == null || specs.isEmpty()) return;
        
        // Iterate through the list of specs.
        for (var spec : specs) {
            // There has to be a load command.
            var loadCmd = spec.getModuleLoadCommand();
            if (StringUtils.isBlank(loadCmd)) continue;
            
            // We allow commands that don't require module parameters.
            var modules = spec.getModulesToLoad();
            if (modules == null || modules.isEmpty()) {
                _cmd.append(loadCmd + "\n");
                continue;
            }
            
            // Put in the required spacing.
            if (!loadCmd.endsWith(" ")) loadCmd += " ";
            
            // Create a module load command for each specified module.
            for (var module : modules)
                if (StringUtils.isNotBlank(module)) 
                    _cmd.append(loadCmd + module + "\n");
        }
        
        // End with a blank line.
        _cmd.append("\n");
    }
    
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
        // Populate the slurm command with user specified options.
        var slurmCmd = new SingularityRunSlurmCmd(_jobCtx);
        setUserSlurmOptions(slurmCmd);
        setTapisOptionsForSlurm(slurmCmd);
        return slurmCmd;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setTapisOptionsForSlurm:                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the standard Tapis settings for Slurm.
     * 
     * TODO: At this time we always omit setting the --mem option as required by TACC systems.
     * 
     * @param slurmCmd
     * @throws TapisException 
     */
    private void  setTapisOptionsForSlurm(SingularityRunSlurmCmd slurmCmd) 
     throws TapisException
    {
        // --------------------- Tapis Mandatory ---------------------
        // Request the total number of nodes from slurm. 
        slurmCmd.setNodes(Integer.toString(_job.getNodeCount()));
        
        // Tell slurm the total number of tasks to run.
        slurmCmd.setNtasks(Integer.toString(_job.getTotalTasks()));
        
        // Tell slurm the total runtime of the application in minutes.
        slurmCmd.setTime(Integer.toString(_job.getMaxMinutes()));
        
        // Tell slurm the memory per node requirement in megabytes.
        slurmCmd.setMem(Integer.toString(_job.getMemoryMB()));
        
        // We've already checked in JobQueueProcessor before processing any
        // state changes that the logical and hpc queues have been assigned.
        var logicalQueue = _jobCtx.getLogicalQueue();
        slurmCmd.setPartition(logicalQueue.getHpcQueueName());
        
        // --------------------- Tapis Optional ----------------------
        // Always assign a job name if user has not specified one.
        if (StringUtils.isBlank(slurmCmd.getJobName())) {
            var singularityRunCmd = _wrappedStager.getSingularityRunCmd();
            String image = singularityRunCmd.getImage();
            var parts = image.split("/");
            
            // The last part element should be present and never empty.
            if (parts == null || parts.length == 0) 
                slurmCmd.setJobName(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
              else slurmCmd.setJobName(parts[parts.length-1]);
        }
        
        // Assign the standard tapis output file name if one is not 
        // assigned and we are not running an array job.  We let slurm
        // use its default naming scheme for array job output files.
        // Unless the user explicitly specifies an error file, both
        // stdout and stderr will go the designated output file.
        if (StringUtils.isBlank(slurmCmd.getOutput()) && 
            StringUtils.isBlank(slurmCmd.getArray())) {
            var fm = _jobCtx.getJobFileManager();
            slurmCmd.setOutput(
                fm.makeAbsExecSysOutputPath(JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE));
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* setUserSlurmOptions:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Set the slurm options that we allow the user to modify.
     * 
     * @param slurmCmd the run command to be updated
     */
    private void setUserSlurmOptions(SingularityRunSlurmCmd slurmCmd)
     throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getSchedulerOptions();
        if (opts == null || opts.isEmpty()) return;
        
        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
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
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
                throw new JobException(msg);
            }
            
            // Save the parsed value.
            slurmCmd.assignCmd(option, value);
        }
    }
}
