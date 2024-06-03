package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;

abstract class AbstractSingularityStager
 extends AbstractJobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractSingularityStager.class);

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public AbstractSingularityStager(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType)
     throws TapisException
    {
        // Initialize _jobCtx, _job, _cmdBuilder, _isBatch, _jobExecCmd, _scheduler (with slurmOptions)
        super(jobCtx, schedulerType);
    }

    /* ********************************************************************** */
    /*                          Public Methods                                */
    /* ********************************************************************** */

    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    /** This method generates the wrapper script content for tapisjob.sh.
     *
     * @return the wrapper script content
     */
    @Override
    public String generateWrapperScriptContent()
            throws TapisException
    {
        // Run as bash script, either BATCH or FORK
        if (_isBatch) initBashBatchScript(); else initBashScript();

        // If a BATCH job add the directives and any module load commands.
        if (_isBatch) {
            _cmdBuilder.append(_jobScheduler.getBatchDirectives());
            _cmdBuilder.append(_jobScheduler.getModuleLoadCalls());
        }

        // Generate the basic single line command text for singularity RUN or START
        String cmdText = _jobExecCmd.generateExecCmd(_job);

        // Add the exec command.
        _cmdBuilder.append(cmdText);
        return _cmdBuilder.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for a environment variable definition file.
     *
     * @return the content for a environment variable definition file
     */
    @Override
    public String generateEnvVarFileContent()
            throws TapisException
    {
        return _jobExecCmd.generateEnvVarFileContent();
    }

    /* ---------------------------------------------------------------------- */
    /* createJobExecCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Create the JobExecCmd.
     *
     */
    @Override
    public abstract JobExecCmd createJobExecCmd() throws TapisException;

    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* makeEnvFilePath:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Write the environment variables to a host file.
     * 
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    protected String makeEnvFilePath() 
     throws TapisException
    {
        // Put the env file in the execution directory.
        var fm = _jobCtx.getJobFileManager();
        return fm.makeAbsExecSysExecPath(JobExecutionUtils.JOB_ENV_FILE);
    }

    /* ---------------------------------------------------------------------- */
    /* setSingularityOptions:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Set the singularity options that we allow the user to modify.
     * 
     * @param singularityCmd the run command to be updated
     */
    protected <T extends AbstractSingularityExecCmd & ISingularityRun>
    void setSingularityOptions(T singularityCmd)
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
            assignRunCmd(singularityCmd, option, value);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignRunCmd:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified singularity run parameter.  If the parameter
     * pertains to run only--that is it's not a paramter also used by start--then
     * it will be set here.  If the parameter is not run only, then the command
     * parameter assignment method in the superclass will be called.
     * 
     * Note that this method overloads but does not override the superclass
     * method with the name.
     * 
     * @param singularityCmd the singularity run command
     * @param option the singularity argument
     * @param value the argument's non-null value
     */
    protected <T extends AbstractSingularityExecCmd & ISingularityRun>
    void assignRunCmd(T singularityCmd, String option, String value)
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
            default: assignCmd(singularityCmd, option, value);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignCmd:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified singularity parameter.  This method set the 
     * singularity options that are common to both singularity run and start.
     * 
     * @param singularityCmd the start or run command
     * @param option the singularity argument
     * @param value the argument's non-null value
     */
    protected void assignCmd(AbstractSingularityExecCmd singularityCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            // Start/Run common options.
            case "--add-caps":
                singularityCmd.setCapabilities(value);
                break;
            case "--bind":
            case "-B":
                isAssigned("singularity", option, value);
                singularityCmd.getBind().add(value);
                break;
            case "--cleanenv":
            case "-e":
                singularityCmd.setCleanEnv(true);
                break;
            case "--compat":
                singularityCmd.setCompat(true);
                break;
            case "--contain":
            case "-c":
                singularityCmd.setContain(true);
                break;
            case "--containall":
            case "-C":
                singularityCmd.setContainAll(true);
                break;
            case "--disable-cache":
                singularityCmd.setDisableCache(true);
                break;
            case "--dns":
                singularityCmd.setDns(value);
                break;
            case "--drop-caps":
                singularityCmd.setDropCapabilities(value);
                break;
            case "--fusemount":
                isAssigned("singularity", option, value);
                singularityCmd.getFusemount().add(value);
                break;
            case "--home":
            case "-H":
                singularityCmd.setHome(value); 
                break;
            case "--hostname":
                singularityCmd.setHostname(value);
                break;
            case "--mount":
                isAssigned("singularity", option, value);
                singularityCmd.getMount().add(value);
                break;
            case "--net":
            case "-n":
                singularityCmd.setNet(true);
                break;
            case "--network":
                singularityCmd.setNetwork(value);
                break;
            case "--network-args":
                isAssigned("singularity", option, value);
                singularityCmd.getNetworkArgs().add(value);
                break;
            case "--no-home":
                singularityCmd.setNoHome(true);
                break;
            case "--no-init":
                singularityCmd.setNoInit(true);
                break;
            case "--no-mount":
                isAssigned("singularity", option, value);
                singularityCmd.getNoMounts().add(value);
                break;
            case "--no-privs":
                singularityCmd.setNoPrivs(true);
                break;
            case "--no-umask":
                singularityCmd.setNoUMask(true);
                break;
            case "--nohttps":
                singularityCmd.setNoHTTPS(true);
                break;
            case "--nv":
                singularityCmd.setNv(true);
                break;
            case "--nvcli":
                singularityCmd.setNvcli(true);
                break;
            case "--overlay":
            case "-O":
                isAssigned("singularity", option, value);
                singularityCmd.getOverlay().add(value);
                break;
            case "--pem-path":
                singularityCmd.setPemPath(value);
                break;
            case "--rocm":
                singularityCmd.setRocm(true);
                break;
            case "--scratch":
            case "-S":
                isAssigned("singularity", option, value);
                singularityCmd.getScratch().add(value);
                break;
            case "--security":
                isAssigned("singularity", option, value);
                singularityCmd.getSecurity().add(value);
                break;
            case "--userns":
            case "-U":
                singularityCmd.setUserNs(true);
                break;
            case "--uts":
                singularityCmd.setUts(true);
                break;
            case "--workdir":
            case "-W":
                singularityCmd.setWorkdir(value);
                break;
            case "--writable":
            case "-w":
                singularityCmd.setWritable(true);
                break;
            case "--writable-tmpfs":
                singularityCmd.setWritableTmpfs(true);
                break;
                
            default:
                // The following options are reserved for tapis-only use.
                // If the user specifies any of them as a container option,
                // the job will abort.  Note that environment variables are 
                // passed in via their own ParameterSet object.
                //
                //   --pidfile, --env
                //
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_UNSUPPORTED_ARG", "singularity", option);
                throw new JobException(msg);
        }
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
}
