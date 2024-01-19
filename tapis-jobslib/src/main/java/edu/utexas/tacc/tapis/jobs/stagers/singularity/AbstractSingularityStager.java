package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

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
        // Initialize _jobCtx, _job, _cmdBuilder, _scheduler, _isBatch, _jobExecCmd
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
    public String generateWrapperScriptContent()
            throws TapisException
    {
        // Run as bash script, either BATCH or FORK
        if (_isBatch) initBashBatchScript(); else initBashScript();

        // If a BATCH job add the directives and any module load commands.
        if (_isBatch) {
            _cmdBuilder.append(_scheduler.getBatchDirectives());
            _cmdBuilder.append(_scheduler.getModuleLoadCalls());
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
    public String generateEnvVarFileContent()
            throws TapisException
    {
//TODO        return _singularityExecCmd.generateEnvVarFileContent();
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

// TODO
//    /* ---------------------------------------------------------------------- */
//    /* configureSingularityStartCmd:                                          */
//    /* ---------------------------------------------------------------------- */
//    private SingularityStartCmd configureExecCmd()
//            throws TapisException
//    {
//        // Create and populate the singularity command.
//        var singularityCmd = new SingularityStartCmd();
//
//        // ----------------- Tapis Standard Definitions -----------------
//        // Container instances are named after the job uuid.
//        singularityCmd.setName(_job.getUuid());
//
//        // Write the container id to a host file.
//        setPidFile(singularityCmd);
//
//        // Write all the environment variables to file.
//        singularityCmd.setEnvFile(makeEnvFilePath());
//
//        // Set the image.
//        singularityCmd.setImage(_jobCtx.getApp().getContainerImage());
//
//        // ----------------- User and Tapis Definitions -----------------
//        // Set all environment variables.
//        singularityCmd.setEnv(getEnvVariables());
//
//        // Set the singularity options.
//        setSingularityOptions(singularityCmd);
//
//        // Set the application arguments.
//        singularityCmd.setAppArguments(concatAppArguments());
//
//        return singularityCmd;
//    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */

}
