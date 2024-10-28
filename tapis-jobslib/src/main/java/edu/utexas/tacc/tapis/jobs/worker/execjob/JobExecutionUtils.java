package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.apps.client.gen.model.TapisApp;
import edu.utexas.tacc.tapis.jobs.cancellers.JobCanceler;
import edu.utexas.tacc.tapis.jobs.cancellers.JobCancelerFactory;
import edu.utexas.tacc.tapis.jobs.exceptions.runtime.JobAsyncCmdException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.CmdMsg;
import edu.utexas.tacc.tapis.jobs.queue.messages.cmd.JobStatusMsg;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisAppAvailableException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSystemAvailableException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public final class JobExecutionUtils 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobExecutionUtils.class);
    
    // Job wrapper script name.
    public static final String JOB_WRAPPER_SCRIPT        = "tapisjob.sh";
    public static final String JOB_ENV_FILE              = "tapisjob.env";
    public static final String JOB_OUTPUT_REDIRECT_FILE  = "tapisjob.out";
    public static final String JOB_OUTPUT_EXITCODE_FILE  = "tapisjob.exitcode";
    public static final String JOB_ZIP_PID_FILE          = "tapisjob.pid";
    public static final String JOB_ZIP_APP_EXEC_DEFAULT  = "tapisjob_app.sh";
    public static final String JOB_ZIP_APP_MANIFEST      = "tapisjob.manifest";
    public static final String JOB_ZIP_SET_EXEC_SCRIPT   = "tapisjob_setexec.sh";

    // ----------------------------- Docker Section -----------------------------
    // Docker command templates.
    private static final String DOCKER_ID = "docker ps -a --no-trunc -f \"%s\" --format \"{{.ID}}\"";
    private static final String DOCKER_STATUS = "docker ps -a --no-trunc -f \"name=%s\" --format \"{{.Status}}\"";
    private static final String DOCKER_RM = "docker rm -f %s";

    // Docker status return values.
    public static final String DOCKER_ACTIVE_STATUS_PREFIX = "Up ";
    public static final String DOCKER_INACTIVE_STATUS_PREFIX = "Exited ";
    
    // Get the remote application's return code as reported by docker.
    // A terminated application will be indicated by a string like "Exited (0) 41 seconds ago\n".
    // Group 1 of this regex would return the "0" in the example.
    public static final Pattern DOCKER_RC_PATTERN = Pattern.compile(".*\\((.*)\\).*\\R*");
    
    // -------------------------- Singularity Section ----------------------------
    // Get the PID of the sinit process.
    public static final String SINGULARITY_START_PID = "singularity instance list ";
    
    // Stop the detached singularity instance.
    public static final String SINGULARITY_STOP = "singularity instance stop ";
    
    // Kill a process using the SIGKILL signal
    public static final String KILL_9_CMD_FMT = "kill -9 %s";
    // Use PKILL to kill a process and it's subprocesses using the SIGKILL signal
    public static final String PKILL_9_CMD_FMT = "pkill -9 -P %s";

    // Cancel Slurm Job
    public static final String SLURM_CANCEL = "scancel ";
    
    // Get select information about all processes running on the system.
    public static final String SINGULARITY_START_MONITOR = "ps --no-headers --sort=pid -eo pid,ppid,stat,euser,cmd";

    // ----------------------------- Zip Section -----------------------------
    public static final String ZIP_UNZIP_CMD_FMT = "cd %s; unzip %s";
    public static final String ZIP_UNTAR_CMD_FMT = "cd %s; tar -xf %s";
    public static final String ZIP_STATUS_CMD_FMT = "ps -o pid,ppid,stat,euser,cmd -p %s";
    public static final String ZIP_SETEXEC_CMD_FMT = "cd %s; ./%s";
    public static final String ZIP_FILE_RM_FROM_EXECDIR_FMT = "cd %s; /bin/rm %s";

    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getDockerCidCommand:                                                   */
    /* ---------------------------------------------------------------------- */
    public static String getDockerCidCommand(String containerName)
    {return String.format(DOCKER_ID, containerName);}
    
    /* ---------------------------------------------------------------------- */
    /* getDockerStatusCommand:                                                */
    /* ---------------------------------------------------------------------- */
    public static String getDockerStatusCommand(String containerName)
    {return String.format(DOCKER_STATUS, containerName);}
    
    /* ---------------------------------------------------------------------- */
    /* getDockerRmCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    public static String getDockerRmCommand(String containerName)
    {return String.format(DOCKER_RM, containerName);}

    /* ---------------------------------------------------------------------- */
    /* getZipStatusCommand:                                                   */
    /* ---------------------------------------------------------------------- */
    public static String getZipStatusCommand(String processPid)
    {return String.format(ZIP_STATUS_CMD_FMT, processPid);}

    /* ---------------------------------------------------------------------- */
    /* getExecDir:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Get absolute path to execSystemExecDir
     * If rootDir is null use "/" as rootDir
     *
     * @param jobCtx the non-null job context
     * @param job the non-null executing job
     * @throws TapisException on error
     */
    public static String getExecDir(JobExecutionContext jobCtx, Job job) throws TapisException
    {
        // Make sure rootDir is not null. If null Paths.get() would throw null ptr exception.
        String rootDir = Optional.ofNullable(jobCtx.getExecutionSystem().getRootDir()).orElse("/");
        return Paths.get(rootDir, job.getExecSystemExecDir()).toString();
    }

    /* ********************************************************************** */
    /*                            Package Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* executeCmdMsg:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Process status or pause commands.
     * 
     * @param jobCtx the job's context
     * @param cmdMsg the asynchronous command received by the job
     * @param newStatus the new job status
     * @throws JobAsyncCmdException on success to stop further job processing
     */
    static void executeCmdMsg(JobExecutionContext jobCtx, CmdMsg cmdMsg, JobStatusType newStatus)
     throws JobAsyncCmdException
    {
        // Informational logging.
        Job job = jobCtx.getJob();
        String msg = MsgUtils.getMsg("JOBS_CMD_MSG_RECEIVED", job.getUuid(), cmdMsg.msgType.name(),
                                     cmdMsg.senderId, cmdMsg.correlationId);
        _log.info(msg);
        
        // Change the job state.  Failure here forces us to ignore the command.
        try {jobCtx.getJobsDao().setStatus(job, newStatus, msg);}
        catch (Exception e) {
            String msg1 = MsgUtils.getMsg("JOBS_STATUS_CHANGE_ON_CMD_ERROR", job.getUuid(), 
                                          newStatus.name(), cmdMsg.msgType.name());
            _log.error(msg1, e);
            return;  // the command failed 
        }
        
        // Best effort to kill the job on cancel.
        if (newStatus.isTerminal())
            try {
            	JobCanceler canceler = JobCancelerFactory.getInstance(jobCtx);
            	canceler.cancel();
                // We never know if the attack worked.
                /*JobKiller killer = JobKillerFactory.getInstance(jobCtx);
                killer.attack();
                */
            } catch (Exception e) {
                _log.warn(MsgUtils.getMsg("JOBS_CMD_KILL_ERROR", job.getUuid(), e.getMessage()));
            }

        // Stop further job processing on success.
        throw new JobAsyncCmdException(msg);
    }
    
    /* ---------------------------------------------------------------------- */
    /* executeCmdMsg:                                                         */
    /* ---------------------------------------------------------------------- */
    static void executeCmdMsg(JobExecutionContext jobCtx, JobStatusMsg cmdMsg)
    {
        // Informational logging.
        Job job = jobCtx.getJob();
        if (_log.isInfoEnabled()) {
            String msg = MsgUtils.getMsg("JOBS_CMD_MSG_RECEIVED", job.getUuid(), cmdMsg.msgType.name(),
                                         cmdMsg.senderId, cmdMsg.correlationId);
            _log.info(msg);
        }
        
        // TODO: put job status on event queue
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkSystemEnabled:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Check system availability.  The system can be null, the job cannot.
     * 
     * @param system the system to be checked
     * @param job the non-null executing job
     * @throws TapisSystemAvailableException when not available 
     */
    static void checkSystemEnabled(TapisSystem system, Job job)
     throws TapisSystemAvailableException
    {
        // See if the system has been explicitly enabled.
        if (system == null) return;
        Boolean enabled = system.getEnabled();
        if (enabled != null && enabled) return;
            
        // Throw a recoverable exception.
        String msg = MsgUtils.getMsg("JOBS_SYSTEM_NOT_AVAILABLE", job.getUuid(), system.getId());
        _log.warn(msg);
        throw new TapisSystemAvailableException(msg, RecoveryUtils.captureSystemState(system));        
    }
    
    /* ---------------------------------------------------------------------- */
    /* checkAppEnabled:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Check application availability.  The app can be null, the job cannot.
     * 
     * @param app the application to be checked
     * @param job the non-null executing job
     * @throws TapisAppAvailableException when not available 
     */
    static void checkAppEnabled(TapisApp app, Job job)
     throws TapisAppAvailableException 
    {
        // See if the specific app version has been explicitly enabled.
    	// We interpret null enabled or versionEnabled as implying "not enabled".
        if (app == null) return;
        final Boolean enabled = app.getEnabled();
        final Boolean versionEnabled = app.getVersionEnabled();
        if (enabled != null && versionEnabled != null && enabled && versionEnabled) return;
            
        // Throw a recoverable exception.
        String msg = MsgUtils.getMsg("JOBS_APP_NOT_AVAILABLE", job.getUuid(), app.getId(), app.getVersion());
        _log.warn(msg);
        throw new TapisAppAvailableException(msg, RecoveryUtils.captureAppState(app));        
    }
}
