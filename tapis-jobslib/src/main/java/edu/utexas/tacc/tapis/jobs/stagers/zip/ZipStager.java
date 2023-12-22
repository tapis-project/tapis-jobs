package edu.utexas.tacc.tapis.jobs.stagers.zip;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInput;
import edu.utexas.tacc.tapis.jobs.schedulers.JobScheduler;
import edu.utexas.tacc.tapis.jobs.schedulers.SlurmScheduler;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerTypeEnum;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 *  This class handles staging of application assets for applications defined using runtime type of ZIP.
 *  Used for both FORK and BATCH type jobs.
 *  For BATCH jobs schedulerType must be set.
 */
public class ZipStager
 extends AbstractJobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipStager.class);

    private static final String ZIP_FILE_EXTENSION = ".zip";
    private static final String UNZIP_COMMAND = "unzip";

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Zip run command object.
    private final ZipRunCmd _zipRunCmd;
    // Attributes describing the app archive and container image
    private String _appArchivePath;
    private String _appArchiveFile;
    private boolean _appArchiveIsZip;
    private String _containerImage;
    private boolean _containerImageIsUrl;
    private final JobScheduler scheduler;
    private final boolean isBatch;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipStager(JobExecutionContext jobCtx, SchedulerTypeEnum schedulerType)
            throws TapisException
    {
        // Set _jobCtx, _job, _cmd
        super(jobCtx);
        // Set containerImage
        _containerImage = _jobCtx.getApp().getContainerImage();
        // Configure the appArchive properties
        configureAppArchiveInfo();
        // Set the scheduler properties as needed.
        if (schedulerType == null) {
            scheduler = null;
        } else {
            // NOTE: Once other schedulers are supported create the appropriate scheduler
            scheduler = new SlurmScheduler(jobCtx, _appArchiveFile);
        }
        isBatch = (schedulerType != null);
        // Create and configure the zip run command
        _zipRunCmd = configureRunCmd();
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
        // Get the job file manager used to make directories, upload files, transfer files, etc.
        var jobFileManager = _jobCtx.getJobFileManager();

        // If archive file is to be processed using unzip then make sure it is available on the exec host
        if (_appArchiveIsZip) jobFileManager.checkForCommand(UNZIP_COMMAND);

        // Stage the app archive. This may involve a transfer
        jobFileManager.stageAppAssets(_containerImage, _containerImageIsUrl, _appArchiveFile);

        // Run a remote command to extract the application archive file into execSystemExecDir.
        jobFileManager.extractZipAppArchive(_appArchivePath, _appArchiveIsZip);

        // Now that app archive is unpacked we can determine the app executable
        // Generate and install the script that we will run to determine the executable: tapisjob_setexec.sh
        String setAppExecutableScript = generateSetAppExecutableScript();
        jobFileManager.installExecFile(setAppExecutableScript, JobExecutionUtils.JOB_ZIP_SET_EXEC_SCRIPT, JobFileManager.RWXRWX);

        // Run the script to determine the executable. Creates tapisjob.exec
        jobFileManager.runZipSetAppExecutable(JobExecutionUtils.JOB_ZIP_SET_EXEC_SCRIPT);

        // Create and install the wrapper script: tapisjob.sh
        String wrapperScript = generateWrapperScript();
        jobFileManager.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);

        // Create and install the environment variable definition file: tapisjob.env
        String envVarFile = generateEnvVarFile();
        jobFileManager.installExecFile(envVarFile, JobExecutionUtils.JOB_ENV_FILE, JobFileManager.RWRW);

        // If a FORK job then create script to monitor status
        if (!isBatch) {
            // Create and install the script used to monitor job status: tapisjob_status.sh
            String jobStatusScript = generateJobStatusScript();
            jobFileManager.installExecFile(jobStatusScript, JobExecutionUtils.JOB_MONITOR_STATUS_SCRIPT, JobFileManager.RWXRWX);
        }
    }

    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    /** This method generates the wrapper script content.
     *
     * @return the wrapper script content
     */
    @Override
    protected String generateWrapperScript() throws JobException
    {
        // Run as bash script
        if (isBatch) initBashBatchScript(); else initBashScript();

        // If a BATCH job add the directives and any module load commands.
        if (isBatch) {
            _cmd.append(scheduler.getBatchDirectives());
            _cmd.append(scheduler.getModuleLoadCalls());
        }

        // Construct the command and append it to get the full command script
        String zipCmd = _zipRunCmd.generateExecCmd(_job);
        _cmd.append(zipCmd);

        return _cmd.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for an environment variable definition file.
     *  
     * @return the content for a environment variable definition file 
     */
    @Override
    protected String generateEnvVarFile()
    {
        return _zipRunCmd.generateEnvVarFileContent();
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */

    /* ---------------------------------------------------------------------- */
    /* generateSetExecutableScript:                                           */
    /* ---------------------------------------------------------------------- */
    /** This method generates script to determine a ZIP runtime executable
     *
     * @return the script content
     */
    private String generateSetAppExecutableScript()
    {
        // Use a text block for simplicity and clarity.
        return
            """
            #!/bin/sh
            #
            # Script to determine Tapis application executable for application defined as runtime type of ZIP.
            #
            # Set a default
            APP_EXEC="tapisjob_app.sh"
            
            # If default does not exist but manifest file is present then search manifest file
            if [ ! -f "./tapisjob_app.sh" ] && [ -f "./tapisjob.manifest" ]; then
              APP_EXEC=$(grep -v "^#" tapisjob.manifest | grep "^tapisjob_executable=" | sed -E 's/(.*)=//')
            fi
            
            # Check for errors. If all OK then ensure that file is executable.
            if [ -z "${APP_EXEC}" ]; then
              echo "ERROR: Unable to determine application executable"
              echo "ERROR: Please provide tapisjob_app.sh or a manifest specifying tapisjob_executable."
              exit 1
            elif [ ! -f "./${APP_EXEC}" ]; then
              echo "ERROR: Looking for application executable = $APP_EXEC but file does not exist"
              exit 2
            else
              chmod +x "./${APP_EXEC}"
            fi
            echo "Found application executable = ${APP_EXEC}"
            echo "${APP_EXEC}" > "./tapisjob.exec"
            """;
    }

    /* ---------------------------------------------------------------------- */
    /* generateJobStatusScript:                                               */
    /* ---------------------------------------------------------------------- */
    /** This method generates script to determine the status for a ZIP runtime job
     *
     * @return the script content
     */
    private String generateJobStatusScript()
    {
        // Use a text block for simplicity and clarity.
        return
            """
            #!/bin/sh
            #
            # Script to determine status and exit code for a Tapis ZIP runtime job.
            #
            # Process ID to monitor must be passed in as the only argument
            # Example: tapisjob_status.sh 1234
            # This script returns 0 if it can determine the status and 1 if there is an error
            # Status is echoed to stdout as either "RUNNING" or "DONE <exit_code>"
            #
            USAGE1="PID must be first and only argument."
            USAGE2="Usage: tapisjob_status.sh <pid>"
            
            # File that might contain app exit code
            APP_EXITCODE_FILE="tapisjob.exitcode"
            # Regex for checking if input argument is an integer
            RE_INT='^[0-9]+$'
            
            # Process ID to monitor must be passed in as the only argument
            # Check number of arguments.
            if [ $# -ne 1 ]; then
              echo $USAGE1
              echo $USAGE2
              exit 1
            fi
            PID=$1
            
            # Check we have an integer
            if ! [[ $PID =~ $RE_INT ]] ; then
              echo $USAGE1
              echo $USAGE2
              exit 1
            fi
            
            # Determine if application executable process is running
            ps -p $PID >/dev/null
            RET_CODE=$?
            if [ $RET_CODE -eq 0 ]; then
              echo "RUNNING"
              exit 0
            fi
            
            # Process has finished. Determine the exit code.
            EXIT_CODE=$(if test ! -f ./${APP_EXITCODE_FILE}; then echo "0"; else head -n 1 ./${APP_EXITCODE_FILE}; fi)
            echo "DONE $EXIT_CODE"
            exit 0
            """;
    }

    /* ---------------------------------------------------------------------- */
    /* configureRunCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    /*
     * Create and configure the run command.
     */
    private ZipRunCmd configureRunCmd()
     throws TapisException
    {
        // Create and populate the command.
        var zipRunCmd = new ZipRunCmd(_jobCtx);
        
        // ----------------- Tapis Standard Definitions -----------------
        // Set the stdout/stderr redirection file.
        zipRunCmd.setLogConfig(resolveLogConfig());

        // ----------------- User and Tapis Definitions -----------------
        // Set all environment variables.
        zipRunCmd.setEnv(getEnvVariables());

        // Set the application arguments.
        zipRunCmd.setAppArguments(concatAppArguments());

        return zipRunCmd;
    }

    /* ---------------------------------------------------------------------- */
    /* configureAppArchiveInfo:                                               */
    /* ---------------------------------------------------------------------- */
    /** Process containerImage and set attributes describing the app archive
     *
     * @throws  TapisException on error
     */
    private void configureAppArchiveInfo()
     throws TapisException
    {
        String msg;
        // For convenience and clarity set some variables.
        String jobUuid = _job.getUuid();
        _containerImage = _containerImage == null ? "" : _containerImage;
        _containerImageIsUrl = false;
        // Determine the location of the app archive using containerImage as either a path or url.
        // If it starts with "/" then it should an absolute path, else it should be a URL
        if (_containerImage.startsWith("/")) {
            // Process as an absolute path
            msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER", jobUuid, "PATH", _containerImage);
            _log.debug(msg);
            _appArchivePath = FilenameUtils.normalize(_containerImage);
            _appArchiveFile = Path.of(_appArchivePath).getFileName().toString();
        }
        else {
            msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER", jobUuid, "URL", _containerImage);
            _log.debug(msg);
            // Not a path, so should be a URL in a format supported by Files service. Validate it.
            Matcher matcher = JobFileInput.URL_PATTERN.matcher(_containerImage);
            if (!matcher.find())
            {
                msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER_URL_INVALID", jobUuid, _containerImage);
                throw new JobException(msg);
            }
            _containerImageIsUrl = true;
            // Extract and normalize the path in the URL. If no path set then use /
            String urlPathStr = Optional.ofNullable(matcher.group(3)).orElse("/");
            // Get file name from the path and set full path to app archive
            Path urlPath = Path.of(FilenameUtils.normalize(urlPathStr));
            String execDir = JobExecutionUtils.getExecDir(_jobCtx, _job);
            _appArchiveFile = urlPath.getFileName().toString();
            _appArchivePath = Paths.get(execDir, _appArchiveFile).toString();
        }

        // Do simple validation of app archive file name.
        if (StringUtils.isBlank(_appArchiveFile) || "/".equals(_appArchiveFile))
        {
            msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER_FILENAME_ERR", jobUuid, _containerImage, _appArchiveFile);
            throw new JobException(msg);
        }

        // Determine if app archive file is to be process with unzip.
        _appArchiveIsZip = StringUtils.endsWith(_appArchiveFile, ZIP_FILE_EXTENSION);
    }
}
