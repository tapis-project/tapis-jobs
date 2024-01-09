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
    private String _zipFullPath; // Full path to zip/tar archive file, including filename.
    private String _zipFileName; // Name of zip/tar archive file.
    private boolean _hasZipExtension; // True if archive file ends with .zip
    private String  _containerImage;
    private boolean _containerImageIsUrl;
    private final JobScheduler _scheduler;
    private final boolean _isBatch;

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
        // Configure the zip file properties
        configureZipFileInfo();
        // Set the scheduler properties as needed.
        if (schedulerType == null) {
            _scheduler = null;
        } else if (SchedulerTypeEnum.SLURM.equals(schedulerType)) {
            // NOTE: Once other schedulers are supported create the appropriate scheduler
            _scheduler = new SlurmScheduler(jobCtx, _zipFileName);
        } else {
            String msg = MsgUtils.getMsg("TAPIS_UNSUPPORTED_APP_RUNTIME", schedulerType, "ZipStager");
            throw new JobException(msg);
        }
        _isBatch = (schedulerType != null);
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
    /*
     * Stage application assets:
     *  1. Check that the UNZIP command is available on the exec host.
     *  2. Stage the zip/tar file. Will be a no-op if containerImage is an absolute path,
     *     otherwise it will be a file transfer.
     *  3. Run command to extract the zip/tar file into the execSystemExecDir
     *  4. Run command to determine the app executable.
     *  5. Create and install the wrapper script tapisjob.sh
     *  6. Create and install the environment variable file tapisjob.env
     */
    public void stageJob() throws TapisException
    {
        // Get the job file manager used to make directories, upload files, transfer files, etc.
        var jobFileManager = _jobCtx.getJobFileManager();

        // 1. If archive file is to be processed using unzip then make sure it is available on the exec host
        if (_hasZipExtension) jobFileManager.checkForCommand(UNZIP_COMMAND);

        // 2. Stage the app archive. This may involve a transfer
        jobFileManager.stageAppAssets(_containerImage, _containerImageIsUrl, _zipFileName);

        // 3. Run a remote command to extract the application archive file into execSystemExecDir.
        jobFileManager.extractZipArchive(_zipFullPath, _hasZipExtension);

        // Now that app archive is unpacked, we can determine the app executable
        // 4. Get the relative path to the app executable.
        // Generate and run a script to determine the executable
        String setAppExecutableScript = generateSetAppExecutableScript();
        jobFileManager.installExecFile(setAppExecutableScript, JobExecutionUtils.JOB_ZIP_SET_EXEC_SCRIPT, JobFileManager.RWXRWX);
        String appExecPath = jobFileManager.runZipSetAppExecutable(JobExecutionUtils.JOB_ZIP_SET_EXEC_SCRIPT);
        _zipRunCmd.setAppExecPath(appExecPath);

        // 5. Create and install the wrapper script: tapisjob.sh
        String wrapperScript = generateWrapperScript();
        jobFileManager.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);

        // 6. Create and install the environment variable definition file: tapisjob.env
        String envVarFile = generateEnvVarFile();
        jobFileManager.installExecFile(envVarFile, JobExecutionUtils.JOB_ENV_FILE, JobFileManager.RWRW);
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
        if (_isBatch) initBashBatchScript(); else initBashScript();

        // If a BATCH job add the directives and any module load commands.
        if (_isBatch) {
            _cmd.append(_scheduler.getBatchDirectives());
            _cmd.append(_scheduler.getModuleLoadCalls());
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
            #!/bin/bash
            #
            # Script to determine Tapis application executable for application defined as runtime type of ZIP.
            # If successful echo result and exit with 0.
            # If unsuccessful echo a message and exit with 1 or 2.
            #
            # Set a default
            APP_EXEC="tapisjob_app.sh"
            
            # If default does not exist but manifest file is present then search manifest file
            if [ ! -f "./tapisjob_app.sh" ] && [ -f "./tapisjob.manifest" ]; then
              APP_EXEC=$(grep -v "^#" tapisjob.manifest | grep "^tapisjob_executable=" | sed -E 's/(.*)=//')
            fi
            
            # Check for errors. If all OK then ensure that file is executable.
            if [ -z "${APP_EXEC}" ]; then
              echo "ERROR: Unable to determine application executable."
              echo "ERROR: Please provide tapisjob_app.sh or a manifest specifying tapisjob_executable."
              exit 1
            elif [ ! -f "./${APP_EXEC}" ]; then
              echo "ERROR: Looking for application executable = $APP_EXEC but file does not exist."
              exit 2
            else
              chmod +x "./${APP_EXEC}"
            fi
            echo "${APP_EXEC}"
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
    /* configureZipFileInfo:                                               */
    /* ---------------------------------------------------------------------- */
    /** Process containerImage and set attributes describing the app archive
     *
     * @throws  TapisException on error
     */
    private void configureZipFileInfo()
     throws TapisException
    {
        String msg;
        // For convenience and clarity set some variables.
        String jobUuid = _job.getUuid();
        // Make sure containerImage is not null.
        if (_containerImage == null)  _containerImage = "";
        _containerImageIsUrl = false;
        // Determine the location of the app archive using containerImage as either a path or url.
        // If it starts with "/" then it should an absolute path, else it should be a URL
        if (_containerImage.startsWith("/")) {
            // Process as an absolute path
            msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER", jobUuid, "PATH", _containerImage);
            _log.debug(msg);
            _zipFullPath = FilenameUtils.normalize(_containerImage);
            _zipFileName = Path.of(_zipFullPath).getFileName().toString();
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
            _zipFileName = urlPath.getFileName().toString();
            _zipFullPath = Paths.get(execDir, _zipFileName).toString();
        }

        // Do simple validation of app archive file name.
        if (StringUtils.isBlank(_zipFileName) || "/".equals(_zipFileName))
        {
            msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER_FILENAME_ERR", jobUuid, _containerImage, _zipFileName);
            throw new JobException(msg);
        }

        // Determine if app archive file is to be process with unzip.
        _hasZipExtension = StringUtils.endsWith(_zipFileName, ZIP_FILE_EXTENSION);
    }
}
