package edu.utexas.tacc.tapis.jobs.stagers.zip;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInput;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;

/** This class handles staging of application assets for applications
 * defined using runtime type of ZIP.
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

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Create and populate the command.
        super(jobCtx);
        _zipRunCmd = configureRunCmd();
        _containerImage = _jobCtx.getApp().getContainerImage();
        configureAppArchiveInfo();
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
        jobFileManager.stageAppAssets(_containerImage, _containerImageIsUrl);

        // Run a remote command to extract the application archive file into execSystemExecDir.
        jobFileManager.extractAppArchive(_appArchivePath);

        // Now that app archive is unpacked we can determine the app executable
        // First generate the script that we will run to determine the executable.
        String setAppExecutableScript = generateSetAppExecutableScript();

        // Install the script. Creates tapisjob_setexec.sh
        jobFileManager.installExecFile(setAppExecutableScript, JobExecutionUtils.JOB_ZIP_SET_EXEC_FILE, JobFileManager.RWXRWX);

        // Run the script to determine the executable. Creates tapisjob.exec
        jobFileManager.runSetAppExecutable(JobExecutionUtils.JOB_ZIP_SET_EXEC_FILE);

        // Create the wrapper script.
        String wrapperScript = generateWrapperScript();

        // Install the wrapper script on the execution system. Creates tapisjob.sh
        jobFileManager.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);

        // Create the environment variable definition file.
        String envVarFile = generateEnvVarFile();

        // Install the exported env variable file. Creates tapisjob.env
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
    protected String generateWrapperScript()
     throws TapisException
    {
        // Construct the command.
        String zipCmd = _zipRunCmd.generateExecCmd(_job);

        // Build the command file content.
        initBashScript();

        // Add the command to the command file.
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
            if [ -z "$APP_EXEC" ]; then
              echo "ERROR: Unable to determine application executable"
              echo "ERROR: Please provide tapisjob_app.sh or a manifest specifying tapisjob_executable."
              exit 1
            elif [ ! -f "./$APP_EXEC" ]; then
              echo "ERROR: Looking for application executable = $APP_EXEC but file does not exist"
              exit 2
            else
              chmod +x "./$APP_EXEC"
            fi
            echo "Found application executable = $APP_EXEC"
            echo "$APP_EXEC" > "./tapisjob.exec"
            """;
    }

    /* ---------------------------------------------------------------------- */
    /* configureRunCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    private ZipRunCmd configureRunCmd()
     throws TapisException
    {
        // Create and populate the command.
        var zipRunCmd = new ZipRunCmd();
        
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
            String rootDir = Optional.ofNullable(_jobCtx.getExecutionSystem().getRootDir()).orElse("/");
            _appArchiveFile = urlPath.getFileName().toString();
            _appArchivePath = Paths.get(rootDir, _job.getExecSystemExecDir(), _appArchiveFile).toString();
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
