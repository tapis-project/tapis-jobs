package edu.utexas.tacc.tapis.jobs.stagers.zip;

import edu.utexas.tacc.tapis.jobs.model.submit.LogConfig;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Zip run command object.
    private final ZipRunCmd _zipRunCmd;

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

        // Stage the app archive. This may involve a transfer
        String appArchivePath = jobFileManager.stageAppAssets();

        // Run a remote command to extract the application archive file into execSystemExecDir.
        jobFileManager.extractAppArchive(appArchivePath);

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
}
