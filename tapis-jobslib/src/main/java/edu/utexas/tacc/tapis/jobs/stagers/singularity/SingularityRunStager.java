package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public class SingularityRunStager
 extends AbstractSingularityStager
 implements ISingularityRun
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunStager.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singularity run command object.
    private final SingularityRunCmd _singularityCmd;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Set _jobCtx, _job, _cmdBuilder, _isBatch, _jobExecCmd
        super(jobCtx, null /* schedulerType */);
        // The docker specific exec command
        _singularityCmd = (SingularityRunCmd) _jobExecCmd;
    }

    /* ********************************************************************** */
    /*                          Public Methods                                */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateWrapperScriptContent() throws TapisException
    {
        // The generated wrapper script will contain a singularity instance
        // start command that conforms to this format:
        //
        //  singularity run [run options...] <container> [args] > tapisjob.out 2>&1 &
        String cmdText = _singularityCmd.generateExecCmd(_job);

        // Build the command file content.
        initBashScript();

        // Add the command to the content
        _cmdBuilder.append(cmdText);

        return _cmdBuilder.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() throws TapisException
    {
        return _singularityCmd.generateEnvVarFileContent();
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
        return configureExecCmd();
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureExecCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    private SingularityRunCmd configureExecCmd()
     throws TapisException
    {
        // Create and populate the singularity command.
        var singularityCmd = new SingularityRunCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Write all the environment variables to file.
        singularityCmd.setEnvFile(makeEnvFilePath());
        
        // Set the image.
        singularityCmd.setImage(_jobCtx.getApp().getContainerImage());
        
        // Set the stdout/stderr redirection file.
        singularityCmd.setLogConfig(resolveLogConfig());

        // ----------------- User and Tapis Definitions -----------------
        // Set all environment variables.
        singularityCmd.setEnv(getEnvVariables());

        // Set the singularity options.
        setSingularityOptions(singularityCmd);
        
        // Set the application arguments.
        singularityCmd.setAppArguments(concatAppArguments());
                
        return singularityCmd;
    }
}
