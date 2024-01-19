package edu.utexas.tacc.tapis.jobs.stagers;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public interface JobExecStager 
{
    /** Stage application assets for job. */
    void stageJob() throws TapisException;

    /** This method generates content for the wrapper script tapisjob.sh. */
    String generateWrapperScriptContent() throws TapisException;

    /** This method generates content for the environment variable definition file tapisjob.env. */
    String generateEnvVarFileContent() throws TapisException;

    /** This method creates the JobExecCmd. */
    JobExecCmd createJobExecCmd() throws TapisException;
}
