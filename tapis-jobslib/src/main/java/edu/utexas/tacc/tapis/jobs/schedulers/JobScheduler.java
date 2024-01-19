package edu.utexas.tacc.tapis.jobs.schedulers;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;

public interface JobScheduler
{
    String getBatchDirectives();

    String getModuleLoadCalls() throws JobException;

    String getBatchJobIdFromOutput(String output, String cmd) throws JobException;
}
