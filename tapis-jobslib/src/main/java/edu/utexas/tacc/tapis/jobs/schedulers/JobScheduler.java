package edu.utexas.tacc.tapis.jobs.schedulers;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.apache.commons.lang3.StringUtils;

public interface JobScheduler
{
    public String getBatchDirectives();

    public String getModuleLoadCalls() throws JobException;

    public String getBatchJobIdFromOutput(String output, String cmd) throws JobException;
}
