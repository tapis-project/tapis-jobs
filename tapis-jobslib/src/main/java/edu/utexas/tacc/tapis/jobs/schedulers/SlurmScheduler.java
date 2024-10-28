package edu.utexas.tacc.tapis.jobs.schedulers;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/*
 * Class to support scheduling a BATCH type job via slurm.
 */
public final class SlurmScheduler 
 implements JobScheduler
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SlurmScheduler.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Backpointer to job's current information.
    private final JobExecutionContext _jobCtx;
    // Convenient access to job.
    private final Job _job;
    private final SlurmOptions _slurmOptions;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SlurmScheduler(JobExecutionContext jobCtx)
            throws TapisException
    {
        _jobCtx = jobCtx;
        _job = _jobCtx.getJob();
        _slurmOptions = new SlurmOptions(jobCtx);
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getBatchDirectives:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    public String getBatchDirectives() {return _slurmOptions.getBatchDirectives();}
    
    /* ---------------------------------------------------------------------- */
    /* getModuleLoadCalls:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    public String getModuleLoadCalls() throws JobException
    {
        // There's nothing to do unless a Tapis profile was specified.
        if (StringUtils.isBlank(_slurmOptions.getTapisProfile())) return "";

        final int capacity = 1024;
        var buf = new StringBuilder(capacity);

        // Make sure we retrieve the profile.
        var profile = _jobCtx.getSchedulerProfile(_slurmOptions.getTapisProfile());

        // Get the array of module load specs.
        var specs = profile.getModuleLoads();
        if (specs == null || specs.isEmpty()) return "";

        // Iterate through the list of specs.
        for (var spec : specs) {
            // There has to be a load command.
            var loadCmd = spec.getModuleLoadCommand();
            if (StringUtils.isBlank(loadCmd)) continue;

            // We allow commands that don't require module parameters.
            var modules = spec.getModulesToLoad();
            if (modules == null || modules.isEmpty()) {
                buf.append(loadCmd).append("\n");
                continue;
            }

            // Put in the required spacing.
            if (!loadCmd.endsWith(" ")) loadCmd += " ";

            // Create a module load command for each specified module.
            for (var module : modules)
                if (StringUtils.isNotBlank(module))
                    buf.append(loadCmd).append(module).append("\n");
        }

        // End with a blank line.
        buf.append("\n");
        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* getBatchIdFromOutput:                                                  */
    /* ---------------------------------------------------------------------- */
    @Override
    public String getBatchJobIdFromOutput(String output, String cmd) throws JobException
    {
    	return JobUtils.getSlurmId(_job, output, cmd);
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */

    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */

}
