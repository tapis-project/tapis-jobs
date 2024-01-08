package edu.utexas.tacc.tapis.jobs.schedulers;

import static edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager._optionPattern;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/*
 * Class to support scheduling a BATCH type job via slurm.
 * Currently only used for Zip jobs.
 */
public final class SlurmScheduler 
 extends AbstractSlurmOptions
 implements JobScheduler
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SlurmScheduler.class);

    // Initialize the regex pattern that extracts the ID slurm assigned to the job.
    // The regex ignores leading and trailing whitespace and groups the numeric ID.
    private static final Pattern RESULT_PATTERN = Pattern.compile("\\s*Submitted batch job (\\d+)\\s*");

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Convenient access to job.
    private final Job _job;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SlurmScheduler(JobExecutionContext jobCtx)
            throws TapisException
    {
    	super(jobCtx);
        _job = _jobCtx.getJob();
         setUserSlurmOptions();
         setTapisOptionsForSlurm();
    }

    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SlurmScheduler(JobExecutionContext jobCtx, String defaultJobName)
            throws TapisException
    {
    	super(jobCtx);
        _job = _jobCtx.getJob();
        setUserSlurmOptions();
        setTapisOptionsForSlurm();
        setJobName(defaultJobName);
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getBatchDirectives:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    public String getBatchDirectives() {return super.getBatchDirectives();}
    
    /* ---------------------------------------------------------------------- */
    /* getModuleLoadCalls:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    public String getModuleLoadCalls() throws JobException
    {
        // There's nothing to do unless a Tapis profile was specified.
        if (StringUtils.isBlank(getTapisProfile())) return "";

        final int capacity = 1024;
        var buf = new StringBuilder(capacity);

        // Make sure we retrieve the profile.
        var profile = _jobCtx.getSchedulerProfile(getTapisProfile());

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
        // We have a problem if the result has no content.
        if (StringUtils.isBlank(output)) {
            String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_NO_RESULT",
                    _job.getUuid(), cmd);
            throw new JobException(msg);
        }

        // Strip any banner information from the remote result.
        output = JobUtils.getLastLine(output.strip());

        // Look for the success message
        Matcher m = RESULT_PATTERN.matcher(output);
        var found = m.matches();
        if (!found) {
            String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_INVALID_RESULT",
                    _job.getUuid(), output);
            throw new JobException(msg);
        }

        int groupCount = m.groupCount();
        if (groupCount < 1) {
            String msg = MsgUtils.getMsg("JOBS_SLURM_SBATCH_INVALID_RESULT",
                    _job.getUuid(), output);
            throw new JobException(msg);
        }

        // Group 1 contains the slurm ID.
        return m.group(1);
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* setUserSlurmOptions:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Set the slurm options that we allow the user to modify.
     *
     */
    private void setUserSlurmOptions()
            throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getSchedulerOptions();
        if (opts == null || opts.isEmpty()) return;

        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
                throw new JobException(msg);
            }

            // Get the option and its value if one is provided.
            String option = null;
            String value  = ""; // default value when none provided
            int groupCount = m.groupCount();
            if (groupCount > 0) option = m.group(1);
            if (groupCount > 1) value  = m.group(2);

            // The option should always exist.
            if (StringUtils.isBlank(option)) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
                throw new JobException(msg);
            }

            // Save the parsed value.
            assignCmd(option, value);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* setTapisOptionsForSlurm:                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the standard Tapis settings for Slurm.
     *
     * @throws TapisException on error
     */
    private void  setTapisOptionsForSlurm()
            throws TapisException
    {
        // --------------------- Tapis Mandatory ---------------------
        // Request the total number of nodes from slurm.
        setNodes(Integer.toString(_job.getNodeCount()));

        // Tell slurm the total number of tasks to run.
        setNtasks(Integer.toString(_job.getTotalTasks()));

        // Tell slurm the total runtime of the application in minutes.
        setTime(Integer.toString(_job.getMaxMinutes()));

        // Tell slurm the memory per node requirement in megabytes.
        setMem(Integer.toString(_job.getMemoryMB()));

        // We've already checked in JobQueueProcessor before processing any
        // state changes that the logical and hpc queues have been assigned.
        var logicalQueue = _jobCtx.getLogicalQueue();
        setPartition(logicalQueue.getHpcQueueName());

        // --------------------- Tapis Optional ----------------------
        // Always assign a job name
        // If user has not specified one then use a default, "tapisjob.sh"
        if (StringUtils.isBlank(getJobName())) {
            setJobName(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
        }

        // Assign the standard tapis output file name if one is not
        // assigned and we are not running an array job.  We let slurm
        // use its default naming scheme for array job output files.
        // Unless the user explicitly specifies an error file, both
        // stdout and stderr will go the designated output file.
        if (StringUtils.isBlank(getOutput()) &&
                StringUtils.isBlank(getArray())) {
            var fm = _jobCtx.getJobFileManager();
            setOutput(fm.makeAbsExecSysOutputPath(JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE));
        }
    }

    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    @Override
    public void setJobName(String jobName) {
        // Use either name provided or "tapisjob.sh" as default fallback.
        jobName = (StringUtils.isBlank(jobName)) ? JobExecutionUtils.JOB_WRAPPER_SCRIPT : jobName;
        // Local (i.e., TACC) policies may not allow colons in name.
        getDirectives().put("--job-name", jobName.replace(':', '-'));
    }
}
