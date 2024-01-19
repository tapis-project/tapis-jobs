package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.schedulers.AbstractSlurmOptions;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

public final class SingularityRunSlurmCmd
// extends AbstractSlurmOptions
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                              Constants                                 */
    /* ********************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmCmd.class);
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* Constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunSlurmCmd(JobExecutionContext jobCtx) {super(jobCtx);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The generated singularity run command text:
        //
        //   singularity run [run options...] <container> [args]

        // ------ Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);

        // ------ Start the command text.
        var p = job.getMpiOrCmdPrefixPadded(); // empty or string w/trailing space
        buf.append(p + "singularity run");

        // ------ Fill in environment variables.
        buf.append(getEnvArg(getEnv()));

        // ------ Fill in the common user-specified arguments.
        addCommonExecArgs(buf);

        // ------ Fill in command-specific user-specified arguments.
        addRunSpecificArgs(buf);

        // ------ Assign image.
        buf.append(" ");
        buf.append(conditionalQuote(getImage()));

        // ------ Assign application arguments.
        if (!StringUtils.isBlank(getAppArguments()))
            buf.append(getAppArguments()); // begins with space char

        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() {
        // We pass environment variables to singularity from the command line
        // so that they are embedded in the wrapper script sent to Slurm. 
        //
        // This method should not be called.
        String msg = MsgUtils.getMsg("JOBS_SCHEDULER_GENERATE_ERROR", "slurm");
        throw new TapisRuntimeException(msg);
    }
}
