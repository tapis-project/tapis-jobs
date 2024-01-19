package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

import java.util.List;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

public final class SingularityRunSlurmCmd
        extends AbstractSingularityExecCmd
{
    /* ********************************************************************** */
    /*                              Constants                                 */
    /* ********************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmCmd.class);
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */

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
        buf.append(p).append("singularity run");

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
        return JobUtils.generateEnvVarFileContentForSingularity(getEnv(), true);
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getEnvArg:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Create the string of key=value pairs separated by commas.  It's assumed
     * that the values are NOT already double quoted.
     *
     * NOTE: We always double quote the value whether or not it contains
     *       dangerous characters, which is different that most other cases
     *       of environment variable construction.  Most of the time we use
     *       TapisUtils.conditionalQuote(), which will only double quote when
     *       dangerous or space characters are present.
     *
     * @param pairs NON-EMPTY list of pair values, one per occurrence
     * @return the string that contains all assignments
     */
    private String getEnvArg(List<Pair<String,String>> pairs)
    {
        // Get a buffer to accumulate the key/value pairs.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);

        // Create the string " --env key=value[,key=value]".
        boolean first = true;
        for (var v : pairs) {
            if (first) {buf.append(" --env "); first = false;}
            else buf.append(",");
            buf.append(v.getLeft());
            buf.append("=");
            buf.append(TapisUtils.safelyDoubleQuoteString(v.getRight()));
        }
        return buf.toString();
    }

}
