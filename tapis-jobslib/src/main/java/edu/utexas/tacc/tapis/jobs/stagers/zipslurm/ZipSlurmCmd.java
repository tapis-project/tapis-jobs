package edu.utexas.tacc.tapis.jobs.stagers.zipslurm;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.LogConfig;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerProfile;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public final class ZipSlurmCmd
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                              Constants                                 */
    /* ********************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(ZipSlurmCmd.class);

    // Slurm directive.
    private static final String DIRECTIVE_PREFIX = "#SBATCH ";

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    private List<Pair<String,String>> env = new ArrayList<>();
    private String                    appArguments;
    // Redirect stdout/stderr to a combined file or to separate files in the output directory.
    // The paths in this object are absolute, fully resolved to the job's output directory.
    private LogConfig logConfig;

    // Backpointer to job's current information.
    private final JobExecutionContext _jobCtx;

    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* ZipSlurmCmd:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipSlurmCmd(JobExecutionContext jobCtx) {_jobCtx = jobCtx;}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The ultimate command produced conforms to this template:
        //
        // sbatch [OPTIONS...] tapisjob.sh
        //
        // The generated tapisjob.sh script will contain the SBATCH directives and
        // run the app executable associated with the zip job.
        //
        // In a departure from the usual role this method plays, we only generate
        // the slurm OPTIONS section of the tapisjob.sh script here.  The caller 
        // constructs the complete script.
        return getSBatchDirectives();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() {
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);

        // Write each assignment to the buffer.
        for (var pair : env) {
            // The key always starts the line.
            buf.append(pair.getKey());

            // Are we going to use the short or long form?
            // The short form is just the name of an environment variable
            // that will get exported to the environment ONLY IF it exists
            // in the environment from which the app is run. The long
            // form is key=value.  Note that we don't escape characters in
            // the value.
            var value = pair.getValue();
            if (value != null && !value.isEmpty()) {
                // The long form forces an explicit assignment.
                buf.append("=");
                buf.append(pair.getValue());
            }
            buf.append("\n");
        }

        return buf.toString();
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */

    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public List<Pair<String, String>> getEnv() {
        // Initialized on construction.
        return env;
    }

    public void setEnv(List<Pair<String, String>> env) {
        this.env = env;
    }

    public String getAppArguments() {
        return appArguments;
    }

    public void setAppArguments(String appArguments) {
        this.appArguments = appArguments;
    }

    public LogConfig getLogConfig() {
        return logConfig;
    }

    public void setLogConfig(LogConfig logConfig) {
        this.logConfig = logConfig;
    }
}
