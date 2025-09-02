package edu.utexas.tacc.tapis.jobs.stagers.zip;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.model.submit.LogConfig;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.JOB_ZIP_PID_FILE;
import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.JOB_ENV_FILE;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.alwaysSingleQuote;

/** This class represents the bash compatible shell command used to launch an application
 * defined using runtime type of ZIP.
 */
public final class ZipRunCmd
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    private List<Pair<String,String>> env = new ArrayList<>();
    private String                    appArguments;
    // Redirect stdout/stderr to a combined file or to separate files in the output directory.
    // The paths in this object are absolute, fully resolved to the job's output directory.
    private LogConfig                 logConfig;

    // Relative path to app executable. Relative to execSystemExecDir.
    private String                    appExecPath;

    // Job info, needed for batch jobs
    private final JobExecutionContext _jobCtx;

    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* ZipRunCmd:                                                             */
    /* ---------------------------------------------------------------------- */
    public ZipRunCmd(JobExecutionContext jobCtx) {_jobCtx = jobCtx;}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // Generate the command that will launch either tapisjob_app.sh or the executable from the manifest file.
        // tapisjob.env contains all environment variables
        // Example format:
        //   export $(cat ./tapisjob.env | xargs)
        //   nohup ./tapisjob_app.sh <appArgs> > tapisjob.out 2>&1 &
        //   pid=$!
        //   echo $pid > ./tapisjob.pid
        
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        buf.append("# Issue launch command for application executable.\n");
        buf.append("# Format: nohup ./tapisjob_app.sh > tapisjob.out 2>&1 &\n\n");

        // Export environment variables from file
        buf.append("# Export Tapis and user defined environment variables.\n");
        buf.append(". ./").append(JOB_ENV_FILE).append("\n\n");

        // Run the executable
        buf.append("# Launch app executable.\n");

        // Build the single line command that will run the executable
        // START -----------------------------------------------------------------
        // If FORK run in background
        if (JobType.FORK.equals(job.getJobType())) buf.append("nohup ");
        // ------ Start the command text.
        var p = job.getMpiOrCmdPrefixPadded(); // empty or string w/trailing space
        buf.append(p);
        // The actual app executable.
        buf.append("./").append(alwaysSingleQuote(appExecPath));
        // ------ Append the application arguments.
        if (!StringUtils.isBlank(appArguments))
            buf.append(appArguments); // begins with space char
        // ------ Add stdout/stderr redirection.
        addOutputRedirection(buf);
        // If FORK run in background and capture pid, else run in foreground.
        if (JobType.FORK.equals(job.getJobType())) {
            buf.append(" &\n");
            // ------ Capture the pid
            buf.append("pid=$!\n");
            buf.append("echo $pid > ./").append(JOB_ZIP_PID_FILE).append('\n');
            // Echo the pid to stdout so launcher can capture it
            // This should be the only output
            buf.append("echo $pid\n");
        } else {
            buf.append("\n");
        }
        // END -------------------------------------------------------------------

        return buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() 
    {
        return JobUtils.generateEnvVarFileContentForZip(env);
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addOutputRedirection:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Add the stdout and stderr redirection to either a single combined file
     * or to two separate files. Append the background operator and proper new
     * line spacing.
     *
     * @param buf the command buffer
     */
    private void addOutputRedirection(StringBuilder buf)
    {
        if (getLogConfig().canMerge()) {
            buf.append(" > ");
            buf.append(alwaysSingleQuote(getLogConfig().getStdoutFilename()));
            buf.append(" 2>&1");
        } else {
            buf.append(" 2> ");
            buf.append(alwaysSingleQuote(getLogConfig().getStderrFilename()));
            buf.append(" 1> ");
            buf.append(alwaysSingleQuote(getLogConfig().getStdoutFilename()));
        }
    }

    /* ********************************************************************** */
    /*                          Top-Level Accessors                           */
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

    public String getAppExecPath() {
        return appExecPath;
    }

    public void setAppExecPath(String appExecPath) {
        this.appExecPath = appExecPath;
    }
}
