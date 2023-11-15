package edu.utexas.tacc.tapis.jobs.stagers.zip;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.LogConfig;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static edu.utexas.tacc.tapis.jobs.stagers.zip.ZipStager.ENV_FILE;
import static edu.utexas.tacc.tapis.jobs.stagers.zip.ZipStager.PID_FILE;

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
    private List<Pair<String,String>> env = new ArrayList<Pair<String,String>>();
    private String                    envFile;
    private String                    appExecutable;
    private String                    appArguments;
    // Redirect stdout/stderr to a combined file or to separate files in the output directory.
    // The paths in this object are absolute, fully resolved to the job's output directory.
    private LogConfig                 logConfig;


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
        // Format:
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
        buf.append("export $(cat ./").append(ENV_FILE).append(" | xargs)\n\n");

        // Run the executable using nohup
        buf.append("# Launch app executable and capture PID of background process.\n");
        buf.append("nohup ").append("./").append(appExecutable);

        // ------ Append the application arguments.
        if (!StringUtils.isBlank(appArguments))
            buf.append(appArguments); // begins with space char

        // ------ Add stdout/stderr redirection.
        addOutputRedirection(buf);

        // ------ Run as a background process and capture the pid.
        buf.append(" &");
        buf.append("pid=$!\n");
        buf.append("echo $pid > ./").append(PID_FILE);
        return buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() 
    {
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
            buf.append(getLogConfig().getStdoutFilename());
            buf.append(" 2>&1");
        } else {
            buf.append(" 2> ");
            buf.append(getLogConfig().getStderrFilename());
            buf.append(" 1> ");
            buf.append(getLogConfig().getStdoutFilename());
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

    public String getAppExecutable() {
        return appExecutable;
    }

    public void setAppExecutable(String appExecutable) {
        this.appExecutable = appExecutable;
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
