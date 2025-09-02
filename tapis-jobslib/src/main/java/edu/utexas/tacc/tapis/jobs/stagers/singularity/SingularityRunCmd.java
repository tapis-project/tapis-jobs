package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.alwaysSingleQuote;

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.LogConfig;

public final class SingularityRunCmd 
 extends AbstractSingularityExecCmd
 implements ISingularityRun
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */

    // Redirect stdout/stderr to a combined file or to separate files in the output directory.
    // The paths in this object are absolute, fully resolved to the job's output directory.
    private LogConfig       logConfig;
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The generated wrapper script will contain a singularity run command:
        //
        //   singularity run [run options...] <container> [args] > tapisjob.out 2>&1 &
        
        // ------ Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // ------ Start filling in the options that are tapis-only assigned.
        buf.append("# Issue background singularity run command and protect from dropped\n");
        buf.append("# terminal sessions by nohup.  Send stdout and stderr to file.\n");
        buf.append("# Format: singularity run [options...] <container> [args] > tapisjob.out 2>&1 &\n");
        
        buf.append("nohup singularity run");
        buf.append(" --env-file ");
        buf.append(alwaysSingleQuote(getEnvFile()));
        
        // ------ Fill in the common user-specified arguments.
        addCommonExecArgs(buf);
        
        // ------ Fill in command-specific user-specified arguments.
        addRunSpecificArgs(buf);
        
        // ------ Assign image.
        buf.append(" ");
        buf.append(getImage());

        // ------ Assign application arguments.
        if (!StringUtils.isBlank(getAppArguments()))
            buf.append(getAppArguments()); // begins with space char
        
        // ------ Run as a background command with stdout/stderr redirected to a file.
        addOutputRedirection(buf);
        
        // ------ Collect the PID of the background process.
        buf.append("# Capture and return the PID of the background process.\n");
        buf.append("pid=$!\n");
        buf.append("echo $pid");

        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() 
    {
        return JobUtils.generateEnvVarFileContentForSingularity(getEnv(), false);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */

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
    		buf.append(conditionalQuote(getLogConfig().getStdoutFilename()));
    		buf.append(" 2>&1 &\n\n");
    	} else {
    		buf.append(" 2> ");
    		buf.append(conditionalQuote(getLogConfig().getStderrFilename()));
    		buf.append(" 1> ");
    		buf.append(conditionalQuote(getLogConfig().getStdoutFilename()));
    		buf.append(" &\n\n");
    	}
    }

    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
	public LogConfig getLogConfig() {
		return logConfig;
	}

	public void setLogConfig(LogConfig logConfig) {
		this.logConfig = logConfig;
	}
}
