package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.alwaysSingleQuote;

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;

public final class SingularityStartCmd 
 extends AbstractSingularityExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Fields specific to instance start. 
    private String                    name;    // Instance name.      
    private String                    pidFile; // Tapis hardcoded file path for instance pid
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The generated wrapper script will contain a singularity start command:
        //
        //  singularity instance start [start options...] <container path> <instance name> [startscript args...]
        
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // ------ Start filling in the options that are tapis-only assigned.
        buf.append("# Issue singularity instance start command and write PID to file.\n");
        buf.append("# Format: singularity instance start [options] <container path> <instance name> [app args]\n");
        
        var p = job.getMpiOrCmdPrefixPadded(); // empty or string w/trailing space
        buf.append(p).append("singularity instance start");
        buf.append(" --env-file ");
        buf.append(alwaysSingleQuote(getEnvFile()));
        buf.append(" --pid-file ");
        buf.append(alwaysSingleQuote(getPidFile()));
        
        // ------ Fill in the common user-specified arguments.
        addCommonExecArgs(buf);
        
        // ------ Assign image.
        buf.append(" ");
        buf.append(conditionalQuote(getImage()));
        
        // ------ Assign instance name.
        buf.append(" ");
        buf.append(getName()); // Name is safe since we set it

        // ------ Assign application arguments.
        if (!StringUtils.isBlank(getAppArguments()))
            buf.append(getAppArguments()); // begins with space char
        
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
    /*                               Accessors                                */
    /* ********************************************************************** */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPidFile() {
        return pidFile;
    }

    public void setPidFile(String pidFile) {
        this.pidFile = pidFile;
    }
}
