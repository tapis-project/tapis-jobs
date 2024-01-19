package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.submit.LogConfig;

public final class SingularityRunCmd 
 extends AbstractSingularityExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Fields specific to instance start. 
    private String          app;      // an application to run inside a container
    private boolean         ipc;      // run container in a new IPC namespace
    private boolean         noNet;    // disable VM network handling
    private boolean         pid;      // run container in a new PID namespace
    private String          pwd;      // initial working directory for payload process inside the container
    private boolean         vm;       // enable VM support
    private String          vmCPU;    // number of CPU cores to allocate to Virtual Machine
    private boolean         vmErr;    // enable attaching stderr from VM
    private String          vmIP;     // IP Address to assign for container usage, default is DHCP in bridge network
    private String          vmRAM;    // amount of RAM in MiB to allocate to Virtual Machine (default "1024")

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
        buf.append(getEnvFile());
        
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
        // This should never happen since tapis variables are always specified.
        if (getEnv().isEmpty()) return null;
        
        // Create the key=value records, one per line.
        return getPairListArgs(getEnv());
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getCmdTextWithEnvVars:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Create a command string appropriate for used with batch schedulers like
     * Slurm.  The generated text inlines the environment variables that would
     * be segregated into an environment file in non-batch executions.
     * 
     * @param job the current job.
     * @return the command text.
     */
    protected String getCmdTextWithEnvVars(Job job)
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
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addRunSpecificArgs:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Add the container arguments that are specific to singularity run
     * 
     * @param buf the command buffer
     */
    private void addRunSpecificArgs(StringBuilder buf)
    {
        if (StringUtils.isNotBlank(getApp())) {
            buf.append(" --app ");
            buf.append(getApp());
        }

        if (isIpc()) buf.append(" --ipc");
        if (isNoNet()) buf.append(" --nonet");
        if (isPid()) buf.append(" --pid");

        if (StringUtils.isNotBlank(getPwd())) {
            buf.append(" --pwd ");
            buf.append(conditionalQuote(getPwd()));
        }
 
        if (isVm()) buf.append(" --vm");
        if (StringUtils.isNotBlank(getVmCPU())) {
            buf.append(" --vm-cpu ");
            buf.append(conditionalQuote(getVmCPU()));
        }
        if (isVmErr()) buf.append(" --vm-err");
        if (StringUtils.isNotBlank(getVmIP())) {
            buf.append(" --vm-ip ");
            buf.append(conditionalQuote(getVmIP()));
        }
        if (StringUtils.isNotBlank(getVmRAM())) {
            buf.append(" --vm-ram ");
            buf.append(conditionalQuote(getVmRAM()));
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* addRunSpecificArgs:                                                    */
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
    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public boolean isIpc() {
        return ipc;
    }

    public void setIpc(boolean ipc) {
        this.ipc = ipc;
    }

    public boolean isNoNet() {
        return noNet;
    }

    public void setNoNet(boolean noNet) {
        this.noNet = noNet;
    }
    
    public boolean isPid() {
        return pid;
    }

    public void setPid(boolean pid) {
        this.pid = pid;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public boolean isVm() {
        return vm;
    }

    public void setVm(boolean vm) {
        this.vm = vm;
    }

    public String getVmCPU() {
        return vmCPU;
    }

    public void setVmCPU(String vmCPU) {
        this.vmCPU = vmCPU;
    }

    public boolean isVmErr() {
        return vmErr;
    }

    public void setVmErr(boolean vmErr) {
        this.vmErr = vmErr;
    }

    public String getVmIP() {
        return vmIP;
    }

    public void setVmIP(String vmIP) {
        this.vmIP = vmIP;
    }

    public String getVmRAM() {
        return vmRAM;
    }

    public void setVmRAM(String vmRAM) {
        this.vmRAM = vmRAM;
    }

	public LogConfig getLogConfig() {
		return logConfig;
	}

	public void setLogConfig(LogConfig logConfig) {
		this.logConfig = logConfig;
	}
}
