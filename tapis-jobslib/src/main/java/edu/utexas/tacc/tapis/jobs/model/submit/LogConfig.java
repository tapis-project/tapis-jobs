package edu.utexas.tacc.tapis.jobs.model.submit;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;

public class LogConfig 
{
	// Default is to merge stdout and stderr into a single file.
	// These values are used in some runtimes and ignored in others.
    private String  stdoutFilename;
    private String  stderrFilename;
    
    public void setToDefault() {
    	stdoutFilename = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
    	stderrFilename = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
    }
    
    public boolean isComplete() {
    	return !StringUtils.isBlank(stdoutFilename) && !StringUtils.isBlank(stderrFilename);
    }
    
    public boolean canMerge() {
    	// This method shouldn't be called until the object is complete,
    	// but just the same we avoid blowing up. If the out and err files
    	// are the same the intent is to target a single file.
    	if (stdoutFilename == null) return false;
    	return stdoutFilename.equals(stderrFilename);
    }
    
    // Accessors
	public String getStdoutFilename() {
		return stdoutFilename;
	}
	public void setStdoutFilename(String stdoutFilename) {
		this.stdoutFilename = stdoutFilename;
	}
	public String getStderrFilename() {
		return stderrFilename;
	}
	public void setStderrFilename(String stderrFilename) {
		this.stderrFilename = stderrFilename;
	}
}
