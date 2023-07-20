package edu.utexas.tacc.tapis.jobs.model.submit;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;

public class LogConfig 
{
	// Default is to merge stdout and stderr into a single file.
    private String  stdoutFilename = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
    private String  stderrFilename = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
    
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
