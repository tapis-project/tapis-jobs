package edu.utexas.tacc.tapis.jobs.model.submit;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import io.swagger.v3.oas.annotations.media.Schema;

public class LogConfig 
{
	// Default is to merge stdout and stderr into a single file.
    private String  stdoutFilename;
    private String  stderrFilename;
    
    @Schema(hidden = true)
    public void safeInit() {
    	if (StringUtils.isBlank(stdoutFilename)) stdoutFilename = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
    	if (StringUtils.isBlank(stderrFilename)) stderrFilename = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
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
