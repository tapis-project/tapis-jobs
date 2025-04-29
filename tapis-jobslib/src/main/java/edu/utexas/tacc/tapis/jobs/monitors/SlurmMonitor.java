package edu.utexas.tacc.tapis.jobs.monitors;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.SlurmStatusType;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Slurm job monitoring class.
 *
 *  Response is determined using either the primary monitoring squeue command:
 *     squeue --noheader -O 'jobid,state,exit_code' -j${JOBID} 2>/dev/null
 *
 *     Example of response returned by squeue:
 *          "4213134             RUNNING                   0"
 *   
 *  Or by the secondary, post execution monitoring sacct command:
 *     sacct -p -o 'JobID,State,ExitCode' -n -j ${JOBID}
 *       
 *     Example of response returned by sacct:
 *          "<jobid>|<state>|<exit_code>|"
 *
 * NOTE: If info is no longer available using squeue then squeue responds on stderr with:
 *           "slurm_load_jobs error: Invalid job id specified"
 *       This is why stderr is redirected to /dev/null.
 */
public final class SlurmMonitor extends AbstractJobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SlurmMonitor.class);
    
    // Placeholder string.
    private static final String PLACEHOLDER = "${JOBID}";
    
    // Active query command.
    private static final String ACTIVE_CMD = 
        "squeue --noheader -O 'jobid,statecompact,exit_code' -j ${JOBID} 2>/dev/null";
    
    // Active command response parser.
    private static final Pattern _spaceDelimited =
        Pattern.compile("\\s*(\\S+)\\s+(\\S+)\\s+(\\S+)\\s*");
    
    // Inactive query command.
    private static final String INACTIVE_CMD =
        "sacct -p -o 'JobID,State,ExitCode' -n -j ${JOBID}";
    
    // Inactive command response splitter.
    // Need to quote the pipe metacharacter; alternate form is "\\Q|\\E".
    private static final Pattern _pipeSplitter = Pattern.compile(Pattern.quote("|"));
    
    // Empty parser response.
    private static final ParsedStatusResponse EMPTY_PARSED_RESP = 
        new ParsedStatusResponse("", "", "");

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The response from the current query command or null.
    private ParsedStatusResponse _parsedStatusResponse;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected SlurmMonitor(JobExecutionContext jobCtx, MonitorPolicy policy) 
    {
        super(jobCtx, policy);
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getExitCode:                                                           */
    /* ---------------------------------------------------------------------- */
    @Override
    public String getExitCode() {
        if (_parsedStatusResponse == null) return null;
        return _parsedStatusResponse.getRemoteJobExitCode();
    }

    /* ---------------------------------------------------------------------- */
    /* queryRemoteJob:                                                        */
    /* ---------------------------------------------------------------------- */
    @Override
    protected JobRemoteStatus queryRemoteJob(boolean active) throws TapisException 
    {
        // Sanity check--we can't do much without the remote job id.
        if (StringUtils.isBlank(_job.getRemoteJobId())) {
            String msg = MsgUtils.getMsg("JOBS_MISSING_REMOTE_JOB_ID", _job.getUuid());
            throw new JobException(msg);
        }
        
        // Reset the response.
        _parsedStatusResponse = null;
        
        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        
        // Get the command text for this job's container.
        String cmd;
        if (active) cmd = ACTIVE_CMD;
          else cmd = INACTIVE_CMD;
        
        // Substitute the actual remote id.
        cmd = cmd.replace(PLACEHOLDER, _job.getRemoteJobId());
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_MONITOR_COMMAND", _job.getUuid(), 
                                       _jobCtx.getExecutionSystem().getHost(), 
                                       _jobCtx.getExecutionSystem().getPort(), cmd));
        
        // Execute the query with retry capability.
        String result = null;
        int rc;
        try {
        	// Unpack results.
        	var resp = runJobMonitorCmd(runCmd, cmd);
        	rc = resp.rc;
        	result = resp.result;
        }
        catch (Exception e) {
            // Exception already logged.
            return JobRemoteStatus.NULL;
        }
        
        // Trim the response. If empty log a warning and return.
        var trimmedResponse = result.trim();
        if (StringUtils.isBlank(trimmedResponse))
        {
          _log.warn(MsgUtils.getMsg("JOBS_MONITOR_NO_RESPONSE"));
          return JobRemoteStatus.EMPTY;
        }

        // Parse the response into strings representing remote jobId, jobState and jobExitCode
        _parsedStatusResponse = parseResponse(trimmedResponse, active);
        
        // If the state info is missing, the job isn't running (or so we think).
        if (StringUtils.isEmpty(_parsedStatusResponse.getRemoteJobState()))
        {
          String msg = MsgUtils.getMsg("JOBS_MONITOR_NO_STATUS", getClass().getSimpleName(),
                                       _job.getUuid(), _job.getRemoteJobId());
          _log.warn(msg);
          return JobRemoteStatus.EMPTY;
        }
        
        // Interpret the state reported by slurm. It should not have embedded spaces,
        // but we replicate this constraint from Agave out of paranoia.
        String firstStatusWord =
            StringUtils.substringBefore(_parsedStatusResponse.getRemoteJobState(), " ").toUpperCase();
        
        // Convert the reported slurm state into a typed status.
        SlurmStatusType statusType;
        try {statusType = SlurmStatusType.valueOf(firstStatusWord);}
        catch (Exception e)
        {
          // Conversion failed. Log a warning and include the response for debugging
          String msg = MsgUtils.getMsg("JOBS_MONITOR_RESP_PARSE_ERR", getClass().getSimpleName(), _job.getUuid(),
                                       _job.getRemoteJobId(), firstStatusWord, trimmedResponse);
          _log.warn(msg);
          return JobRemoteStatus.EMPTY;
        }

        // Log the result
        String msg = MsgUtils.getMsg("JOBS_MONITOR_RESULT", _job.getUuid(), _job.getRemoteJobId(),
                                     statusType.getCode(), statusType.name());
        _log.debug(msg);

        // Are we still waiting in the HPC queue?
        if (statusType.isQueued()) return JobRemoteStatus.QUEUED;
        
        // Return right away if the job completed.
        if (statusType.isCompleted()) return JobRemoteStatus.DONE;
            
        // Return right away if the job is active.
        if (statusType.isActive()) return JobRemoteStatus.ACTIVE;
        
        // Count slurm-paused as running.
        if (statusType.isPaused()) return JobRemoteStatus.ACTIVE;
        
        // If the job is in an unrecoverable state, throw the exception so the job is cleaned up.
        // The job condition is also set if the status is unrecoverable.
        if (statusType.isUnrecoverable()) {
            msg = MsgUtils.getMsg("JOBS_MONITOR_UNRECOVERABLE_RESPONSE",
                                  getClass().getSimpleName(), _parsedStatusResponse.getRemoteJobId(),
                                  statusType.name(), _parsedStatusResponse.getRemoteJobExitCode(), _job.getUuid());
            _log.warn(msg);
            
            // Update the finalMessage field in the jobCtx to reflect this status.
            _job.setCondition(statusType.getJobCondition()); // reflect slurm error code
            updateFinalMessage(_parsedStatusResponse);
            return JobRemoteStatus.FAILED;
        }
        
        // Failures.  The job condition is also set if the status is failed.
        if (statusType.isFailed()) {
            msg = MsgUtils.getMsg("JOBS_MONITOR_FAILURE_RESPONSE",
                                  getClass().getSimpleName(), _parsedStatusResponse.getRemoteJobId(),
                                  statusType.name(), _parsedStatusResponse.getRemoteJobExitCode(), _job.getUuid());
            _log.warn(msg);

            // Update the finalMessage field in the jobCtx to reflect this status. 
            _job.setCondition(statusType.getJobCondition()); // reflect slurm error code
            updateFinalMessage(_parsedStatusResponse);
            return JobRemoteStatus.FAILED;
        }

        // We shouldn't get here since all slurm states are accounted for 
        // in the above conditionals, but if we do get here we note it.
        msg = MsgUtils.getMsg("JOBS_MONITOR_UNKNOWN_RESPONSE", getClass().getSimpleName(),
                              _parsedStatusResponse.getRemoteJobId(),
                              _parsedStatusResponse.getRemoteJobState().toUpperCase(), _job.getUuid());
        _log.warn(msg);
        return JobRemoteStatus.DONE;
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* parseResponse:                                                         */
    /* ---------------------------------------------------------------------- */
    /** The response should come back as space or '|' delimited string like:
     * 
     *          "{@code <job_id> <state> <exit_code>}".
     *
     * Return parsed response object which will be the empty object if parsing was unsuccessful.
     * 
     * @param trimmedResponse the raw, non-null response from the scheduler. Should be in format:
     *            "{@code <job_id>   <state>   <exit_code>}" or
     *            "{@code <job_id>|<state>|<exit_code>|}"
     * @return a parsed response object or null if the response is null or blank
     */
    private ParsedStatusResponse parseResponse(String trimmedResponse, boolean active)
    {
        // Active responses are space delimited, inactive ones are '|' delimited.
        ParsedStatusResponse resp;
        if (active)
        {
           // ----------------- Active Job -------------------
           // Parse the active command's response. 
           String lastLineInResponse = JobUtils.getLastLine(trimmedResponse);
           var matcher = _spaceDelimited.matcher(lastLineInResponse);
           var matches = matcher.matches();
           if (!matches) {
               if (_log.isDebugEnabled()) {
                   String msg = MsgUtils.getMsg("JOBS_MONITOR_INVALID_RESPONSE", lastLineInResponse);
                   _log.debug(msg);
               }
               return EMPTY_PARSED_RESP;
           }
           var groupCount = matcher.groupCount();
           if (groupCount != 3) {
               if (_log.isDebugEnabled()) {
                   String msg = MsgUtils.getMsg("JOBS_MONITOR_INVALID_RESPONSE", lastLineInResponse);
                   _log.debug(msg);
               }
               return EMPTY_PARSED_RESP;
           }
          
          // Create response.
          resp = new ParsedStatusResponse(matcher.group(1), matcher.group(2), matcher.group(3));
        } 
        else
        {
          // ----------------- Inactive Job -----------------
          // Split the inactive command's response on pipe characters.
          // The results could look like this (note the embedded newline):
          //
          //    65|FAILED|127:0|\n65.batch|FAILED|127:0|
          //
          String lastLineInResponse = removeBannerFromInactive(trimmedResponse);
          var parts = _pipeSplitter.split(lastLineInResponse);
          if (parts.length < 3) {
              if (_log.isDebugEnabled()) {
                  String msg = MsgUtils.getMsg("JOBS_MONITOR_INVALID_RESPONSE", lastLineInResponse);
                  _log.debug(msg);
              }
              return EMPTY_PARSED_RESP;
          }
        
          // Create response removing any whitespace.
          resp = new ParsedStatusResponse(parts[0].trim(), parts[1].trim(), parts[2].trim());
        }
      
        return resp;
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateFinalMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Helper method that updates the finalMessage field with useful messaging 
     * in the jobCtx for certain failure scenarios.  The lastMessage field in 
     * the db will be updated at the end of the job to reflect the finalMessage,
     * if finalMessage is not null. 
     * 
     * @param parsedResponse monitoring response object for failed jobs
     */
    private void updateFinalMessage(ParsedStatusResponse parsedResponse) {
        String rc = StringUtils.isBlank(parsedResponse.getRemoteJobExitCode()) ?
                                        "unknown" : parsedResponse.getRemoteJobExitCode();
        String finalMessage = MsgUtils.getMsg("JOBS_USER_APP_FAILURE",
                                              parsedResponse.getRemoteJobId(),
                                              parsedResponse.getRemoteJobState(),
                                              rc); 
        _job.getJobCtx().setFinalMessage(finalMessage);
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeBannerFromInactive:                                              */
    /* ---------------------------------------------------------------------- */
    /** Remove any gorp that might appear before the actual command response.
     * We can't use JobUtils.getLastLine(response) here because the response 
     * might container more than one line. Instead, we look for the first place
     * the job id appears with a pipe character immediately following it. 
     * 
     * @param response the raw response from an inactive monitor query
     * @return the possibly decluttered response
     */
    private String removeBannerFromInactive(String response)
    {
        String start = _job.getRemoteJobId() + "|";
        int index = response.indexOf(start);
        if (index < 1) return response;
          else return response.substring(index); 
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    private static final class ParsedStatusResponse 
    {
        // Each field can be null, the empty string
        // or an actual text value.
        private final String remoteJobId;
        private final String remoteJobState;
        private final String remoteJobExitCode;
        
        // Constructor.
        private ParsedStatusResponse(String j, String s, String e)
        {remoteJobId = j; remoteJobState = s; remoteJobExitCode = e;}

        // Accessors.
        private String getRemoteJobId() {return remoteJobId;}
        private String getRemoteJobState() {return remoteJobState;}
        private String getRemoteJobExitCode() {return remoteJobExitCode;}
    }
}
