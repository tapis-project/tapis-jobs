package edu.utexas.tacc.tapis.jobs.cancellers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;

public class SingularityRunCanceler extends AbstractJobCanceler{
	/* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunCanceler.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunCanceler(JobExecutionContext jobCtx)     
    {
        
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* launch:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
	public void cancel() throws JobException, TapisException {
    	 // Best effort, no noise.
        try {
            // Get the command object.
            var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
            JobUtils.killJob(runCmd, _job.getUuid(), _job.getRemoteJobId(), _jobCtx );
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SINGULARITY_CLEAN_UP_ERROR", _job.getUuid(),
                                         _job.getRemoteJobId());
            _log.error(msg, e);
        }
    	
	}
}
