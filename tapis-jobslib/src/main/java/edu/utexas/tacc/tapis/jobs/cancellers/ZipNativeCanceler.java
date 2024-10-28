package edu.utexas.tacc.tapis.jobs.cancellers;

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Provide support for cancelling a ZIP job of type FORK.
 */
public class ZipNativeCanceler
 extends AbstractJobCanceler
{

	/* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipNativeCanceler.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipNativeCanceler(JobExecutionContext jobCtx)
    {
        // Set _jobCtx, _job
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* cancel:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
	public void cancel() throws TapisException {
    	var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        JobUtils.killJob(runCmd, _job.getUuid(), _job.getRemoteJobId(), _jobCtx );
	}
}
