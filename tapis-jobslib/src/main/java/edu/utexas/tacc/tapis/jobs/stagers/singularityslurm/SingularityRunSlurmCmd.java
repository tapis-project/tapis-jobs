package edu.utexas.tacc.tapis.jobs.stagers.singularityslurm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.schedulers.AbstractSlurmOptions;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SingularityRunSlurmCmd
 extends AbstractSlurmOptions
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                              Constants                                 */
    /* ********************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmCmd.class);
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* Constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunSlurmCmd(JobExecutionContext jobCtx) {super(jobCtx);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The ultimate command produced conforms to this template:
        //
        // sbatch [OPTIONS...] tapisjob.sh
        //
        // The generated tapisjob.sh script will contain the singularity run 
        // command with its options, the designated image and the application
        // arguments.
        //
        //   singularity run [run options...] <image> [args] 
        //
        // In a departure from the usual role this method plays, we only generate
        // the slurm OPTIONS section of the tapisjob.sh script here.  The caller 
        // constructs the complete script.
        return getBatchDirectives();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() {
        // We pass environment variables to singularity from the command line
        // so that they are embedded in the wrapper script sent to Slurm. 
        //
        // This method should not be called.
        String msg = MsgUtils.getMsg("JOBS_SCHEDULER_GENERATE_ERROR", "slurm");
        throw new TapisRuntimeException(msg);
    }
}
