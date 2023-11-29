package edu.utexas.tacc.tapis.jobs.stagers.zip;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO revisit. Initially based on DockerSlurmStager because it is the simplest.
//               Probably won't need to be as complex as singularity but might need some updating.
//      currently just calls superclass constructor. May need updating.
public final class ZipSlurmStager
 extends ZipStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipSlurmStager.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipSlurmStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Create and populate the command.
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
}