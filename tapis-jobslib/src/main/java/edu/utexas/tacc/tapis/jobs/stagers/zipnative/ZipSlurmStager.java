package edu.utexas.tacc.tapis.jobs.stagers.zipnative;

import edu.utexas.tacc.tapis.jobs.stagers.dockernative.DockerNativeStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ZipSlurmStager
 extends DockerNativeStager
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
        // Create and populate the docker command.
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
}