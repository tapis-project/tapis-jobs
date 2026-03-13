package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.model.JobAnnotation;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespJobAnnotationUpdate extends RespAbstract
{
    public JobAnnotation result;
    
    public RespJobAnnotationUpdate(JobAnnotation jobAnnotation)
    {
        this.result = jobAnnotation;
    }
}
