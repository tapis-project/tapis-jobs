package edu.utexas.tacc.tapis.jobs.stager.runtimes;

public interface DockerMount 
{
    // Legal docker mount types.
    enum MountType { bind, volume, tmpfs }
    
    MountType getType();
    String getSource();
    void setSource(String source);
    String getTarget();
    void setTarget(String target);
}
