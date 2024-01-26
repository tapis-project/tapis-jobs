package edu.utexas.tacc.tapis.jobs.worker.execjob;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/** During job execution the Jobs service uses three main directories for input
 * and output:
 * 
 *  - the execution asset directory
 *  - the input file directory
 *  - the output file directory
 *  
 *  These directories are often local file systems on the execution system, but 
 *  they can also be remote directories mounted on the execution system.  In
 *  addition, these remote directories can be designated as residing on Data
 *  Transfer Nodes (DTNs), which are specially defined Tapis systems that are
 *  used because of their high IO capacity.  When an execution system designates
 *  that a DTN should be used for IO, Jobs will direct the files service to 
 *  read and/or write data directly to the DTN host rather than to the execution
 *  host to improve IO performance.
 *  
 *  A DTN is assigned a mountpoint on an execution system.  If any of the above
 *  listed three directory paths start with the mountpoint directory, then IO
 *  will target the DTN directly rather than the execution system.  To do this,
 *  Jobs substitutes the DTN system as the target system and replaces the
 *  mountpoint prefix in the directory path with the source path on the DTN
 *  used for that mount.  When data are read from or written to the DTN directory, 
 *  they will be accessible on the execution system in directories rooted at
 *  the mountpoint.
 *  
 *  The role of this class is to figure out in which cases the DTN should be
 *  targeted and in which class the execution should be targeted.  The mentioned
 *  adjustments to systems and directories are performed once upon initialization
 *  and used throughout job execution for IO.
 * 
 * @author rcardone
 *
 */
public final class JobIOTargets 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobIOTargets.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The initialized job context.
    private final Job                 _job;
    private final TapisSystem         _execSystem;
    private final TapisSystem         _dtnSystem;

    // Initialized on construction.
    private final JobIOTarget         _execTarget = new JobIOTarget();
    private final JobIOTarget         _inputTarget = new JobIOTarget();
    private final JobIOTarget         _outputTarget = new JobIOTarget();
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobIOTargets(Job job, TapisSystem execSystem, TapisSystem dtnSystem) 
     throws TapisException
    {
        _job = job;
        _execSystem = execSystem;
        _dtnSystem = dtnSystem;
        
        initSystemsAndDirs();
        checkDtnEnabled();
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    // Accessors.
    public JobIOTarget getExecTarget() {return _execTarget;}
    public JobIOTarget getInputTarget() {return _inputTarget;}
    public JobIOTarget getOutputTarget() {return _outputTarget;}

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initSystemsAndDirs:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Assign the systems and directories for the three job targets.  The
     * values depend on whether a dtn is being used.  When a dtn directory is 
     * being used as a target then the system accessed will be the dtn system 
     * rather than the execution system.
     */
    private void initSystemsAndDirs()
    {
        // We always write directly to the job exec directory.
        _execTarget.systemId       = _job.getExecSystemId();
        _execTarget.host           = _execSystem.getHost();
        _execTarget.dir            = _job.getExecSystemExecDir();
        
        // Job input directory io target assignment.
        if (_dtnSystem != null && !StringUtils.isBlank(_job.getDtnSystemInputDir())) {
            _inputTarget.systemId  = _job.getDtnSystemId();
            _inputTarget.host      = _dtnSystem.getHost();
            _inputTarget.dir       = _job.getDtnSystemInputDir();
        } else {
            _inputTarget.systemId  = _job.getExecSystemId();
            _inputTarget.host      = _execSystem.getHost();
            _inputTarget.dir       = _job.getExecSystemInputDir();
        }
        
        // Job output directory io target assignment.
        if (_dtnSystem != null && !StringUtils.isBlank(_job.getDtnSystemOutputDir())) {
            _outputTarget.systemId = _job.getDtnSystemId();
            _outputTarget.host     = _dtnSystem.getHost();
            _outputTarget.dir      = _job.getDtnSystemOutputDir();
        } else {
            _outputTarget.systemId = _job.getExecSystemId();
            _outputTarget.host     = _execSystem.getHost();
            _outputTarget.dir      = _job.getExecSystemOutputDir();
        }
    }

    /* ---------------------------------------------------------------------- */
    /* checkDtnEnabled:                                                       */
    /* ---------------------------------------------------------------------- */
    private void checkDtnEnabled() throws TapisException
    {
        // DTN systems are not checked for availability when loaded because
        // they are used only if the job directories reference their mountpoints.
        if (_dtnSystem == null) return;
        if (_inputTarget.systemId.equals(_dtnSystem.getId())  ||
            _outputTarget.systemId.equals(_dtnSystem.getId()) ||
            _execTarget.systemId.equals(_dtnSystem.getId())) 
        {
            JobExecutionUtils.checkSystemEnabled(_dtnSystem, _job);
        }
    }
    
    /* ********************************************************************** */
    /*                           JobIOTarget Class                            */
    /* ********************************************************************** */
    /** Simple data record to contain targeting information. */
    public static final class JobIOTarget
    {
        public String systemId;
        public String host;
        public String dir;
    }
}
