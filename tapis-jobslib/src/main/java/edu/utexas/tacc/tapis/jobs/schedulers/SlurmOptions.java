package edu.utexas.tacc.tapis.jobs.schedulers;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.SchedulerProfile;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager._optionPattern;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.alwaysSingleQuote;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

/** This class represents slurm scheduler options
 * Currently some processing is specific to HPC systems at TACC
 *
 * @author rcardone
 */
public class SlurmOptions
{
    /* ********************************************************************** */
    /*                              Constants                                 */
    /* ********************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SlurmOptions.class);

    // Slurm directive.
    private static final String DIRECTIVE_PREFIX = "#SBATCH ";

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Backpointer to job's current information.
    private final JobExecutionContext _jobCtx;
    // Convenient access to job.
    private final Job _job;

    // List of slurm options to be ignored.  The options are always take the
    // long form (--nodes rather than -N).
    private List<String>              _skipList;

    // SBATCH directive mapping used to ease wrapper script generation.
    private TreeMap<String,String>    _directives = new TreeMap<>();

    // Slurm sbatch parameters.
    private String  array;                 // comma separated list
    private String  account;               // allocation account name
    private String  acctgFreq;             // account data gathering (<datatype>=<interval>)
    private String  extraNodeInfo;         // node selector
    private String  batch;                 // list of needed node features, subset of constraint list
    private String  bb;                    // burst buffer specification
    private String  bbf;                   // burst buffer file name
    private String  begin;                 // begin datetime (YYYY-MM-DD[THH:MM[:SS]])
    private String  clusterConstraint;     // federated cluster constraint
    private String  clusters;              // comma separated list of cluster names that can run job
    private String  comment;               // job comment, automatically double quoted
    private String  constraint;            // list of needed node features
    private boolean contiguous;            // require contiguous nodes
    private String  coreSpec;              // count of cores per node reserved by job for system operations
    private String  coresPerSocket;        // node restriction
    private String  cpuFreq;               // srun frequency request
    private String  cpusPerGpu;            // number of cpus per gpu
    private String  cpusPerTask;           // number of processors per task
    private String  deadline;              // don't run unless can complete before deadline
    private String  delayBoot;             // minutes to delay rebooting nodes to satisfy job feature
    private String  dependency;            // list of jobs this job depends on
    private String  distribution;          // alternate distribution methods for srun
    private String  error;                 // filename template for stdout and stderr
    private String  exclude;               // explicitly exclude certain nodes
    private String  exclusive;             // prohibit node sharing
    private String  export;                // propagate environment variables to app
    private String  exportFile;            // file of null-separated key/value pairs
    private String  getUserEnv;            // capture the user's login environment settings
    private String  gid;                   // group id under which the job is run
    private String  gpus;                  // total number of gpus
    private String  gpuBind;               // bind tasks to specific GPUs
    private String  gpuFreq;               // specify required gpu frequency
    private String  gpusPerNode;           // specify gpus required per node
    private String  gpusPerSocket;         // specify gpus required per socket
    private String  gpusPerTask;           // specify gpus required per task
    private String  gres;                  // comma delimited list of generic consumable resources
    private String  gresFlags;             // generic resource task binding options
    private String  hint;                  // scheduler hints
    private boolean hold;                  // submit job in a held state, unblock using scontrol
    private boolean ignorePbs;             // ignore all "#PBS" and "#BSUB" options in batch script
    private String  input;                 // connect job's stdin to a file
    private String  jobName;               // name the job
    private String  killOnInvalidDep;      // kill job with invalid dependency (yes|no)
    private String  licenses;              // named licenses needed by job
    private String  mailType;              // events that trigger emails
    private String  mailUser;              // target email address
    private String  mcsLabel;              // used with plugins
    private String  mem;                   // real memory required per node (default units are megabytes)
    private String  memPerCpu;             // minimum memory required per allocated CPU
    private String  memPerGpu;             // minimum memory required per allocated GPU
    private String  memBind;               // specify NUMA task/memory binding with affinity plugin
    private String  minCpus;               // minimum number of logical cpus/processors per node
    private String  network;               // network configuration
    private String  nice;                  // nice value within slurm
    private String  nodeFile;              // file containing a list of nodes
    private String  nodeList;              // request a specific list of hosts
    private String  nodes;                 // minimum number of allocated to job
    private String  noKill;                // don't kill job if a node fails (optionally set to "off")
    private boolean noRequeue;             // never restart or requeue job
    private String  ntasks;                // maximum tasks job will launch
    private String  ntasksPerCore;         // maximum tasks per core
    private String  ntasksPerGpu;          // tasks started per gpu
    private String  ntasksPerNode;         // tasks started per node
    private String  ntasksPerSocket;       // maximum tasks per socket
    private boolean overcommit;            // 1 job per node, or 1 task per cpu
    private boolean oversubscribe;         // allocation can over-subscribe resources with other running jobs
    private String  output;                // connect batch script's stdout/stderr to a file
    private String  openMode;              // open output and error files with append or truncate
    private String  partition;             // the queue name
    private String  power;                 // power plugin options
    private String  priority;              // request job priority
    private String  profile;               // use one or more profiles
    private String  propagate;             // propagate specified configurations to compute nodes
    private String  qos;                   // quality of service
    private boolean reboot;                // reboot nodes
    private boolean requeue;               // allow job to be requeued
    private String  reservation;           // allocate resources for the job from the named reservation
    private String  signal;                // signal job when nearing end time
    private String  socketsPerNode;        // minimum sockets per node
    private String  spreadJob;             // spread job over maximum number of nodes
    private String  switches;              // the maximum count of switches required for job allocation
    private String  time;                  // set total run time limit
    private String  threadSpec;            // count of specialized threads per node reserved by the job for system operations
    private String  threadsPerCore;        // select nodes with at least the specified number of threads per core
    private String  timeMin;               // minimum run time limit for the job
    private String  tmp;                   // minimum amount of temporary disk space per node (default units are megabytes)
    private boolean useMinNodes;           // when multiple ranges given, prefer the smaller counts
    private boolean verbose;               // allow sbatch informational messages
    private String  waitAllNodes;          // wait to begin execution until all nodes are ready for use
    private String  wckey;                 // specify wckey to be used with job

    // --tapis-profile switch for customized data center support.  The profile can be used
    //   to skip outputting specific slurm options.
    // For example, at TACC there is a profile defined for running on TACC HPC systems.
    //   It is defined such that we avoid passing the --mem option to slurm and we load
    //   the singularity module before calling sbatch.
    private String tapisProfile;

    // Slurm options not supported.
    //
    //  --chdir, --help, --parsable, --quiet, --test-only
    //  --uid, --usage, --version, --wait, --wrap,
    //
    // Slurm options automatically set by Jobs and not directly available to users.
    //
    //  --mem, --nodes (-N), --ntasks (-n), --partition (-p), --time (-t)

    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* Constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SlurmOptions(JobExecutionContext jobCtx) throws TapisException
    {
        _jobCtx = jobCtx;
        _job = jobCtx.getJob();
        setUserSlurmOptions();
        setTapisOptionsForSlurm();
    }

    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* setUserSlurmOptions:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Set the slurm options that we allow the user to modify.
     *
     */
    public void setUserSlurmOptions()
            throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getSchedulerOptions();
        if (opts == null || opts.isEmpty()) return;

        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
                throw new JobException(msg);
            }

            // Get the option and its value if one is provided.
            String option = null;
            String value  = ""; // default value when none provided
            int groupCount = m.groupCount();
            if (groupCount > 0) option = m.group(1);
            if (groupCount > 1) value  = m.group(2);

            // The option should always exist.
            if (StringUtils.isBlank(option)) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
                throw new JobException(msg);
            }

            // Save the parsed value.
            assignCmd(option, value);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* assignCmd:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified scheduler parameter.
     * 
     * @param option the scheduler argument
     * @param value the argument's non-null value
     */
    public void assignCmd(String option, String value)
     throws JobException
    {
        switch (option) {
            // Start/Run common options.
            case "--array":
            case "-a":
                setArray(value);
                break;
                
            case "--account":
            case "-A":
                setAccount(value);
                break;
                
            case "--acctg-freq":
                setAcctgFreq(value);
                break;
                
            case "--extra-node-info":
            case "-B":
                setExtraNodeInfo(value);
                break;
                
            case "--batch":
                setBatch(value);
                break;
                
            case "--bb":
                setBb(value);
                break;
                
            case "--bbf":
                setBbf(value);
                break;
                
            case "--begin":
            case "-b":
                setBegin(value);
                break;
                
            case "--cluster-contstraint":
                setClusterConstraint(value);
                break;
                
            case "--clusters":
            case "-M":
                setClusters(value);
                break;
                
            case "--comment":
                setComment(value);
                break;
                
            case "--constraint":
            case "-C":
                setConstraint(value);
                break;
                
            case "--contiguous":
                setContiguous(true);
                break;
                
            case "--core-spec":
            case "-S":
                setCoreSpec(value);
                break;
                
            case "--cores-per-socket":
                setCoresPerSocket(value);
                break;
                
            case "--cpu-freq":
                setCpuFreq(value);
                break;
                
            case "--cpus-per-gpu":
                setCpusPerGpu(value);
                break;
                
            case "--cpus-per-task":
                setCpusPerTask(value);
                break;
                
            case "--deadline":
                setDeadline(value);
                break;
                
            case "--delay-boot":
                setDelayBoot(value);
                break;
                
            case "--dependency":
            case "-d":
                setDependency(value);
                break;
                
            case "--distribution":
            case "-m":
                setDistribution(value);
                break;
                
            case "--error":
            case "-e":
                setError(value);
                break;
                
            case "--exclude":
            case "-X":
                setExclusive(value);
                break;

            case "--exclusive":
                setExclusive(value);
                break;

            case "--export":
                setExport(value);
                break;
                
            case "--export-file":
                setExportFile(value);
                break;
                
            case "--get-user-env":
                setGetUserEnv(value);
                break;
                
            case "--gid":
                setGid(value);
                break;
                
            case "--gpus":
            case "-G":
                setGpus(value);
                break;
                
            case "--gpu-bind":
                setGpuBind(value);
                break;
                
            case "--gpu-freq":
                setGpuFreq(value);
                break;
                
            case "--gpus-per-node":
                setGpusPerNode(value);
                break;
                
            case "--gpus-per-socket":
                setGpusPerSocket(value);
                break;
                
            case "--gpus-per-task":
                setGpusPerTask(value);
                break;
                
            case "--gres":
                setGres(value);
                break;
                
            case "--gres-flags":
                setGresFlags(value);
                break;
                
            case "--hint":
                setHint(value);
                break;
                
            case "--hold":
            case "-H":
                setHold(true);
                break;
                
            case "--ignore-pbs":
                setIgnorePbs(true);
                break;
                
            case "--input":
            case "-i":
                setInput(value);
                break;
                
            case "--job-name":
            case "-J":
                setJobName(value);
                break;
                
            case "--kill-on-invalid-dep":
                setKillOnInvalidDep(value);
                break;
                
            case "--licenses":
            case "-L":
                setLicenses(value);
                break;
                
            case "--mail-type":
                setMailType(value);
                break;
                
            case "--mail-user":
                setMailUser(value);
                break;
                
            case "--mcs-label":
                setMcsLabel(value);
                break;
                
            case "--mem-per-cpu":
                setMemPerCpu(value);
                break;
                
            case "--mem-per-gpu":
                setMemPerGpu(value);
                break;
                
            case "--mem-bind":
                setMemBind(value);
                break;
                
            case "--mincpus":
                setMinCpus(value);
                break;
                
            case "--network":
                setNetwork(value);
                break;
                
            case "--nice":
                setNice(value);
                break;
                
            case "--nodefile":
            case "-F":
                setNodeFile(value);
                break;
                
            case "--nodelist":
            case "-W":
                setNodeList(value);
                break;
                
            case "--no-kill":
            case "-k":
                setNoKill(value);
                break;
                
            case "--no-requeue":
                setNoRequeue(true);
                break;
                
            case "--ntasks-per-core":
                setNTasksPerCore(value);
                break;
                
            case "--ntasks-per-gpu":
                setNTasksPerGpu(value);
                break;
                
            case "--ntasks-per-node":
                setNTasksPerNode(value);
                break;
                
            case "--ntasks-per-socket":
                setNTasksPerSocket(value);
                break;
                
            case "--overcommit":
            case "-O":
                setOvercommit(true);
                break;
                
            case "--oversubscribe":
            case "-s":
                setOversubscribe(true);
                break;
                
            case "--output":
            case "-o":
                setOutput(value);
                break;
                
            case "--open-mode":
                setOpenMode(value);
                break;
                
            case "--power":
                setPower(value);
                break;
                
            case "--priority":
                setPriority(value);
                break;
                
            case "--profile":
                setProfile(value);
                break;
                
            case "--propagate":
                setPropagate(value);
                break;
                
            case "--qos":
            case "-q":
                setQos(value);
                break;
                
            case "--reboot":
                setReboot(true);
                break;
                
            case "--requeue":
                setRequeue(true);
                break;

            case "--reservation":
                setReservation(value);
                break;
                
            case "--signal":
                setSignal(value);
                break;
                
            case "--sockets-per-node":
                setSocketsPerNode(value);
                break;
                
            case "--spread-job":
                setSpreadJob(value);
                break;
                
            case "--switches":
                setSwitches(value);
                break;
                
            case "--thread-spec":
                setThreadSpec(value);
                break;
                
            case "--threads-per-core":
                setThreadsPerCore(value);
                break;
                
            case "--time-min":
                setTimeMin(value);
                break;
                
            case "--tmp":
                setTmp(value);
                break;
                
            case "--use-min-nodes":
                setUseMinNodes(true);
                break;
                
            case "--verbose":
            case "-v":
                // Multiple -v's increase verbosity, 
                // we only support the first level. 
                setVerbose(true);
                break;
                
            case "--wait-all-nodes":
                setWaitAllNodes(value);
                break;
                
            case "--wckey":
                setWckey(value);
                break;

            // Tapis extended arguments (Job.TAPIS_PROFILE_KEY).    
            case "--tapis-profile":
                setTapisProfile(value);
                break;
            
            // Subsumed options.
            case "--mem":
            case "--nodes":
            case "-N":
            case "--ntasks":
            case "-n":
            case "--partition":
            case "-p":
            case "--time":
            case "-t":
                String tapisArg = getTapisArg(option);
                String msg1 = MsgUtils.getMsg("JOBS_SCHEDULER_SUBSUMED_ARG", "slurm", option, tapisArg);
                throw new JobException(msg1);
                
                
            default:
                // Slurm options not supported:
                //
                //  --chdir, --help, --parsable, --quiet, --test-only
                //  --uid, --usage, --version, --wait, --wrap, 
                //
                // Slurm options automatically set by Jobs and not directly available to users.
                // These are the above subsumed options.
                //
                //  --mem, --nodes (-N), --ntasks (-n), --partition (-p), --time (-t)
                //
                String msg2 = MsgUtils.getMsg("JOBS_SCHEDULER_UNSUPPORTED_ARG", "slurm", option);
                throw new JobException(msg2);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* setTapisOptionsForSlurm:                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the standard Tapis settings for Slurm.
     *
     * @throws TapisException on error
     */
    private void  setTapisOptionsForSlurm()
            throws TapisException
    {
        // --------------------- Tapis Mandatory ---------------------
        // Request the total number of nodes from slurm.
        setNodes(Integer.toString(_job.getNodeCount()));

        // Tell slurm the total number of tasks to run.
        setNtasks(Integer.toString(_job.getTotalTasks()));

        // Tell slurm the total runtime of the application in minutes.
        setTime(Integer.toString(_job.getMaxMinutes()));

        // Tell slurm the memory per node requirement in megabytes.
        setMem(Integer.toString(_job.getMemoryMB()));

        // We've already checked in JobQueueProcessor before processing any
        // state changes that the logical and hpc queues have been assigned.
        var logicalQueue = _jobCtx.getLogicalQueue();
        setPartition(logicalQueue.getHpcQueueName());

        // --------------------- Tapis Optional ----------------------
        // Always assign a job name
        // If user has not specified one then use a default, "tapisjob.sh"
        if (StringUtils.isBlank(getJobName())) {
            setJobName(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
        }
        // Assign the standard tapis output file name if one is not
        // assigned and we are not running an array job.  We let slurm
        // use its default naming scheme for array job output files.
        // Unless the user explicitly specifies an error file, both
        // stdout and stderr will go the designated output file.
        if (StringUtils.isBlank(getOutput()) && StringUtils.isBlank(getArray())) {
        	// The log configuration should never be null after getting the parameter set model.
        	// Unset output files use the default file, though they should always be set by now.
        	var logConfig = _job.getParameterSetModel().getLogConfig();
        	
        	// Get the output and error file names.
        	var fout = logConfig.getStdoutFilename();
        	if (StringUtils.isBlank(fout)) fout = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
        	var ferr = logConfig.getStderrFilename();
        	if (StringUtils.isBlank(ferr)) ferr = JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE;
        	
        	// Set the error file only if the user explicitly set it  
        	// and it differs them the output file.  This effectively
        	// gives precedence to the slurm error file option over 
        	// LogConfig setting if both are set.  By default, Slurm 
        	// sets the error file to the output file.
        	var fm = _jobCtx.getJobFileManager();
            setOutput(fm.makeAbsExecSysOutputPath(fout));
            if (StringUtils.isBlank(getError()) && !fout.equals(ferr) ) 
            	setError(fm.makeAbsExecSysOutputPath(ferr));
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getTapisArg:                                                           */
    /* ---------------------------------------------------------------------- */
    public String getTapisArg(String option)
    {
        return switch (option) {
            case "--mem"       -> "memoryMB";
            case "--nodes"     -> "nodeCount";
            case "-N"          -> "nodeCount";
            case "--ntasks"    -> "coresPerNode";
            case "-n"          -> "coresPerNode";
            case "--partition" -> "execSystemLogicalQueue";
            case "-p"          -> "execSystemLogicalQueue";
            case "--time"      -> "maxMinutes";
            case "-t"          -> "maxMinutes";
            default            -> "unknown";
        };
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getDirectives:                                                         */
    /* ---------------------------------------------------------------------- */
    protected TreeMap<String,String> getDirectives(){return _directives;}
    
    /* ---------------------------------------------------------------------- */
    /* getBatchDirectives:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Generate the sbatch directives separated by newline characters.  Each
     * system can have its own policy that filters out certain slurm options.
     * This method performs that filtering.
     * 
     * NOTE: SBATCH directive values are not quoted since the shell script
     * 		 does not treat them as executable lines of code.
     * 
     * @return the list of #SBATCH directives
     */
    protected String getBatchDirectives()
    {
        // Create a buffer to hold the sbatch directives
        // which must appear in the script before any 
        // executable statements.
        final int capacity = 1024;
        var buf = new StringBuilder(capacity);
        
        // Add the sbatch directives in alphabetic order.
        buf.append("# Slurm directives.\n");
        for (var entry : _directives.entrySet()) {
            if (skipSlurmOption(entry.getKey())) continue;
            buf.append(DIRECTIVE_PREFIX);
            buf.append(entry.getKey());
            if (StringUtils.isNotBlank(entry.getValue())) {
                buf.append(" ");
                buf.append(entry.getValue());
            }
            buf.append("\n");
        }
        buf.append("\n");
        
        // Return the sbatch directives.
        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* skipSlurmOption:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Skip options that are not allowed by the target system.
     *
     * @param option the long form slurm option
     * @return true to skip option, false to pass option to slurm
     */
    protected boolean skipSlurmOption(String option)
    {
        // No special filtering unless using tacc profile.
        if (StringUtils.isBlank(getTapisProfile())) return false;

        // Initialize the skip list.
        initializeSkipList();

        // Check the option against the list of options to ignore.
        return _skipList.contains(option);
    }

    /* ---------------------------------------------------------------------- */
    /* initializeSkipList:                                                    */
    /* ---------------------------------------------------------------------- */
    private void initializeSkipList()
    {
        // Only initialize once.
        if (_skipList != null) return;

        // Always create the skip list.
        _skipList = new ArrayList<String>();

        // If we don't receive the profile we just continue from here.
        // Other attempts to retrieve the profile will cause the job to
        // abort.  If no exception is thrown, the profile is not null.
        SchedulerProfile profile = null;
        try {profile = _jobCtx.getSchedulerProfile(getTapisProfile());}
        catch (Exception e) {
            _log.error(e.getMessage(), e);
            return;
        }

        // Assign all skip values from the profile to the list.  The auto-generated
        // client code makes it difficult to have values that exactly match the
        // long form slurm options, so we explicitly code the mapping in the
        // client code.  This mapping is only available in the Java client.
        var hiddenOpts = profile.getHiddenOptions();
        if (hiddenOpts != null && !hiddenOpts.isEmpty())
            for (var opt : hiddenOpts) {
                String value = SystemsClient.getSchedulerHiddenOptionValue(opt);
                if (StringUtils.isNotBlank(value)) _skipList.add(value);
                else {
                    String msg = MsgUtils.getMsg("JOBS_SCHEDULER_NO_HIDDEN_OPTION_VALUE",
                            profile.getOwner(), _jobCtx.getJob().getTenant(),
                            profile.getName(), opt.name());
                    _log.error(msg);
                }
            }
    }
    
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public String getArray() {
        return array;
    }

    public void setArray(String array) {
        this.array = array;
        _directives.put("--array", array);
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
        _directives.put("--account", account);
    }

    public String getAcctgFreq() {
        return acctgFreq;
    }

    public void setAcctgFreq(String acctgFreq) {
        this.acctgFreq = acctgFreq;
        _directives.put("--acctg-freq", acctgFreq);
    }

    public String getExtraNodeInfo() {
        return extraNodeInfo;
    }

    public void setExtraNodeInfo(String extraNodeInfo) {
        this.extraNodeInfo = extraNodeInfo;
        _directives.put("--extra-node-info", extraNodeInfo);
    }

    public String getBatch() {
        return batch;
    }

    public void setBatch(String batch) {
        this.batch = batch;
        _directives.put("--batch", batch);
    }

    public String getBb() {
        return bb;
    }

    public void setBb(String bb) {
        this.bb = bb;
        _directives.put("--bb", bb);
    }

    public String getBbf() {
        return bbf;
    }

    public void setBbf(String bbf) {
        this.bbf = bbf;
        _directives.put("--bbf", bbf);
    }

    public String getBegin() {
        return begin;
    }

    public void setBegin(String begin) {
        this.begin = begin;
        _directives.put("--begin", begin);
    }

    public String getClusterConstraint() {
        return clusterConstraint;
    }

    public void setClusterConstraint(String clusterConstraint) {
        this.clusterConstraint = clusterConstraint;
        _directives.put("--cluster-constraint", clusterConstraint);
    }

    public String getClusters() {
        return clusters;
    }

    public void setClusters(String clusters) {
        this.clusters = clusters;
        _directives.put("--clusters", clusters);
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
        _directives.put("--comment", comment);
    }

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
        _directives.put("--constraint", constraint);
    }

    public boolean isContiguous() {
        return contiguous;
    }

    public void setContiguous(boolean contiguous) {
        this.contiguous = contiguous;
        _directives.put("--contiguous", "");
    }

    public String getCoresPerSocket() {
        return coresPerSocket;
    }

    public void setCoresPerSocket(String coresPerSocket) {
        this.coresPerSocket = coresPerSocket;
        _directives.put("--cores-per-socket", coresPerSocket);
    }

    public String getCpuFreq() {
        return cpuFreq;
    }

    public void setCpuFreq(String cpuFreq) {
        this.cpuFreq = cpuFreq;
        _directives.put("--cpu-freq", cpuFreq);
    }

    public String getCpusPerGpu() {
        return cpusPerGpu;
    }

    public void setCpusPerGpu(String cpusPerGpu) {
        this.cpusPerGpu = cpusPerGpu;
        _directives.put("--cpus-per-gpu", cpusPerGpu);
    }

    public String getCpusPerTask() {
        return cpusPerTask;
    }

    public void setCpusPerTask(String cpusPerTask) {
        this.cpusPerTask = cpusPerTask;
        _directives.put("--cpus-per-task", cpusPerTask);
    }

    public String getDeadline() {
        return deadline;
    }

    public void setDeadline(String deadline) {
        this.deadline = deadline;
        _directives.put("--deadline", deadline);
    }

    public String getDelayBoot() {
        return delayBoot;
    }

    public void setDelayBoot(String delayBoot) {
        this.delayBoot = delayBoot;
        _directives.put("--delay-boot", delayBoot);
    }

    public String getDependency() {
        return dependency;
    }

    public void setDependency(String dependency) {
        this.dependency = dependency;
        _directives.put("--dependency", dependency);
    }

    public String getDistribution() {
        return distribution;
    }

    public void setDistribution(String distribution) {
        this.distribution = distribution;
        _directives.put("--distribution", distribution);
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
        _directives.put("--error", alwaysSingleQuote(error));
    }

    public String getExclude() {
        return exclude;
    }

    public void setExclude(String exclude) {
        this.exclude = exclude;
        _directives.put("--exclude", exclude);
    }

    public String getExclusive() {
        return exclusive;
    }

    public void setExclusive(String exclusive) {
        this.exclusive = exclusive;
        _directives.put("--exclusive", exclusive);
    }

    public String getExport() {
        return export;
    }

    public void setExport(String export) {
        this.export = export;
        _directives.put("--export", export);
    }

    public String getExportFile() {
        return exportFile;
    }

    public void setExportFile(String exportFile) {
        this.exportFile = exportFile;
        _directives.put("--export-file", exportFile);
    }

    public String getGetUserEnv() {
        return getUserEnv;
    }

    public void setGetUserEnv(String getUserEnv) {
        this.getUserEnv = getUserEnv;
        _directives.put("--get-user-env", getUserEnv);
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
        _directives.put("--gid", gid);
    }

    public String getGpus() {
        return gpus;
    }

    public void setGpus(String gpus) {
        this.gpus = gpus;
        _directives.put("--gpus", gpus);
    }

    public String getGpuBind() {
        return gpuBind;
    }

    public void setGpuBind(String gpuBind) {
        this.gpuBind = gpuBind;
        _directives.put("--gpu-bind", gpuBind);
    }

    public String getGpuFreq() {
        return gpuFreq;
    }

    public void setGpuFreq(String gpuFreq) {
        this.gpuFreq = gpuFreq;
        _directives.put("--gpu-freq", gpuFreq);
    }

    public String getGpusPerNode() {
        return gpusPerNode;
    }

    public void setGpusPerNode(String gpusPerNode) {
        this.gpusPerNode = gpusPerNode;
        _directives.put("--gpus-per-node", gpusPerNode);
    }

    public String getGpusPerSocket() {
        return gpusPerSocket;
    }

    public void setGpusPerSocket(String gpusPerSocket) {
        this.gpusPerSocket = gpusPerSocket;
        _directives.put("--gpus-per-socket", gpusPerSocket);
    }

    public String getGpusPerTask() {
        return gpusPerTask;
    }

    public void setGpusPerTask(String gpusPerTask) {
        this.gpusPerTask = gpusPerTask;
        _directives.put("--gpus-per-task", gpusPerTask);
    }

    public String getGres() {
        return gres;
    }

    public void setGres(String gres) {
        this.gres = gres;
        _directives.put("--gres", gres);
    }

    public String getGresFlags() {
        return gresFlags;
    }

    public void setGresFlags(String gresFlags) {
        this.gresFlags = gresFlags;
        _directives.put("--gres-flags", gresFlags);
    }

    public String getHint() {
        return hint;
    }

    public void setHint(String hint) {
        this.hint = hint;
        _directives.put("--hint", hint);
    }

    public boolean isHold() {
        return hold;
    }

    public void setHold(boolean hold) {
        this.hold = hold;
        _directives.put("--hold", "");
    }

    public boolean isIgnorePbs() {
        return ignorePbs;
    }

    public void setIgnorePbs(boolean ignorePbs) {
        this.ignorePbs = ignorePbs;
        _directives.put("--ignore-pbs", "");
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
        _directives.put("--input", input);
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
        // Local (i.e., TACC) policies may not allow colons in name.
        _directives.put("--job-name", jobName.replace(':', '-'));
    }

    public String getKillOnInvalidDep() {
        return killOnInvalidDep;
    }

    public void setKillOnInvalidDep(String killOnInvalidDep) {
        this.killOnInvalidDep = killOnInvalidDep;
        _directives.put("--kill-on-invalid-dep", killOnInvalidDep);
    }

    public String getLicenses() {
        return licenses;
    }

    public void setLicenses(String licenses) {
        this.licenses = licenses;
        _directives.put("--licenses", licenses);
    }

    public String getMailType() {
        return mailType;
    }

    public void setMailType(String mailType) {
        this.mailType = mailType;
        _directives.put("--mail-type", mailType);
    }

    public String getMailUser() {
        return mailUser;
    }

    public void setMailUser(String mailUser) {
        this.mailUser = mailUser;
        _directives.put("--mail-user", mailUser);
    }

    public String getMcsLabel() {
        return mcsLabel;
    }

    public void setMcsLabel(String mcsLabel) {
        this.mcsLabel = mcsLabel;
        _directives.put("--mcs-label", mcsLabel);
    }

    public String getMem() {
        return mem;
    }

    public void setMem(String mem) {
        this.mem = mem;
        _directives.put("--mem", mem);
    }

    public String getMemPerCpu() {
        return memPerCpu;
    }

    public void setMemPerCpu(String memPerCpu) {
        this.memPerCpu = memPerCpu;
        _directives.put("--mem-per-cpu", memPerCpu);
    }

    public String getMemPerGpu() {
        return memPerGpu;
    }

    public void setMemPerGpu(String memPerGpu) {
        this.memPerGpu = memPerGpu;
        _directives.put("-mem-per-gpu", memPerGpu);
    }

    public String getMemBind() {
        return memBind;
    }

    public void setMemBind(String memBind) {
        this.memBind = memBind;
        _directives.put("--mem-bind", memBind);
    }

    public String getMinCpus() {
        return minCpus;
    }

    public void setMinCpus(String minCpus) {
        this.minCpus = minCpus;
        _directives.put("--mincpus", minCpus);
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
        _directives.put("--network", network);
    }

    public String getNice() {
        return nice;
    }

    public void setNice(String nice) {
        this.nice = nice;
        _directives.put("--nice", nice);
    }

    public String getNodeFile() {
        return nodeFile;
    }

    public void setNodeFile(String nodefile) {
        this.nodeFile = nodefile;
        _directives.put("--nodefile", nodefile);
    }

    public String getNodeList() {
        return nodeList;
    }

    public void setNodeList(String nodeList) {
        this.nodeList = nodeList;
        _directives.put("--nodelist", nodeList);
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
        _directives.put("--nodes", nodes);
    }

    public String getNoKill() {
        return noKill;
    }

    public void setNoKill(String noKill) {
        this.noKill = noKill;
        _directives.put("--no-kill", noKill);
    }

    public boolean isNoRequeue() {
        return noRequeue;
    }

    public void setNoRequeue(boolean noRequeue) {
        this.noRequeue = noRequeue;
        _directives.put("--no-requeue", "");
    }

    public String getNtasks() {
        return ntasks;
    }

    public void setNtasks(String ntasks) {
        this.ntasks = ntasks;
        _directives.put("--ntasks", ntasks);
    }

    public boolean isOvercommit() {
        return overcommit;
    }

    public void setOvercommit(boolean overcommit) {
        this.overcommit = overcommit;
        _directives.put("--overcommit", "");
    }

    public boolean isOversubscribe() {
        return oversubscribe;
    }

    public void setOversubscribe(boolean oversubscribe) {
        this.oversubscribe = oversubscribe;
        _directives.put("--oversubscribe", "");
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
        _directives.put("--output", alwaysSingleQuote(output));
    }

    public String getOpenMode() {
        return openMode;
    }

    public void setOpenMode(String openMode) {
        this.openMode = openMode;
        _directives.put("--open-mode", openMode);
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
        _directives.put("--partition", partition);
    }

    public String getPower() {
        return power;
    }

    public void setPower(String power) {
        this.power = power;
        _directives.put("--power", power);
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
        _directives.put("--priority", priority);
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
        _directives.put("--profile", profile);
    }

    public String getPropagate() {
        return propagate;
    }

    public void setPropagate(String propagate) {
        this.propagate = propagate;
        _directives.put("--propagate", propagate);
    }

    public String getQos() {
        return qos;
    }

    public void setQos(String qos) {
        this.qos = qos;
        _directives.put("--qos", qos);
    }

    public boolean isReboot() {
        return reboot;
    }

    public void setReboot(boolean reboot) {
        this.reboot = reboot;
        _directives.put("--reboot", "");
    }

    public boolean isRequeue() {
        return requeue;
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
        _directives.put("--requeue", "");
    }

    public String getReservation() {
        return reservation;
    }

    public void setReservation(String reservation) {
        this.reservation = reservation;
        _directives.put("--reservation", reservation);
    }

    public String getCoreSpec() {
        return coreSpec;
    }

    public void setCoreSpec(String coreSpec) {
        this.coreSpec = coreSpec;
        _directives.put("--core-spec", coreSpec);
    }

    public String getSignal() {
        return signal;
    }

    public void setSignal(String signal) {
        this.signal = signal;
        _directives.put("--signal", signal);
    }

    public String getSocketsPerNode() {
        return socketsPerNode;
    }

    public void setSocketsPerNode(String socketsPerNode) {
        this.socketsPerNode = socketsPerNode;
        _directives.put("--sockets-per-node", socketsPerNode);
    }

    public String getSpreadJob() {
        return spreadJob;
    }

    public void setSpreadJob(String spreadJob) {
        this.spreadJob = spreadJob;
        _directives.put("--spread-job", spreadJob);
    }

    public String getSwitches() {
        return switches;
    }

    public void setSwitches(String switches) {
        this.switches = switches;
        _directives.put("--switches", switches);
    }

    public String getNTasksPerCore() {
        return ntasksPerCore;
    }

    public void setNTasksPerCore(String ntasksPerCore) {
        this.ntasksPerCore = ntasksPerCore;
        _directives.put("--ntasks-per-core", ntasksPerCore);
    }

    public String getNTasksPerGpu() {
        return ntasksPerGpu;
    }

    public void setNTasksPerGpu(String ntasksPerGpu) {
        this.ntasksPerGpu = ntasksPerGpu;
        _directives.put("--ntasks-per-gpu", ntasksPerGpu);
    }

    public String getNTasksPerNode() {
        return ntasksPerNode;
    }

    public void setNTasksPerNode(String ntasksPerNode) {
        this.ntasksPerNode = ntasksPerNode;
        _directives.put("--ntasks-per-node", ntasksPerNode);
    }

    public String getNTasksPerSocket() {
        return ntasksPerSocket;
    }

    public void setNTasksPerSocket(String ntasksPerSocket) {
        this.ntasksPerSocket = ntasksPerSocket;
        _directives.put("--ntasks-per-socket", ntasksPerSocket);
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
        _directives.put("--time", time);
    }

    public String getThreadSpec() {
        return threadSpec;
    }

    public void setThreadSpec(String threadSpec) {
        this.threadSpec = threadSpec;
        _directives.put("--thread-spec", threadSpec);
    }

    public String getThreadsPerCore() {
        return threadsPerCore;
    }

    public void setThreadsPerCore(String threadsPerCore) {
        this.threadsPerCore = threadsPerCore;
        _directives.put("--threads-per-core", threadsPerCore);
    }

    public String getTimeMin() {
        return timeMin;
    }

    public void setTimeMin(String timeMin) {
        this.timeMin = timeMin;
        _directives.put("--time-min", timeMin);
    }

    public String getTmp() {
        return tmp;
    }

    public void setTmp(String tmp) {
        this.tmp = tmp;
        _directives.put("--tmp", tmp);
    }

    public boolean isUseMinNodes() {
        return useMinNodes;
    }

    public void setUseMinNodes(boolean useMinNodes) {
        this.useMinNodes = useMinNodes;
        _directives.put("--use-min-nodes", "");
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
        _directives.put("--verbose", "");
    }

    public String getWaitAllNodes() {
        return waitAllNodes;
    }

    public void setWaitAllNodes(String waitAllNodes) {
        this.waitAllNodes = waitAllNodes;
        _directives.put("--wait-all-nodes", waitAllNodes);
    }

    public String getWckey() {
        return wckey;
    }

    public void setWckey(String wckey) {
        this.wckey = wckey;
        _directives.put("--wckey", wckey);
    }

    public String getTapisProfile() {
        return tapisProfile;
    }

    public void setTapisProfile(String tapisProfile) {
        this.tapisProfile = tapisProfile;
    }
}
