package edu.utexas.tacc.tapis.jobs.stagers.docker;

import java.util.regex.Pattern;

import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.stagers.docker.DockerRunCmd.BindMount;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public class DockerStager
 extends AbstractJobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerStager.class);
    
    // Container id file suffix.
    private static final String CID_SUFFIX = ".cid";
    
    // Split port values that can have the maximal form: ipaddr:port:port/protocol,
    // such as 127.0.0.1:80:8080/tcp.  NOTE: We currently only support ipv4 port 
    // mappings.
    private static final Pattern _portPattern = Pattern.compile("[:/]");

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Docker run command object.
    private final DockerRunCmd _dockerRunCmd;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DockerStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Set _jobCtx, _job, _cmdBuilder,  _isBatch, _jobExecCmd
        super(jobCtx, null /* schedulerType */);
        // The docker specific exec command
        _dockerRunCmd = (DockerRunCmd) _jobExecCmd;
    }

    /* ********************************************************************** */
    /*                          Public Methods                                */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    /** This method generates the wrapper script content.
     *  Note that only FORK supported. No BATCH as of yet.
     *
     * @return the wrapper script content
     */
    @Override
    public String generateWrapperScriptContent()
     throws TapisException
    {
        // Construct the docker command.
        String dockerCmd = _dockerRunCmd.generateExecCmd(_job);
        
        // Start building the command file content.
        initBashScript();
        
        // Add the docker command.
        _cmdBuilder.append(dockerCmd);
                
        return _cmdBuilder.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for a environment variable definition file.
     *  
     * @return the content for a environment variable definition file 
     */
    @Override
    public String generateEnvVarFileContent()
    {
        return _dockerRunCmd.generateEnvVarFileContent();
    }

    /* ---------------------------------------------------------------------- */
    /* createJobExecCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Create the JobExecCmd.
     *
     */
    @Override
    public JobExecCmd createJobExecCmd() throws TapisException
    {
        return configureRunCmd();
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureRunCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    private DockerRunCmd configureRunCmd()
     throws TapisException
    {
        // Create and populate the docker command.
        var dockerRunCmd = new DockerRunCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Containers are named after the job uuid.
        dockerRunCmd.setName(_job.getUuid());
        
        // Set the user id under which the container runs.
        dockerRunCmd.setUser("$(id -u):$(id -g)");
        
        // Write the container id to a host file.
        setCidFile(dockerRunCmd);
        
        // Write all the environment variables to file.
        setEnvFile(dockerRunCmd);
        
        // Set the standard bind mounts.
        setStandardBindMounts(dockerRunCmd);
        
        // Set any tapislocal mounts.
        setTapisLocalBindMounts(dockerRunCmd);
        
        // Set the image.
        dockerRunCmd.setImage(_jobCtx.getApp().getContainerImage());
        
        // ----------------- User and Tapis Definitions -----------------
        // Set all environment variables.
        dockerRunCmd.setEnv(getEnvVariables());

        // Set the docker options.
        setDockerOptions(dockerRunCmd);
        
        // Set the application arguments.
        dockerRunCmd.setAppArguments(concatAppArguments());
                
        return dockerRunCmd;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setCidFile:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Write the container id to a fhost file.
     * 
     * @param dockerRunCmd the run command to be updated
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    private void setCidFile(DockerRunCmd dockerRunCmd) 
     throws TapisException
    {
        // Put the cid file in the execution directory.
        var fm = _jobCtx.getJobFileManager();
        String path = fm.makeAbsExecSysExecPath(_job.getUuid() + CID_SUFFIX);
        dockerRunCmd.setCidFile(path);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setEnvFile:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Write the environment variables to a host file.
     * 
     * @param dockerRunCmd the run command to be updated
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    private void setEnvFile(DockerRunCmd dockerRunCmd) 
     throws TapisException
    {
        // Put the cid file in the execution directory.
        var fm = _jobCtx.getJobFileManager();
        String path = fm.makeAbsExecSysExecPath(JobExecutionUtils.JOB_ENV_FILE);
        dockerRunCmd.setEnvFile(path);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setStandardBindMounts:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Tapis mounts the execution system's input, output and exec directories
     * using the same, standard Tapis mountpoints in each container.
     * 
     * @param dockerRunCmd the run command to be updated
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    private void setStandardBindMounts(DockerRunCmd dockerRunCmd) 
     throws TapisException
    {
        // Let the file manager make paths.
        var fm = _jobCtx.getJobFileManager();
        
        // Set standard bind mounts.
        var mount = new BindMount();
        mount.setSource(fm.makeAbsExecSysInputPath());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_INPUT_MOUNTPOINT);
        mount.setReadOnly(true);
        dockerRunCmd.getMount().add(mount.toString());
        
        mount = new BindMount();
        mount.setSource(fm.makeAbsExecSysOutputPath());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_OUTPUT_MOUNTPOINT);
        dockerRunCmd.getMount().add(mount.toString());
        
        mount = new BindMount();
        mount.setSource(fm.makeAbsExecSysExecPath());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_EXEC_MOUNTPOINT);
        dockerRunCmd.getMount().add(mount.toString());
    }
    
    /* ---------------------------------------------------------------------- */
    /* setTapisLocalBindMounts:                                               */
    /* ---------------------------------------------------------------------- */
    /** Tapis mounts the user-specified tapislocal inputs when automount is on.
     * 
     * @param dockerRunCmd the run command to be updated
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    private void setTapisLocalBindMounts(DockerRunCmd dockerRunCmd) 
     throws TapisException
    {
        // Let the file manager make paths.
        var fm = _jobCtx.getJobFileManager();
        
        for (var reqInput : _job.getFileInputsSpec()) {
            // We only process tapislocal input with automount set.
            if (!reqInput.isTapisLocal() || !reqInput.getAutoMountLocal())
                continue;
            
            // Set tapislocal bind mount from a file or directory under the execution
            // system's rootDir to the absolute targetPath in the container.
            var mount = new BindMount();
            mount.setSource(fm.makeAbsExecSysTapisLocalPath(
                _jobCtx.getExecutionSystem().getRootDir(), reqInput.getSourceUrl()));
            String targetPath = reqInput.getTargetPath();
            if (!targetPath.startsWith("/")) targetPath = "/" + targetPath;
            mount.setTarget(targetPath);
            mount.setReadOnly(true);
            dockerRunCmd.getMount().add(mount.toString());
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* setDockerOptions:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Set the docker options that we allow the user to modify.
     * 
     * @param dockerRunCmd the run command to be updated
     */
    private void setDockerOptions(DockerRunCmd dockerRunCmd)
     throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getContainerArgs();
        if (opts == null || opts.isEmpty()) return;
        
        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "docker", opt.getArg());
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
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "docker", opt.getArg());
                throw new JobException(msg);
            }
            
            // Save the parsed value.
            assignRunCmd(dockerRunCmd, option, value);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignRunCmd:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified docker parameter.
     * 
     * @param dockerRunCmd the run command
     * @param option the docker argument
     * @param value the argument's non-null value
     */
    private void assignRunCmd(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            case "--add-host":
                dockerRunCmd.setAddHost(value); // Should be name:ipaddr format
                break;
            case "--cpus":
                dockerRunCmd.setCpus(value); // Value will be doublequoted
                break;
            case "--cpuset-cpus":
                dockerRunCmd.setCpusetCPUs(value);
                break;
            case "--cpuset-mems":
                dockerRunCmd.setCpusetMEMs(value);
                break;
            case "--entrypoint":
                dockerRunCmd.setEntrypoint(value);
                break;
            case "--gpus":
                dockerRunCmd.setGpus(value);
                break;
            case "--group-add":
                isAssigned("docker", option, value);
                dockerRunCmd.getGroups().add(value);
                break;
            case "--hostname":
            case "-h":
                dockerRunCmd.setHostName(value);
                break;
            case "--ip":
                dockerRunCmd.setIp(value);
                break;
            case "--ip6":
                dockerRunCmd.setIp6(value);
                break;
            case "--label":
            case "-l":
                addLabel(dockerRunCmd, option, value);
                break;
            case "--log-driver":
                dockerRunCmd.setLogDriver(value);
                break;
            case "--log-opt":
                dockerRunCmd.setLogOpts(value);
                break;
            case "--memory":
            case "-m":
                dockerRunCmd.setMemory(value);
                break;
            case "--mount":
                isAssigned("docker", option, value);
                dockerRunCmd.getMount().add(value);
                break;
            case "--network":
            case "--net":
                dockerRunCmd.setNetwork(value);
                break;
            case "--network-alias":
            case "--net-alias":
                dockerRunCmd.setNetworkAlias(value);
                break;
            case "--publish":
            case "-p":
                isAssigned("docker", option, value);
                dockerRunCmd.getPortMappings().add(value);
                break;
            case "--rm":
                // Allow user to determine whether container is saved or not.
            	dockerRunCmd.setRm(true);
                break;
            case "--tmpfs":
                isAssigned("docker", option, value);
                dockerRunCmd.getTmpfs().add(value);
                break;
            case "--volume":
            case "-v":
                isAssigned("docker", option, value);
                dockerRunCmd.getVolumeMount().add(value);
                break;
            case "--workdir":
            case "-w":
                dockerRunCmd.setWorkdir(value);
                break;
                
            default:
                // The following options are reserved for tapis-only use.
                // If the user specifies any of them as a container option,
                // the job will abort.  Note that environment variables are 
                // passed in via their own ParameterSet object.
                //
                //   --cidfile, -e, --env, --env-file, --name, --user 
                //
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_UNSUPPORTED_ARG", "docker", option);
                throw new JobException(msg);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* addLabel:                                                              */
    /* ---------------------------------------------------------------------- */
    private void addLabel(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        isAssigned("docker", option, value);
        
        // Find the first equals sign.  We expect the value to be in key=text format.
        int index = value.indexOf("=");
        if (index < 1) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_INVALID_ARG", "docker", option, value);
            throw new JobException(msg);
        }
        
        // The text can be the empty string when value is "key=". 
        String key  = value.substring(0, index);
        String text = value.substring(index+1);
        
        // This is a repeatable option where each occurrence specifies a single group.
        dockerRunCmd.getLabels().add(Pair.of(key, text));
    }
}
