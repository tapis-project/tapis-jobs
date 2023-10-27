package edu.utexas.tacc.tapis.jobs.stagers.zipnative;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipNativeStager
 extends AbstractJobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ZipNativeStager.class);

    // Process id file suffix.
    private static final String PID_SUFFIX = ".pid";

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Zip run command object.
    private final ZipRunCmd _zipRunCmd;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public ZipNativeStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Create and populate the command.
        super(jobCtx);
        _zipRunCmd = configureRunCmd();
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    @Override
    public void stageJob() throws TapisException
    {
        // Get the job file manager used to make directories, upload files, transfer files, etc.
        var fm = _jobCtx.getJobFileManager();

        // TODO Determine if the containerImage is an https or tapis url indicating we need to use the Files service
        //  to transfer the archive file from a remote system.
        // TODO For now, assume the containerImage is an absolute path on the exec system pointing to the
        //  application archive file .

        // Extract the application archive file into execSystemExecDir.
        String appArchivePath = _zipRunCmd.getImage();
        fm.extractAppArchive(appArchivePath, _zipRunCmd.getAppArguments());

        // Create the environment variable definition file.
        String envVarFile = generateEnvVarFile();

        // Install the exported env variable file.
        fm.installExecFile(envVarFile, JobExecutionUtils.JOB_ENV_FILE, JobFileManager.RWRW);
    }

    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    // TODO is this method needed for ZIP?
    /** This method generates the wrapper script content.
     *
     * @return the wrapper script content
     */
    @Override
    protected String generateWrapperScript()
     throws TapisException
    {
        // Construct the command.
        String zipCmd = _zipRunCmd.generateExecCmd(_job);

        // Build the command file content.
        initBashScript();

        // Add the command to the command file.
        _cmd.append(zipCmd);

        return _cmd.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for a environment variable definition file.
     *  
     * @return the content for a environment variable definition file 
     */
    @Override
    protected String generateEnvVarFile()
    {
        return _zipRunCmd.generateEnvVarFileContent();
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureRunCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    private ZipRunCmd configureRunCmd()
     throws TapisException
    {
        // Create and populate the command.
        var zipRunCmd = new ZipRunCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Containers are named after the job uuid.
        zipRunCmd.setName(_job.getUuid());
        
        // Set the user id under which the container runs.
        zipRunCmd.setUser("$(id -u):$(id -g)");
        
        // Write the container id to a host file.
        setPidFile(zipRunCmd);
        
        // Write all the environment variables to file.
        setEnvFile(zipRunCmd);
        
        // Set the image.
        zipRunCmd.setImage(_jobCtx.getApp().getContainerImage());
        
        // ----------------- User and Tapis Definitions -----------------
        // Set all environment variables.
        setEnvVariables(zipRunCmd);
        
        // Set the zip options.
        setZipOptions(zipRunCmd);

        // Set the application arguments.
        setAppArguments(zipRunCmd);
                
        return zipRunCmd;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setCidFile:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Write the container id to a fhost file.
     * 
     * @param zipRunCmd the run command to be updated
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    private void setPidFile(ZipRunCmd zipRunCmd)
     throws TapisException
    {
        // Put the pid file in the execution directory.
        var fm = _jobCtx.getJobFileManager();
        String path = fm.makeAbsExecSysExecPath(_job.getUuid() + PID_SUFFIX);
        zipRunCmd.setCidFile(path);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setEnvFile:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Write the environment variables to a fhost file.
     * 
     * @param zipRunCmd the run command to be updated
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    private void setEnvFile(ZipRunCmd zipRunCmd)
     throws TapisException
    {
        // Put the cid file in the execution directory.
        var fm = _jobCtx.getJobFileManager();
        String path = fm.makeAbsExecSysExecPath(JobExecutionUtils.JOB_ENV_FILE);
        zipRunCmd.setEnvFile(path);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setEnvVariables:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Both the standard tapis and user-supplied environment variables are
     * assigned here.  The user is prevented at job submission time from 
     * setting any environment variable that starts with the reserved "_tapis" 
     * prefix, so collisions are not possible. 
     * 
     * @param zipRunCmd the run command to be updated
     */
    private void setEnvVariables(ZipRunCmd zipRunCmd)
    {
        // Get the list of environment variables.
        var parmSet = _job.getParameterSetModel();
        var envList = parmSet.getEnvVariables();
        if (envList == null || envList.isEmpty()) return;
        
        // Process each environment variable.
        var zipEnv = zipRunCmd.getEnv();
        for (var kv : envList) zipEnv.add(Pair.of(kv.getKey(), kv.getValue()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* setAppArguments:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Assemble the application arguments into a single string and then assign
     * them to the zipRunCmd.  If there are any arguments, the generated
     * string always begins with a space character.
     * 
     * @param zipRunCmd the run command to be updated
     */
     private void setAppArguments(ZipRunCmd zipRunCmd)
    {
         // Assemble the application's argument string.
         String args = concatAppArguments();
         if (args != null) zipRunCmd.setAppArguments(args);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setZipOptions:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Set the zip options that we allow the user to modify.
     * 
     * @param zipRunCmd the run command to be updated
     */
    private void setZipOptions(ZipRunCmd zipRunCmd)
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
            assignRunCmd(zipRunCmd, option, value);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignRunCmd:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified zip parameter.
     * 
     * @param zipRunCmd the run command
     * @param option the zip argument
     * @param value the argument's non-null value
     */
    private void assignRunCmd(ZipRunCmd zipRunCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            case "--add-host":
                zipRunCmd.setAddHost(value); // Should be name:ipaddr format
                break;
            case "--cpus":
                zipRunCmd.setCpus(value); // Value will be doublequoted
                break;
            case "--cpuset-cpus":
                zipRunCmd.setCpusetCPUs(value);
                break;
            case "--cpuset-mems":
                zipRunCmd.setCpusetMEMs(value);
                break;
            case "--gpus":
                zipRunCmd.setGpus(value);
                break;
            case "--group-add":
                isAssigned("docker", option, value);
                zipRunCmd.getGroups().add(value);
                break;
            case "--hostname":
            case "-h":
                zipRunCmd.setHostName(value);
                break;
            case "--ip":
                zipRunCmd.setIp(value);
                break;
            case "--ip6":
                zipRunCmd.setIp6(value);
                break;
            case "--label":
            case "-l":
                addLabel(zipRunCmd, option, value);
                break;
            case "--log-driver":
                zipRunCmd.setLogDriver(value);
                break;
            case "--log-opt":
                zipRunCmd.setLogOpts(value);
                break;
            case "--memory":
            case "-m":
                zipRunCmd.setMemory(value);
                break;
            case "--mount":
                isAssigned("docker", option, value);
                zipRunCmd.getMount().add(value);
                break;
            case "--network":
            case "--net":
                zipRunCmd.setNetwork(value);
                break;
            case "--network-alias":
            case "--net-alias":
                zipRunCmd.setNetworkAlias(value);
                break;
            case "--publish":
            case "-p":
                isAssigned("docker", option, value);
                zipRunCmd.getPortMappings().add(value);
                break;
            case "--rm":
                // Allow user to determine whether container is saved or not.
            	zipRunCmd.setRm(true);
                break;
            case "--tmpfs":
                isAssigned("docker", option, value);
                zipRunCmd.getTmpfs().add(value);
                break;
            case "--volume":
            case "-v":
                isAssigned("docker", option, value);
                zipRunCmd.getVolumeMount().add(value);
                break;
            case "--workdir":
            case "-w":
                zipRunCmd.setWorkdir(value);
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
    private void addLabel(ZipRunCmd zipRunCmd, String option, String value)
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
        zipRunCmd.getLabels().add(Pair.of(key, text));
    }
}