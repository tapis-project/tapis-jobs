package edu.utexas.tacc.tapis.jobs.stagers.docker;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.alwaysSingleQuote;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalSingleQuote;

/** This class stores the command line options for the docker run command that executes
 * an application's container.  The general approach is to take the user-specified text
 * as is.  Validation and reformatting kept to a minimum.  More rigorous parsing and 
 * validation can be added if the need arises, but this approach is simple to implement
 * and maintain.
 * 
 * @author rcardone
 */
public final class DockerRunCmd 
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
	// Placeholder for the --volume command in string representations of 
	// VolumeMount instances.  This placeholder allows the docker --read-only 
	// flag to precede the --volume command during command generation.
	private static final String VOLUME_CMD = "{VOLUME_CMD}";
	
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // List fields that we always populate are initialized on construction,
    // all others are initialized on demand.
    private String                    addHost;
    private String                    cidFile;
    private String                    cpus;
    private String                    cpusetCPUs;
    private String                    cpusetMEMs;
    private String                    entrypoint;
    private List<Pair<String,String>> env = new ArrayList<Pair<String,String>>();
    private String                    envFile;
    private String                    gpus;
    private List<String>              groups;
    private String                    hostName;
    private String                    ip;
    private String                    ip6;
    private String                    image;
    private List<Pair<String,String>> labels;
    private String                    logDriver;
    private String                    logOpts;
    private String                    memory;
    private List<String>              mount = new ArrayList<String>();
    private String                    name;
    private String                    network;
    private String                    networkAlias;
    private List<String>              portMappings;
    private boolean                   rm;
    private List<String>              tmpfs;
    private String                    user;
    private List<String>              volumeMount;
    private String                    workdir;
    
    // Arguments passed to application, which always begin with a space character.
    private String                    appArguments; 
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The generated wrapper script will contain a docker run command: 
        //
        //   docker run [OPTIONS] IMAGE[:TAG|@DIGEST] [COMMAND] [ARG...]
        
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // ------ Start filling in the options that are tapis-only assigned.
        buf.append("# Issue docker run command and write container ID to file.\n");
        buf.append("# Format: docker run [options] image[:tag|@digest] [app args]\n");
        
        var p = job.getMpiOrCmdPrefixPadded(); // empty or string w/trailing space
        buf.append(p + "docker run -d --name ");
        buf.append(name);
        buf.append(" --user ");
        buf.append(user);
        buf.append(" --cidfile ");
        buf.append(alwaysSingleQuote(cidFile));
        buf.append(" --env-file ");
        buf.append(alwaysSingleQuote(envFile));
        if (rm) buf.append(" --rm");
        
        // ------ Fill in the options that the user may have set.
        if (addHost != null) {
            buf.append(" --addhost ");
            buf.append(conditionalQuote(addHost));
        }
        if (cpus != null) {
            buf.append(" --cpus ");
            buf.append(conditionalQuote(cpus));
        }
        if (cpusetCPUs != null) {
            buf.append(" --cpuset-cpus ");
            buf.append(conditionalQuote(cpusetCPUs));
        }
        if (cpusetMEMs != null) {
            buf.append(" --cpuset-mems ");
            buf.append(conditionalQuote(cpusetMEMs));
        }
        if (entrypoint != null) {
        	buf.append(" --entrypoint ");
        	buf.append(conditionalSingleQuote(entrypoint));
        }
        if (gpus != null) {
            buf.append(" --gpus ");
            buf.append(conditionalQuote(gpus));
        }
        if (groups != null) {
            for (var s : groups) {
                buf.append(" --group-add ");
                buf.append(conditionalQuote(s));
            }
        }
        if (hostName != null) {
            buf.append(" --hostname ");
            buf.append(conditionalQuote(hostName));
        }
        if (ip != null) {
            buf.append(" --ip ");
            buf.append(conditionalQuote(ip));
        }
        if (ip6 != null) {
            buf.append(" --ip6 ");
            buf.append(conditionalQuote(ip6));
        }
        if (labels != null) {
            for (var pair : labels) {
                buf.append(" --label ");
                buf.append(conditionalQuote(pair.getKey()));
                if (!StringUtils.isBlank(pair.getValue())) {
                	buf.append("=");
                	buf.append(conditionalQuote(pair.getValue()));
                }
            }
        }
        if (logDriver != null) {
            buf.append(" --log-driver ");
            buf.append(conditionalQuote(logDriver));
        }
        if (logOpts != null) {
            buf.append(" --log-opt ");
            buf.append(conditionalQuote(logOpts));
        }
        if (memory != null) {
            buf.append(" --memory ");
            buf.append(conditionalQuote(memory));
        }
        if (network != null) {
            buf.append(" --network ");
            buf.append(network);
        }
        if (networkAlias != null) {
            buf.append(" --network-alias ");
            buf.append(conditionalQuote(networkAlias));
        }
        if (portMappings != null) {
            for (var s : portMappings) {
                buf.append(" -p ");
                buf.append(conditionalQuote(s));
            }
        }
        if (workdir != null) {
            buf.append(" --workdir ");
            buf.append(alwaysSingleQuote(workdir));
        }
        
        // ------ Assign the volume mounts.
        for (var s : mount) {
            buf.append(" --mount ");
            buf.append(conditionalSingleQuote(s));
        }
        if (tmpfs != null) {
            for (var s : tmpfs) {
                buf.append(" --tmpfs ");
                buf.append(conditionalSingleQuote(s));
            }
        }
        if (volumeMount != null) {
            for (var s : volumeMount) {
                buf.append(" --volume ");
                buf.append(conditionalSingleQuote(s));
            }
        }
        
        // ------ Append the image.
        buf.append(" ");
        buf.append(conditionalQuote(image));
        
        // ------ Append the application arguments.
        if (!StringUtils.isBlank(appArguments))
            buf.append(appArguments); // begins with space char
        
        return buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() 
    {
        return JobUtils.generateEnvVarFileContentForDocker(env);
    }
    
    /* ********************************************************************** */
    /*                           BindMount Class                              */
    /* ********************************************************************** */
    public static final class BindMount
     extends AbstractDockerMount
    {
        private boolean readOnly;
        
        public BindMount() {super(MountType.bind);}
        
        public boolean isReadOnly() {return readOnly;}
        public void setReadOnly(boolean readOnly) {this.readOnly = readOnly;}
        
        @Override
        public String toString()
        {
            // Construct the value of a bind mount (i.e. everything
            // other than the --mount flag).
            final int capacity = 256;
            StringBuilder buf = new StringBuilder(capacity);
            buf.append("type=");
            buf.append(type.name());
            buf.append(",source=");
            buf.append(source);
            buf.append(",target=");
            buf.append(target);
            if (readOnly) buf.append(",readonly");
            // Conditional single quote here matches how user provided --mount arguments are handled
            return conditionalSingleQuote(buf.toString());
        }
    }

    /* ********************************************************************** */
    /*                          TmpfsMount Class                              */
    /* ********************************************************************** */
    public static final class TmpfsMount
     extends AbstractDockerMount
    {
        private String size; // unlimited size by default
        private String mode; // 1777 octal - world writable w/sticky bit by default
        
        public TmpfsMount() {super(MountType.tmpfs);}
        
        public String getSize() {return size;}
        public void setSize(String size) {this.size = size;}
        public String getMode() {return mode;}
        public void setMode(String mode) {this.mode = mode;}
    }
    
    /* ********************************************************************** */
    /*                          VolumeMount Class                             */
    /* ********************************************************************** */
    public static final class VolumeMount
     extends AbstractDockerMount
    {
        private boolean readOnly;
        
        public VolumeMount() {super(MountType.volume);}
        
        public boolean isReadOnly() {return readOnly;}
        public void setReadOnly(boolean readonly) {this.readOnly = readonly;}
        
        @Override
        public String toString()
        {
            // Construct the value of a volume mount (i.e. everything
            // other than the --volume flag).
            final int capacity = 256;
            StringBuilder buf = new StringBuilder(capacity);
            if (readOnly) buf.append(" --read-only");
            buf.append(" ");
            buf.append(VOLUME_CMD); // Placeholder replaced during command generation
            buf.append(" ");
            buf.append(source);
            buf.append(":");
            buf.append(target);
            // Conditional single quote here matches how user provided --volume arguments are handled
            return conditionalSingleQuote(buf.toString());
        }
    }

    /* ********************************************************************** */
    /*                          Top-Level Accessors                           */
    /* ********************************************************************** */
    public String getAddHost() {
        return addHost;
    }

    public void setAddHost(String addHost) {
        this.addHost = addHost;
    }

    public String getCidFile() {
        return cidFile;
    }

    public void setCidFile(String cidFile) {
        this.cidFile = cidFile;
    }

    public String getCpus() {
        return cpus;
    }

    public void setCpus(String cpus) {
        this.cpus = cpus;
    }

    public String getCpusetCPUs() {
        return cpusetCPUs;
    }

    public void setCpusetCPUs(String cpusetCPUs) {
        this.cpusetCPUs = cpusetCPUs;
    }

    public String getCpusetMEMs() {
        return cpusetMEMs;
    }

    public void setCpusetMEMs(String cpusetMEMs) {
        this.cpusetMEMs = cpusetMEMs;
    }

    public List<Pair<String, String>> getEnv() {
        // Initialized on construction.
        return env;
    }

	public String getEntrypoint() {
		return entrypoint;
	}

	public void setEntrypoint(String entrypoint) {
		this.entrypoint = entrypoint;
	}
	
    public void setEnv(List<Pair<String, String>> env) {
        this.env = env;
    }

    public String getEnvFile() {
        return envFile;
    }

    public void setEnvFile(String envFile) {
        this.envFile = envFile;
    }

    public String getGpus() {
        return gpus;
    }

    public void setGpus(String gpus) {
        this.gpus = gpus;
    }

    public List<String> getGroups() {
        if (groups == null) groups = new ArrayList<String>();
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIp6() {
        return ip6;
    }

    public void setIp6(String ip6) {
        this.ip6 = ip6;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public List<Pair<String, String>> getLabels() {
        if (labels == null) labels = new ArrayList<Pair<String,String>>();
        return labels;
    }

    public void setLabels(List<Pair<String, String>> labels) {
        this.labels = labels;
    }

    public String getLogDriver() {
        return logDriver;
    }

    public void setLogDriver(String logDriver) {
        this.logDriver = logDriver;
    }

    public String getLogOpts() {
        return logOpts;
    }

    public void setLogOpts(String logOpts) {
        this.logOpts = logOpts;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public List<String> getMount() {
        return mount;
    }

    public void setMount(List<String> mount) {
        this.mount = mount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getNetworkAlias() {
        return networkAlias;
    }

    public void setNetworkAlias(String networkAlias) {
        this.networkAlias = networkAlias;
    }

    public List<String> getPortMappings() {
        if (portMappings == null) portMappings = new ArrayList<String>();
        return portMappings;
    }

    public void setPortMappings(List<String> portMappings) {
        this.portMappings = portMappings;
    }

    public boolean isRm() {
        return rm;
    }

    public void setRm(boolean rm) {
        this.rm = rm;
    }

    public List<String> getTmpfs() {
        if (tmpfs == null) tmpfs = new ArrayList<String>();
        return tmpfs;
    }

    public void setTmpfs(List<String> tmpfs) {
        this.tmpfs = tmpfs;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<String> getVolumeMount() {
        if (volumeMount == null) volumeMount = new ArrayList<String>();
        return volumeMount;
    }

    public void setVolumeMount(List<String> volumeMount) {
        this.volumeMount = volumeMount;
    }

    public String getWorkdir() {
        return workdir;
    }

    public void setWorkdir(String workdir) {
        this.workdir = workdir;
    }

    public String getAppArguments() {
        return appArguments;
    }

    public void setAppArguments(String appArguments) {
        this.appArguments = appArguments;
    }
}
