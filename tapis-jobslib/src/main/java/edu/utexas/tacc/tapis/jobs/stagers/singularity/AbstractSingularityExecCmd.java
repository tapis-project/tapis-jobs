package edu.utexas.tacc.tapis.jobs.stagers.singularity;

import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.conditionalQuote;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

abstract class AbstractSingularityExecCmd 
  implements JobExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // List fields that we always populate are initialized on construction,
    // all others are initialized on demand.
    private String                    capabilities; // comma separated list
    private List<String>              bind;         // bind specs where each one is a comma separated list of src[:dest[:opts]]
    private boolean                   cleanEnv;     // clean environment before running container
    private boolean                   compat;       // apply settings for increased OCI/Docker compatibility. Infers --containall, --no-init, --no-umask, --writable-tmpfs.
    private boolean                   contain;      // use minimal /dev and empty other directories
    private boolean                   containAll;   // contain file systems and also PID, IPC, and environment
    private boolean                   disableCache; // don't read or write cache
    private String                    dns;          // list of DNS servers separated by commas to add in resolv.conf
    private String                    dropCapabilities; // a comma separated capability list to drop
    private List<Pair<String,String>> env;          // pass environment variable to contained process
    private String                    envFile;      // file of key=value environment assignments
    private List<String>              fusemount;    // A FUSE filesystem mount specification of the form '<type>:<fuse command> <mountpoint>' - where <type> is 'container' or 'host', specifying where the mount will be performed ('container-daemon' or 'host-daemon' will run the FUSE process detached). <fuse command> is the path to the FUSE executable, plus options for the mount. <mountpoint> is the location in the container to which the FUSE mount will be attached. E.g. 'container:sshfs 10.0.0.1:/ /sshfs'. Implies --pid.
    private String                    home;         // either be a src path or src:dest pair
    private String                    hostname;     // set container host name
    private String                    image;        // the full image specification
    private List<String>              mount;        // a mount specification e.g. 'type=bind,source=/opt,destination=/hostopt'.
    private boolean                   net;          // run container in a new network namespace
    private String                    network;      // network type separated by commas
    private List<String>              networkArgs;  // network arguments to pass to CNI plugins
    private boolean                   noHome;       // do NOT mount users home directory
    private boolean                   noInit;       // disable one or more mount xxx options set in singularity.conf
    private List<String>              noMounts;     // disable one or more mount xxx options set in singularity.conf
    private boolean                   noPrivs;      // drop all privileges from root user in container
    private boolean                   noUMask;      // do not propagate umask to the container, set default 0022 umask
    private boolean                   noHTTPS;      // do NOT use HTTPS with the docker:// transport
    private boolean                   nv;           // enable experimental Nvidia support
    private boolean                   nvcli;        // use nvidia-container-cli for GPU setup (experimental)
    private List<String>              overlay;      // use an overlayFS image for persistent data storage
    private String                    pemPath;      // enter an path to a PEM formated RSA key for an encrypted container
    private boolean                   rocm;         // enable experimental Rocm support
    private List<String>              scratch;      // include a scratch directory in container linked to a temporary dir
    private List<String>              security;     // enable security features (SELinux, Apparmor, Seccomp)
    private boolean                   userNs;       // run container in a new user namespace
    private boolean                   uts;          // run container in a new UTS namespace
    private String                    workdir;      // working directory to be used for /tmp, /var/tmp and $HOME (if --contain was also used)
    private boolean                   writable;     // This option makes the container file system accessible as read/write
    private boolean                   writableTmpfs;// makes the file system accessible as read-write with non persistent data (with overlay support only)

    // Fields specific to singularity run.
    private String          app;      // an application to run inside a container
    private boolean         ipc;      // run container in a new IPC namespace
    private boolean         noNet;    // disable VM network handling
    private boolean         pid;      // run container in a new PID namespace
    private String          pwd;      // initial working directory for payload process inside the container
    private boolean         vm;       // enable VM support
    private String          vmCPU;    // number of CPU cores to allocate to Virtual Machine
    private boolean         vmErr;    // enable attaching stderr from VM
    private String          vmIP;     // IP Address to assign for container usage, default is DHCP in bridge network
    private String          vmRAM;    // amount of RAM in MiB to allocate to Virtual Machine (default "1024")

    // Arguments passed to application, which always begin with a space character.
    private String                    appArguments; 
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    public AbstractSingularityExecCmd()
    {
        // Lists we know are not going to be empty.
        env = new ArrayList<Pair<String,String>>();
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addCommonExecArgs:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Fill in container arguments common to both start and run.  Note that
     * env and envFile are processed separately by subclasses.
     * 
     * @param buf the buffer accumulating are argument assignments 
     */
    protected void addCommonExecArgs(StringBuilder buf)
    {
        // ------ Fill in user-supplied options common to start and run.
        if (StringUtils.isNotBlank(getCapabilities())) {
            buf.append(" --add-caps ");
            buf.append(conditionalQuote(getCapabilities()));
        }
        
        if (!bindIsNull() && !getBind().isEmpty()) 
            buf.append(getStringListArgs(" --bind ", getBind()));

        if (isCleanEnv()) buf.append(" --cleanenv");
        if (isCompat()) buf.append(" --compat");
        if (isContain()) buf.append(" --contain");
        if (isContainAll()) buf.append(" --containall");
        if (isDisableCache()) buf.append(" --disable-cache");
        
        if (StringUtils.isNotBlank(getDns())) {
            buf.append(" --dns ");
            buf.append(conditionalQuote(getDns()));
        }
        if (StringUtils.isNotBlank(getDropCapabilities())) {
            buf.append(" --drop-caps ");
            buf.append(conditionalQuote(getDropCapabilities()));
        }
        
        if (!fusemountIsNull() && !getFusemount().isEmpty()) 
            buf.append(getStringListArgs(" --fusemount ", getFusemount()));
        
        if (StringUtils.isNotBlank(getHome())) {
            buf.append(" --home ");
            buf.append(conditionalQuote(getHome()));
        }
        if (StringUtils.isNotBlank(getHostname())) {
            buf.append(" --hostname ");
            buf.append(conditionalQuote(getHostname()));
        }
        
        if (!mountIsNull() && !getMount().isEmpty()) 
            buf.append(getStringListArgs(" --mount ", getMount()));
        
        if (isNet()) buf.append(" --net");
        if (StringUtils.isNotBlank(getNetwork())) {
            buf.append(" --network ");
            buf.append(conditionalQuote(getNetwork()));
        }
        if (!networkArgsIsNull() && !getNetworkArgs().isEmpty()) 
            buf.append(getStringListArgs(" --network-args ", getNetworkArgs()));
        
        if (isNoHome()) buf.append(" --no-home");
        if (isNoInit()) buf.append(" --no-init");
        if (!noMountsIsNull() && !getNoMounts().isEmpty()) 
            buf.append(getStringListArgs(" --no-mount ", getNoMounts()));
        if (isNoPrivs()) buf.append(" --no-privs");
        if (isNoUMask()) buf.append(" --no-umask");
        if (isNoHTTPS()) buf.append(" --nohttps");
        if (isNv()) buf.append(" --nv");
        if (isNvcli()) buf.append(" --nvcli");
        
        if (!overlayIsNull() && !getOverlay().isEmpty()) 
            buf.append(getStringListArgs(" --overlay ", getOverlay()));
        if (StringUtils.isNotBlank(getPemPath())) {
            buf.append(" --pem-path ");
            buf.append(conditionalQuote(getPemPath()));
        }
        if (isRocm()) buf.append(" --rocm");
        
        if (!scratchIsNull() && !getScratch().isEmpty()) 
            buf.append(getStringListArgs(" --scratch ", getScratch()));
        if (!securityIsNull() && !getSecurity().isEmpty()) 
            buf.append(getStringListArgs(" --security ", getSecurity()));
        
        if (isUserNs()) buf.append(" --userns");
        if (isUts()) buf.append(" --uts");
        if (StringUtils.isNotBlank(getWorkdir())) {
            buf.append(" --workdir ");
            buf.append(conditionalQuote(getWorkdir()));
        }
        if (isWritable()) buf.append(" --writable");
        if (isWritableTmpfs()) buf.append(" --writable-tmpfs");
    }

    /* ---------------------------------------------------------------------- */
    /* addRunSpecificArgs:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Add the container arguments that are specific to singularity run
     *
     * @param buf the command buffer
     */
    protected void addRunSpecificArgs(StringBuilder buf)
    {
        if (StringUtils.isNotBlank(getApp())) {
            buf.append(" --app ");
            buf.append(getApp());
        }

        if (isIpc()) buf.append(" --ipc");
        if (isNoNet()) buf.append(" --nonet");
        if (isPid()) buf.append(" --pid");

        if (StringUtils.isNotBlank(getPwd())) {
            buf.append(" --pwd ");
            buf.append(conditionalQuote(getPwd()));
        }

        if (isVm()) buf.append(" --vm");
        if (StringUtils.isNotBlank(getVmCPU())) {
            buf.append(" --vm-cpu ");
            buf.append(conditionalQuote(getVmCPU()));
        }
        if (isVmErr()) buf.append(" --vm-err");
        if (StringUtils.isNotBlank(getVmIP())) {
            buf.append(" --vm-ip ");
            buf.append(conditionalQuote(getVmIP()));
        }
        if (StringUtils.isNotBlank(getVmRAM())) {
            buf.append(" --vm-ram ");
            buf.append(conditionalQuote(getVmRAM()));
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getPairListArgs:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Create the string of key=value pairs separated by new line characters.
     * 
     * Similar code can be found at AbstractJobExecStager.getExportPairListArgs().
     * 
     * @param pairs NON-EMPTY list of pair values, one per occurrence
     * @return the string that contains all assignments
     */
    protected String getPairListArgs(List<Pair<String,String>> pairs)
    {
        // Get a buffer to accumulate the key/value pairs.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // Create a list of key=value assignment, each followed by a new line.
        for (var v : pairs) {
            buf.append(v.getLeft());
            buf.append("=");
            buf.append(TapisUtils.conditionalQuote(v.getRight()));
            buf.append("\n");
        }
        return buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getStringListArgs:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Create the string of multiple occurrence arguments from a non-empty
     * list. The result is a string that concatenates " --arg value" for each
     * value in the list.
     * 
     * @param arg the multiple occurrence argument padded with spaces on both sides 
     * @param values NON-EMPTY list of values, one per occurrence
     * @return the string that contains all assignments
     */
    protected String getStringListArgs(String arg, List<String> values)
    {
        String s = "";
        for (var v : values) s += arg + conditionalQuote(v);
        return s;
    }
    
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    // List null checks.
    public boolean bindIsNull()        {return bind == null;}
    public boolean envIsNull()         {return env == null;}
    public boolean fusemountIsNull()   {return fusemount == null;}
    public boolean mountIsNull()       {return mount == null;}
    public boolean networkArgsIsNull() {return networkArgs == null;}
    public boolean noMountsIsNull()    {return noMounts == null;}
    public boolean overlayIsNull()     {return overlay == null;}
    public boolean scratchIsNull()     {return scratch == null;}
    public boolean securityIsNull()    {return security == null;}
    
    public String getCapabilities() {
        return capabilities;
    }
    public void setCapabilities(String capabilities) {
        this.capabilities = capabilities;
    }
    public List<String> getBind() {
    	if (bind == null) bind = new ArrayList<String>();
        return bind;
    }
    public void setBind(List<String> bind) {
        this.bind = bind;
    }
    public boolean isCleanEnv() {
        return cleanEnv;
    }
    public void setCleanEnv(boolean cleanEnv) {
        this.cleanEnv = cleanEnv;
    }
    public boolean isCompat() {
        return compat;
    }
    public void setCompat(boolean compat) {
        this.compat = compat;
    }
    public boolean isContain() {
        return contain;
    }
    public void setContain(boolean contain) {
        this.contain = contain;
    }
    public boolean isContainAll() {
        return containAll;
    }
    public void setContainAll(boolean containAll) {
        this.containAll = containAll;
    }
    public boolean isDisableCache() {
        return disableCache;
    }
    public void setDisableCache(boolean disableCache) {
        this.disableCache = disableCache;
    }
    public String getDns() {
        return dns;
    }
    public void setDns(String dns) {
        this.dns = dns;
    }
    public String getDropCapabilities() {
        return dropCapabilities;
    }
    public void setDropCapabilities(String dropCapabilities) {
        this.dropCapabilities = dropCapabilities;
    }
    public List<Pair<String,String>> getEnv() {
        if (env == null) env = new ArrayList<Pair<String,String>>();
        return env;
    }
    public void setEnv(List<Pair<String,String>> env) {
        this.env = env;
    }
    public String getEnvFile() {
        return envFile;
    }
    public void setEnvFile(String envFile) {
        this.envFile = envFile;
    }
    public List<String> getFusemount() {
        if (fusemount == null) fusemount = new ArrayList<String>();
        return fusemount;
    }
    public void setFusemount(List<String> fusemount) {
        this.fusemount = fusemount;
    }
    public String getHome() {
        return home;
    }
    public void setHome(String home) {
        this.home = home;
    }
    public String getHostname() {
        return hostname;
    }
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
    public String getImage() {
        return image;
    }
    public void setImage(String image) {
        this.image = image;
    }
    public List<String> getMount() {
        if (mount == null) mount = new ArrayList<String>();
        return mount;
    }
    public void setMount(List<String> mount) {
        this.mount = mount;
    }
    public boolean isNet() {
        return net;
    }
    public void setNet(boolean net) {
        this.net = net;
    }
    public String getNetwork() {
        return network;
    }
    public void setNetwork(String network) {
        this.network = network;
    }
    public List<String> getNetworkArgs() {
        if (networkArgs == null) networkArgs = new ArrayList<String>();
        return networkArgs;
    }
    public void setNetworkArgs(List<String> networkArgs) {
        this.networkArgs = networkArgs;
    }
    public boolean isNoHome() {
        return noHome;
    }
    public void setNoHome(boolean noHome) {
        this.noHome = noHome;
    }
    public boolean isNoInit() {
        return noInit;
    }
    public void setNoInit(boolean noInit) {
        this.noInit = noInit;
    }
    public List<String> getNoMounts() {
        if (noMounts == null) noMounts = new ArrayList<String>();
        return noMounts;
    }
    public void setNoMounts(List<String> noMounts) {
        this.noMounts = noMounts;
    }
    public boolean isNoPrivs() {
        return noPrivs;
    }
    public void setNoPrivs(boolean noPrivs) {
        this.noPrivs = noPrivs;
    }
    public boolean isNoUMask() {
        return noUMask;
    }
    public void setNoUMask(boolean noUMask) {
        this.noUMask = noUMask;
    }
    public boolean isNoHTTPS() {
        return noHTTPS;
    }
    public void setNoHTTPS(boolean noHTTPS) {
        this.noHTTPS = noHTTPS;
    }
    public boolean isNv() {
        return nv;
    }
    public void setNv(boolean nv) {
        this.nv = nv;
    }
    public boolean isNvcli() {
        return nvcli;
    }
    public void setNvcli(boolean nvcli) {
        this.nvcli = nvcli;
    }
    public List<String> getOverlay() {
        if (overlay == null) overlay = new ArrayList<String>();
        return overlay;
    }
    public void setOverlay(List<String> overlay) {
        this.overlay = overlay;
    }
    public String getPemPath() {
        return pemPath;
    }
    public void setPemPath(String pemPath) {
        this.pemPath = pemPath;
    }
    public boolean isRocm() {
        return rocm;
    }
    public void setRocm(boolean rocm) {
        this.rocm = rocm;
    }
    public List<String> getScratch() {
        if (scratch == null) scratch = new ArrayList<String>();
        return scratch;
    }
    public void setScratch(List<String> scratch) {
        this.scratch = scratch;
    }
    public List<String> getSecurity() {
        if (security == null) security = new ArrayList<String>();
        return security;
    }
    public void setSecurity(List<String> security) {
        this.security = security;
    }
    public boolean isUserNs() {
        return userNs;
    }
    public void setUserNs(boolean userNs) {
        this.userNs = userNs;
    }
    public boolean isUts() {
        return uts;
    }
    public void setUts(boolean uts) {
        this.uts = uts;
    }
    public String getWorkdir() {
        return workdir;
    }
    public void setWorkdir(String workdir) {
        this.workdir = workdir;
    }
    public boolean isWritable() {
        return writable;
    }
    public void setWritable(boolean writable) {
        this.writable = writable;
    }
    public boolean isWritableTmpfs() {
        return writableTmpfs;
    }
    public void setWritableTmpfs(boolean writableTmpfs) {
        this.writableTmpfs = writableTmpfs;
    }

    public String getAppArguments() {
        return appArguments;
    }
    public void setAppArguments(String appArguments) {
        this.appArguments = appArguments;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public boolean isIpc() {
        return ipc;
    }

    public void setIpc(boolean ipc) {
        this.ipc = ipc;
    }

    public boolean isNoNet() {
        return noNet;
    }

    public void setNoNet(boolean noNet) {
        this.noNet = noNet;
    }

    public boolean isPid() {
        return pid;
    }

    public void setPid(boolean pid) {
        this.pid = pid;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public boolean isVm() {
        return vm;
    }

    public void setVm(boolean vm) {
        this.vm = vm;
    }

    public String getVmCPU() {
        return vmCPU;
    }

    public void setVmCPU(String vmCPU) {
        this.vmCPU = vmCPU;
    }

    public boolean isVmErr() {
        return vmErr;
    }

    public void setVmErr(boolean vmErr) {
        this.vmErr = vmErr;
    }

    public String getVmIP() {
        return vmIP;
    }

    public void setVmIP(String vmIP) {
        this.vmIP = vmIP;
    }

    public String getVmRAM() {
        return vmRAM;
    }

    public void setVmRAM(String vmRAM) {
        this.vmRAM = vmRAM;
    }
}
