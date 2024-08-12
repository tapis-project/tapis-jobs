package edu.utexas.tacc.tapis.jobs.worker.execjob;

import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.ZIP_FILE_RM_FROM_EXECDIR_FMT;
import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.ZIP_SETEXEC_CMD_FMT;
import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.ZIP_UNTAR_CMD_FMT;
import static edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils.ZIP_UNZIP_CMD_FMT;
import static edu.utexas.tacc.tapis.shared.utils.TapisUtils.alwaysSingleQuote;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.client.FilesClient;
import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;
import edu.utexas.tacc.tapis.files.client.gen.model.ReqTransfer;
import edu.utexas.tacc.tapis.files.client.gen.model.ReqTransferElement;
import edu.utexas.tacc.tapis.files.client.gen.model.ReqTransferElement.TransferTypeEnum;
import edu.utexas.tacc.tapis.files.client.gen.model.TransferTask;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao.TransferValueType;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.filesmonitor.TransferMonitorFactory;
import edu.utexas.tacc.tapis.jobs.model.IncludeExcludeFilter;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.submit.JobFileInput;
import edu.utexas.tacc.tapis.jobs.recover.RecoveryUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHScpClient;
import edu.utexas.tacc.tapis.shared.uri.TapisLocalUrl;
import edu.utexas.tacc.tapis.shared.uri.TapisUrl;
import edu.utexas.tacc.tapis.shared.utils.FilesListSubtree;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;

public final class JobFileManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobFileManager.class);
    
    // Special transfer id value indicating no files to stage.
    private static final String NO_FILE_INPUTS = "no inputs";
    
    // Filters are interpreted as globs unless they have this prefix.
    public static final String REGEX_FILTER_PREFIX = "REGEX:";
    
    // Various useful posix permission settings.
    public static final List<PosixFilePermission> RWRW   = SSHScpClient.RWRW_PERMS;
    public static final List<PosixFilePermission> RWXRWX = SSHScpClient.RWXRWX_PERMS;
    
    // Placeholder values used in URLs for DTN support. 
    private static final String SYSTEM_PLACEHOLER = "{SYSTEM_PLACEHOLER}";
    private static final String PATH_PLACEHOLER   = "{PATH_PLACEHOLER}";
    
    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // We transfer files in these phases of job processing.
    private enum JobTransferPhase {INPUT, ARCHIVE, STAGE_APP, DTN_IN, DTN_OUT}
    
    // Archive filter types.
    private enum FilterType {INCLUDES, EXCLUDES}
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The initialized job context.
    private final JobExecutionContext _jobCtx;
    private final Job                 _job;
    
    // Unpack shared context directory settings.
    private final String              _shareExecSystemInputDirAppOwner;
    private final String              _shareExecSystemExecDirAppOwner;
    private final String              _shareExecSystemOutputDirAppOwner;
    private final String              _shareArchiveSystemDirAppOwner;
    private final String              _shareDtnSystemInputDirAppOwner;
    private final String              _shareDtnSystemOutputDirAppOwner;
    
    // Derived path prefix value removed before filtering.
    private String                    _filterIgnoreOutputPrefix;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobFileManager(JobExecutionContext ctx)
    {
        _jobCtx = ctx;
        _job = ctx.getJob();
        
        // Empty string means not shared.
        _shareExecSystemInputDirAppOwner  = ctx.getJobSharedAppCtx().getSharingExecSystemInputDirAppOwner();
        _shareExecSystemExecDirAppOwner   = ctx.getJobSharedAppCtx().getSharingExecSystemExecDirAppOwner();
        _shareExecSystemOutputDirAppOwner = ctx.getJobSharedAppCtx().getSharingExecSystemOutputDirAppOwner();
        _shareArchiveSystemDirAppOwner    = ctx.getJobSharedAppCtx().getSharingArchiveSystemDirAppOwner();
        _shareDtnSystemInputDirAppOwner   = ctx.getJobSharedAppCtx().getSharingDtnSystemInputDirAppOwner();
        _shareDtnSystemOutputDirAppOwner  = ctx.getJobSharedAppCtx().getSharingDtnSystemOutputDirAppOwner();
    }
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* createDirectories:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Create the directories used for I/O on this job.  The directories may
     * already exist.
     * 
     * @throws TapisImplException
     * @throws TapisServiceConnectionException
     */
    public void createDirectories() 
     throws TapisException, TapisServiceConnectionException
    {
        // Get the client from the context.
        FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
        
        // Get the IO targets for the job and check that the systems are enabled.
        var ioTargets = _jobCtx.getJobIOTargets();
        
        // Create a set to that records the directories already created.
        var createdSet = new HashSet<String>();
        
        // ---------------------- Exec System Exec Dir ----------------------
        // Create the directory on the system.
        try {
            filesClient.mkdir(ioTargets.getExecTarget().systemId, 
                              ioTargets.getExecTarget().dir, _shareExecSystemExecDirAppOwner);
        } catch (TapisClientException e) {
            String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                         ioTargets.getExecTarget().host,
                                         _job.getOwner(), _job.getTenant(),
                                         ioTargets.getExecTarget().dir, e.getCode());
            throw new TapisImplException(msg, e, e.getCode());
        }
        
        // Save the created directory key to avoid attempts to recreate it.
        createdSet.add(getDirectoryKey(ioTargets.getExecTarget().systemId, 
                                       ioTargets.getExecTarget().dir));
        
        // ---------------------- Exec System Output Dir ----------------- 
        // See if the output dir is the same as the exec dir.
        var execSysOutputDirKey = getDirectoryKey(ioTargets.getOutputTarget().systemId, 
                                                  ioTargets.getOutputTarget().dir);
        if (!createdSet.contains(execSysOutputDirKey)) {
            // Create the directory on the system.
            try {
                filesClient.mkdir(ioTargets.getOutputTarget().systemId, 
                                  _job.getExecSystemOutputDir(), _shareExecSystemOutputDirAppOwner);
            } catch (TapisClientException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             ioTargets.getOutputTarget().host,
                                             _job.getOwner(), _job.getTenant(),
                                             ioTargets.getOutputTarget().dir, e.getCode());
                throw new TapisImplException(msg, e, e.getCode());
            }
            
            // Save the created directory key to avoid attempts to recreate it.
            createdSet.add(execSysOutputDirKey);
        }
        
        // ---------------------- Exec System Input Dir ------------------ 
        // See if the input dir is the same as any previously created dir.
        var execSysInputDirKey = getDirectoryKey(ioTargets.getInputTarget().systemId, 
                                                 ioTargets.getInputTarget().dir);
        if (!createdSet.contains(execSysInputDirKey)) {
            // Create the directory on the system.
            try {
                filesClient.mkdir(ioTargets.getInputTarget().systemId, 
                                  ioTargets.getInputTarget().dir, _shareExecSystemInputDirAppOwner);
            } catch (TapisClientException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             ioTargets.getInputTarget().host,
                                             _job.getOwner(), _job.getTenant(),
                                             ioTargets.getInputTarget().dir, e.getCode());
                throw new TapisImplException(msg, e, e.getCode());
            }
            
            // Save the created directory key to avoid attempts to recreate it.
            createdSet.add(execSysInputDirKey);
        }
        
        // ---------------------- DTN System Input Dir ------------------- 
        // Most jobs don't use a dtn.
        if (ioTargets.getDtnInputTarget() != null) {
        	// See if the input dir is the same as any previously created dir.
        	var dtnSysInputDirKey = getDirectoryKey(ioTargets.getDtnInputTarget().systemId, 
                                                 	ioTargets.getDtnInputTarget().dir);
        	if (!createdSet.contains(dtnSysInputDirKey)) {
        		// Create the directory on the system.
        		try {
        			filesClient.mkdir(ioTargets.getDtnInputTarget().systemId, 
                                 	  ioTargets.getDtnInputTarget().dir, _shareDtnSystemInputDirAppOwner);
        		} catch (TapisClientException e) {
        			String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             	 ioTargets.getDtnInputTarget().host,
                                             	 _job.getOwner(), _job.getTenant(),
                                             	 ioTargets.getDtnInputTarget().dir, e.getCode());
        			throw new TapisImplException(msg, e, e.getCode());
        		}
            
        		// Save the created directory key to avoid attempts to recreate it.
        		createdSet.add(dtnSysInputDirKey);
        	}
        }
        
        // ---------------------- DTN System Output Dir ------------------ 
        // Most jobs don't use a dtn.
        if (ioTargets.getDtnOutputTarget() != null) {
        	// See if the output dir is the same as any previously created dir.
        	var dtnSysOutputDirKey = getDirectoryKey(ioTargets.getDtnOutputTarget().systemId, 
                                                 	ioTargets.getDtnOutputTarget().dir);
        	if (!createdSet.contains(dtnSysOutputDirKey)) {
        		// Create the directory on the system.
        		try {
        			filesClient.mkdir(ioTargets.getDtnOutputTarget().systemId, 
                                 	  ioTargets.getDtnOutputTarget().dir, _shareDtnSystemOutputDirAppOwner);
        		} catch (TapisClientException e) {
        			String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             	 ioTargets.getDtnOutputTarget().host,
                                             	 _job.getOwner(), _job.getTenant(),
                                             	 ioTargets.getDtnOutputTarget().dir, e.getCode());
        			throw new TapisImplException(msg, e, e.getCode());
        		}
            
        		// Save the created directory key to avoid attempts to recreate it.
        		createdSet.add(dtnSysOutputDirKey);
        	}
        }
        
        // ---------------------- Archive System Dir ---------------------
        // See if the archive dir is the same as any previously created dir.
        // There is no mkdir command on S3 systems, so we skip those systems. 
        var archiveSysDirKey = getDirectoryKey(_job.getArchiveSystemId(), 
                                               _job.getArchiveSystemDir());
        if (!createdSet.contains(archiveSysDirKey) && 
        	_jobCtx.getArchiveSystem().getSystemType() != SystemTypeEnum.S3) 
        {
            // Create the directory on the system.
            try {
                var sharedAppCtx = _jobCtx.getJobSharedAppCtx().getSharingArchiveSystemDirAppOwner();
                filesClient.mkdir(_job.getArchiveSystemId(), 
                                  _job.getArchiveSystemDir(), sharedAppCtx);
            } catch (TapisClientException e) {
                String msg = MsgUtils.getMsg("FILES_REMOTE_MKDIRS_ERROR", 
                                             _jobCtx.getArchiveSystem().getHost(),
                                             _job.getOwner(), _job.getTenant(),
                                             _job.getArchiveSystemDir(), e.getCode());
                throw new TapisImplException(msg, e, e.getCode());
            }
        }
    }

    /* ---------------------------------------------------------------------- */
    /* stageAppAssets:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Perform or restart the app assets staging process. Both recoverable
     * and non-recoverable exceptions can be thrown from here.
     * This may involve calling the Files service to start or resume a transfer.
     *
     * @throws TapisException on error
     */
    public void stageAppAssets(String containerImage, boolean containerImageIsUrl, String appArchiveFile)
            throws TapisException
    {
        // If a url, then start or restart a file transfer and wait for it to finish.
        if (!containerImageIsUrl) return;
        
        // If a url, then start or restart a file transfer and wait for it to finish.
        // Create the transfer request. sourceUrl is the containerImage
        String sourceUrl = containerImage;
        // Build destUrl from exec system and path = execSystemExecDir
        String destUrl = makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemExecDir(), appArchiveFile);

        // Determine sharing info for sourceUrl and destinationUrl
        String sharingOwnerSourceUrl = _jobCtx.getJobSharedAppCtx().getSharingContainerImageUrlAppOwner();;
        String sharingOwnerDestUrl   = _shareExecSystemExecDirAppOwner;

        var reqTransfer = new ReqTransfer();
        var task = new ReqTransferElement().sourceURI(sourceUrl).destinationURI(destUrl);
        task.setOptional(false);
        task.setSrcSharedCtx(sharingOwnerSourceUrl);
        task.setDestSharedCtx(sharingOwnerDestUrl);
        reqTransfer.addElementsItem(task);
        // Transfer the app archive file. This method will start or restart the transfer and monitor
        //   it until it completes.
        stageAppZipFile(reqTransfer);
    }

    /* ---------------------------------------------------------------------- */
    /* stageInputs:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Perform or restart the input file staging process.  Both recoverable
     * and non-recoverable exceptions can be thrown from here.
     * 
     * @throws TapisException on error
     */
    public void stageInputs() throws TapisException
    {
        // Determine if we are restarting a previous staging request.
        var transferInfo = _jobCtx.getJobsDao().getTransferInfo(_job.getUuid());
        String transferId = transferInfo.inputTransactionId;
        String corrId     = transferInfo.inputCorrelationId;
        
        // Assign the local dtn usage flag;
        final var useDtn = _jobCtx.useDtnInput();
        
        // See if the transfer id has been set for this job (this implies the
        // correlation id has also been set).  If so, then the job had already 
        // submitted its transfer request and we are now in recovery processing.  
        // There's no need to resubmit the transfer request in this case.  
        // 
        // It's possible that the corrId was set but we died before the transferId
        // was saved.  In this case, we simply generate a new corrId and resubmit.
        if (StringUtils.isBlank(transferId)) {
            corrId = UUID.randomUUID().toString();
            transferId = stageNewInputs(corrId, useDtn);
        }
        
        // Is there anything to transfer?
        if (transferId.equals(NO_FILE_INPUTS)) return;
        _log.info(MsgUtils.getMsg("JOBS_FILE_TRANSFER_INFO", _job.getUuid(), 
                                  _job.getStatus().name(), transferId, corrId));
        
        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        // Events are not posted when using a DTN until the final move is completed.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, corrId, !useDtn);
        
        // DTN post-processing.
        if (useDtn) moveDtnInputs();
    }
    
    /* ---------------------------------------------------------------------- */
    /* archiveOutputs:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Perform or restart the output file archiving process.  Both recoverable
     * and non-recoverable exceptions can be thrown from here.
     * 
     * @throws TapisException on error
     * @throws TapisClientException 
     */
    public void archiveOutputs() throws TapisException, TapisClientException
    {
        // Determine if archiving is necessary.
        if (_job.getRemoteOutcome() == JobRemoteOutcome.FAILED_SKIP_ARCHIVE) return;
        
        // Determine if we are restarting a previous archiving request.
        var transferInfo = _jobCtx.getJobsDao().getTransferInfo(_job.getUuid());
        String transferId = transferInfo.archiveTransactionId;
        String corrId     = transferInfo.archiveCorrelationId;
        
        // See if the transfer id has been set for this job (this implies the
        // correlation id has also been set).  If so, then the job had already 
        // submitted its transfer request and we are now in recovery processing.  
        // There's no need to resubmit the transfer request in this case.  
        // 
        // It's possible that the corrId was set but we died before the transferId
        // was saved.  In this case, we simply generate a new corrId.
        if (StringUtils.isBlank(transferId)) {
            corrId = UUID.randomUUID().toString();
            transferId = archiveNewOutputs(corrId);
        }
        
        // Is there anything to transfer?
        if (transferId.equals(NO_FILE_INPUTS)) return;
        _log.info(MsgUtils.getMsg("JOBS_FILE_TRANSFER_INFO", _job.getUuid(), 
                                  _job.getStatus().name(), transferId, corrId));

        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, corrId);
    }
    
    /* ---------------------------------------------------------------------- */
    /* cancelTransfer:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Best effort attempt to cancel a transfer.
     * 
     * @param transferId the transfer's uuid
     */
    public void cancelTransfer(String transferId)
    {
        // Get the client from the context.
        FilesClient filesClient = null;
        try {filesClient = _jobCtx.getServiceClient(FilesClient.class);}
            catch (Exception e) {
                _log.error(e.getMessage(), e);
                return;
            }
        
        // Issue the cancel command.
        try {filesClient.cancelTransferTask(transferId);}
            catch (Exception e) {_log.error(e.getMessage(), e);}
    }
    
    /* ---------------------------------------------------------------------- */
    /* installExecFile:                                                       */
    /* ---------------------------------------------------------------------- */

    /**
     * installExecFile
     * Create a file in ExecSystemExecDir
     * @param content - file contents
     * @param fileName - file name
     * @param mod - list of posix permissions
     * @throws TapisException - on error
     */
    public void installExecFile(String content, String fileName, List<PosixFilePermission> mod)
      throws TapisException
    {
        // Calculate the destination file path.
        String destPath = makePath(JobExecutionUtils.getExecDir(_jobCtx, _job), fileName);
        // Always single quote the path, in case it has spaces, parentheses, etc.
        destPath = alwaysSingleQuote(destPath); // SCP doesn't treat spaces like SFTP!

        // Transfer the wrapper script.
        try {
            // Initialize a scp client.
            var scpClient = _jobCtx.getExecSystemTapisSSH().getScpClient();
            scpClient.uploadBytesToFile(content.getBytes(), destPath, mod, null);
        } 
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SFTP_CMD_ERROR", 
                                         _jobCtx.getExecutionSystem().getId(),
                                         _jobCtx.getExecutionSystem().getHost(),
                                         _jobCtx.getExecutionSystem().getEffectiveUserId(),
                                         _jobCtx.getExecutionSystem().getTenant(),
                                         _job.getUuid(),
                                         destPath, e.getMessage());
            throw new JobException(msg, e);
        } 
    }

    /* ---------------------------------------------------------------------- */
    /* extractZipAppArchive:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Extract the application archive file into the exec system directory.
     *
     * @param archiveAbsolutePath location of archive file
     * @throws TapisException on error
     */
    public void extractZipArchive(String archiveAbsolutePath, boolean archiveIsZip)
            throws TapisException
    {
        String host = _jobCtx.getExecutionSystem().getHost();
        // Calculate the file path to where archive will be unpacked.
        String execDir = alwaysSingleQuote(JobExecutionUtils.getExecDir(_jobCtx, _job));
        String quotedArchiveAbsolutePath = alwaysSingleQuote(archiveAbsolutePath);

        // Build the command to extract the archive
        String cmd;
        if (archiveIsZip) cmd = String.format(ZIP_UNZIP_CMD_FMT, execDir, quotedArchiveAbsolutePath);
        else cmd = String.format(ZIP_UNTAR_CMD_FMT, execDir, quotedArchiveAbsolutePath);
        // Log the command we are about to issue.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_ZIP_EXTRACT_CMD", _job.getUuid(), host, cmd));

        // Run the command to extract the app archive
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsTrimmedString();

        // Log exit code and result
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_ZIP_EXTRACT_EXIT", _job.getUuid(), host, cmd, exitStatus, result));

        // If non-zero exit code consider it a failure. Throw non-recoverable exception.
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_ZIP_EXTRACT_ERROR", _job.getUuid(), host, cmd, exitStatus, result);
            throw new TapisException(msg);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* removeZipAppArchive:                                                   */
    /* ---------------------------------------------------------------------- */
    /** If containerImage was a URL then the ZIP app archive file was transferred 
     * onto the exec host using a URL and we should remove it once job is done.
     *
     * @throws TapisException on error
     */
    public void removeZipAppArchive() throws TapisException
    {
        // For convenience and clarity set some variables.
        String jobUuid = _job.getUuid();
        String containerImage =  _jobCtx.getApp().getContainerImage();
        containerImage = containerImage == null ? "" : containerImage;

        // If an absolute path nothing to do
        if (containerImage.startsWith("/")) return;

        // Figure out the name of the zip file.
        String msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER_RM", jobUuid, containerImage);
        _log.debug(msg);
        // Not a path, so should be a URL in a format supported by Files service. Validate it.
        Matcher matcher = JobFileInput.URL_PATTERN.matcher(containerImage);
        if (!matcher.find())
        {
            msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER_URL_INVALID", jobUuid, containerImage);
            throw new JobException(msg);
        }
        // Extract and normalize the path in the URL. If no path set then use /
        String urlPathStr = Optional.ofNullable(matcher.group(3)).orElse("/");
        // Get file name from the path and set full path to app archive
        Path urlPath = Path.of(FilenameUtils.normalize(urlPathStr));
        String zipFileName = urlPath.getFileName().toString();
        // Do simple validation of app archive file name.
        if (StringUtils.isBlank(zipFileName) || "/".equals(zipFileName))
        {
            msg = MsgUtils.getMsg("JOBS_ZIP_CONTAINER_FILENAME_ERR", jobUuid, containerImage, zipFileName);
            throw new JobException(msg);
        }

        // Remove the file
        removeFileFromExecDir(zipFileName);
    }

    /* ---------------------------------------------------------------------- */
    /* checkForCommand:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Check if the specified command is available on the execution host.
     * Uses command -v.
     * Throws exception if not available
     *
     * @param command Executable to check
     * @throws TapisException on error
     */
    public void checkForCommand(String command)
            throws TapisException
    {
        String host = _jobCtx.getExecutionSystem().getHost();
        // Build the command to run the check
        String cmd = String.format("command -V %s", command);
        // Log the command we are about to issue.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_CHECK_CMD", _job.getUuid(), host, cmd));

        // Run the command to check
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsTrimmedString();

        // Log exit code and result
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_CHECK_CMD_EXIT", _job.getUuid(), host, cmd, exitStatus, result));

        // If non-zero exit code consider it a failure. Throw non-recoverable exception.
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_CHECK_CMD_ERROR", _job.getUuid(), host, cmd, exitStatus, result);
            throw new TapisException(msg);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* runZipSetAppExecutable:                                                */
    /* ---------------------------------------------------------------------- */
    /** Run a script to determine the app executable for ZIP runtime applications.
     * The relative path to the app executable is returned. The path is relative to execSystemExecDir
     *
     * @param setAppExecScript name of script to run
     * @throws TapisException on error
     */
    public String runZipSetAppExecutable(String setAppExecScript)
            throws TapisException
    {
        String host = _jobCtx.getExecutionSystem().getHost();

        // Calculate the file path to where the script will be run.
        String execDir = alwaysSingleQuote(JobExecutionUtils.getExecDir(_jobCtx, _job));
        // Build the command to run the script.
        String cmd = String.format(ZIP_SETEXEC_CMD_FMT, execDir, setAppExecScript);
        // Log the command we are about to issue.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_ZIP_SETEXEC_CMD", _job.getUuid(), host, cmd));

        // Run the command to extract the app archive
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsTrimmedString();

        // Log exit code and result
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_ZIP_SETEXEC_EXIT", _job.getUuid(), host, cmd, exitStatus, result));

        // If non-zero exit code consider it a failure. Throw non-recoverable exception.
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_ZIP_SETEXEC_ERROR", _job.getUuid(), host, cmd, exitStatus, result);
            throw new TapisException(msg);
        }
        // We expect the output to be a single line to the app executable to run, but sometimes extraneous text
        // precedes the output we are looking for. So extract just the final line.
        return TapisUtils.getLastLine(result);
    }

    /* ---------------------------------------------------------------------- */
    /* removeFileFromExecDir:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Remove a file from execSystemExecDir
     *
     * @param fileName name of file to remove
     * @throws TapisException on error
     */
    public void removeFileFromExecDir(String fileName)
            throws TapisException
    {
        String host = _jobCtx.getExecutionSystem().getHost();

        // Calculate the file path
        String execDir = alwaysSingleQuote(JobExecutionUtils.getExecDir(_jobCtx, _job));
        // Build the command to delete the archive file
        String cmd = String.format(ZIP_FILE_RM_FROM_EXECDIR_FMT, execDir, fileName);
        // Log the command we are about to issue.
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_ZIP_FILE_RM_CMD", _job.getUuid(), host, cmd));

        // Run the command to remove the file
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        String result  = runCmd.getOutAsTrimmedString();

        // Log exit code and result
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_ZIP_FILE_RM_EXIT", _job.getUuid(), host, cmd, exitStatus, result));

        // If non-zero exit code consider it a failure. Throw an exception.
        if (exitStatus != 0) {
            String msg = MsgUtils.getMsg("JOBS_ZIP_FILE_RM_ERROR", _job.getUuid(), host, cmd, exitStatus, result);
            throw new TapisException(msg);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysInputPath:                                               */
    /* ---------------------------------------------------------------------- */
    /** Make the absolute path on the exec system starting at the rootDir, 
     * including the input directory and ending with 0 or more other segments.
     * 
     * @param more 0 or more path segments
     * @return the absolute path
     * @throws TapisException 
     */
    public String makeAbsExecSysInputPath(String... more) 
     throws TapisException
    {
        String[] components = new String[1 + more.length];
        components[0] = _job.getExecSystemInputDir();
        for (int i = 0; i < more.length; i++) components[i+1] = more[i];
        return makePath(_jobCtx.getExecutionSystem().getRootDir(), 
                        components);
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysExecPath:                                                */
    /* ---------------------------------------------------------------------- */
    /** Make the absolute path on the exec system starting at the rootDir, 
     * including the exec directory and ending with 0 or more other segments.
     * 
     * @param more 0 or more path segments
     * @return the absolute path
     * @throws TapisException 
     */
    public String makeAbsExecSysExecPath(String... more) 
     throws TapisException
    {
        String[] components = new String[1 + more.length];
        components[0] = _job.getExecSystemExecDir();
        for (int i = 0; i < more.length; i++) components[i+1] = more[i];
        return makePath(_jobCtx.getExecutionSystem().getRootDir(), 
                        components);
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysOutputPath:                                              */
    /* ---------------------------------------------------------------------- */
    /** Make the absolute path on the exec system starting at the rootDir, 
     * including the output directory and ending with 0 or more other segments.
     * 
     * @param more 0 or more path segments
     * @return the absolute path
     * @throws TapisException 
     */
    public String makeAbsExecSysOutputPath(String... more) 
     throws TapisException
    {
        String[] components = new String[1 + more.length];
        components[0] = _job.getExecSystemOutputDir();
        for (int i = 0; i < more.length; i++) components[i+1] = more[i];
        return makePath(_jobCtx.getExecutionSystem().getRootDir(), 
                        components);
    }
    
    /* ---------------------------------------------------------------------- */
    /* makeAbsExecSysTapisLocalPath:                                          */
    /* ---------------------------------------------------------------------- */
    /** Construct the absolute path on the execution system where the pre-positioned
     * tapislocal input resides.
     * 
     * @param execSystemRootDir the root directory of the execution system
     * @param sourceUrl the path under the root directory where the input resides
     * @return the absolute path on the execution system where the input resides 
     */
    public String makeAbsExecSysTapisLocalPath(String execSystemRootDir, 
                                               String sourceUrl)
    {
        return makePath(execSystemRootDir, TapisUtils.extractFilename(sourceUrl));
    }

    /* ---------------------------------------------------------------------- */
    /* makeSystemUrl:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Create a tapis url based on the systemId, a base path on that system
     * and a file pathname.
     *
     * Implicit in the tapis protocol is that the Files service will prefix path
     * portion of the url with  the execution system's rootDir when actually
     * transferring files.
     *
     * The pathName can be null or empty.
     *
     * @param systemId the target tapis system
     * @param basePath the jobs base path (input, output, exec) relative to the system's rootDir
     * @param pathName the file pathname relative to the basePath
     * @return the tapis url indicating a path on the exec system.
     */
    public String makeSystemUrl(String systemId, String basePath, String pathName)
    {
        // Start with the system id.
        String url = TapisUrl.TAPIS_PROTOCOL_PREFIX + systemId;

        // Add the job's put input path.
        if (basePath.startsWith("/")) url += basePath;
        else url += "/" + basePath;

        // Add the suffix.
        if (StringUtils.isBlank(pathName)) return url;
        if (url.endsWith("/") && pathName.startsWith("/")) url += pathName.substring(1);
        else if (!url.endsWith("/") && !pathName.startsWith("/")) url += "/" + pathName;
        else url += pathName;
        return url;
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageAppZipFile:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Perform or restart the app assets file staging process. Both recoverable
     * and non-recoverable exceptions can be thrown from here.
     *
     * @throws TapisException on error
     */
    private void stageAppZipFile(ReqTransfer reqTransfer) throws TapisException
    {
        // Determine if we are restarting a previous request.
        var transferInfo = _jobCtx.getJobsDao().getTransferInfo(_job.getUuid());
        String transferId = transferInfo.stageAppTransactionId;
        String corrId     = transferInfo.stageAppCorrelationId;
        // See if the transfer id has been set for this job (this implies the
        // correlation id has also been set).  If so, then the job had already
        // submitted its transfer request, and we are now in recovery processing.
        // There's no need to resubmit the transfer request in this case.
        //
        // It's possible that the corrId was set, but we died before the transferId
        // was saved.  In this case, we simply generate a new corrId.
        if (StringUtils.isBlank(transferId)) {
            corrId = UUID.randomUUID().toString();
            transferId = submitTransferTask(reqTransfer, corrId, JobTransferPhase.STAGE_APP);
        }

        // Debugging.
        _log.info(MsgUtils.getMsg("JOBS_FILE_TRANSFER_INFO", _job.getUuid(),
                _job.getStatus().name(), transferId, corrId));

        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, corrId);
    }

    /* ---------------------------------------------------------------------- */
    /* getDirectoryKey:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Create a hash key for a system/directory combination.
     * 
     * @param systemId the system id
     * @param directory the directory path
     * @return a string to use as a hash key to identify the system/directory
     */
    private String getDirectoryKey(String systemId, String directory)
    {
        return systemId + "|" + directory;
    }
    
    /* ---------------------------------------------------------------------- */
    /* stageNewInputs:                                                        */
    /* ---------------------------------------------------------------------- */
    private String stageNewInputs(String tag, boolean useDtn) throws TapisException
    {
        // -------------------- Assign Transfer Tasks --------------------
        // Get the job input objects.
        var fileInputs = _job.getFileInputsSpec();
        if (fileInputs.isEmpty()) return NO_FILE_INPUTS;
        
        // Create the list of elements to send to files.
        var tasks = new ReqTransfer();
        
        // Assign each input task.
        for (var fileInput : fileInputs) {
            // Skip files that are already in-place. 
            if (fileInput.getSourceUrl().startsWith(TapisLocalUrl.TAPISLOCAL_PROTOCOL_PREFIX))
                continue;
            
            // The source is always actual source system whether or not a dtn 
            // is being used.  The destination, however, changes depending on 
            // whether a dtn is used.  This requires us to adjust the destination
            // sharing flag accordingly.
            var shareDest = useDtn ? _shareDtnSystemInputDirAppOwner : fileInput.getDestSharedAppCtx();
            
            // Assign the task.  Input files have already been assigned their
            // sharing attributes during submission.  For details, see
            // SubmitContext.calculateDirectorySharing().
            var task = new ReqTransferElement().
                            sourceURI(fileInput.getSourceUrl()).
                            destinationURI(makeStagingInputsDestUrl(fileInput, useDtn));
            task.setOptional(fileInput.isOptional());
            task.setSrcSharedCtx(fileInput.getSrcSharedAppCtx());
            task.setDestSharedCtx(shareDest);
            tasks.addElementsItem(task);
        }
        
        // Return the transfer id.
        if (tasks.getElements().isEmpty()) return NO_FILE_INPUTS;
        return submitTransferTask(tasks, tag, JobTransferPhase.INPUT);
    }
    
    /* ---------------------------------------------------------------------- */
    /* archiveNewOutputs:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Build the list of archival artifacts (files and directories) and submit 
     * it to Files in a transfer request.  The list is built with sourceURIs
     * that indicate the actual locations from which the artifacts will be read
     * when transferred to the archival system.  Under normal circumstances, this
     * means sourceURIs will refer to the execution system's execSystemOutputDir
     * for application generated output.  
     * 
     * When a dtn is being used, however, application generated output sourceURIs 
     * will refer instead to the dtn system's dtnSystemOutputDir.  A separate 
     * transfer will move (using Linux mv) the artifacts from the execution system's 
     * execSystemOutputDir to a locally mounted directory that is accessible to 
     * the dtn.  This local move takes place before the final transfer to the 
     * archive system.
     * 
     * Launch files, if archived, are always transfered directly from the 
     * execution system's execSystemExecDir whether or not a dtn is specified.
     * 
     * @param tag the Jobs generated correlation id
     * @return the Files generated transfer id
     * @throws TapisException
     * @throws TapisClientException
     */
    private String archiveNewOutputs(String tag) 
     throws TapisException, TapisClientException
    {
        // -------------------- Assess Work ------------------------------
        // Get the archive filter spec in canonical form.
        var parmSet = _job.getParameterSetModel();
        var archiveFilter = parmSet.getArchiveFilter();
        if (archiveFilter == null) archiveFilter = new IncludeExcludeFilter();
        archiveFilter.initAll();
        var includes = archiveFilter.getIncludes();
        var excludes = archiveFilter.getExcludes();
        
        // Determine if the archive directory is the same as the output
        // directory on the same system.  If so, we won't apply either of
        // the two filters.
        boolean archiveSameAsOutput = _job.isArchiveSameAsOutput();
        
        // See if there's any work to do at all.
        if (archiveSameAsOutput && !archiveFilter.getIncludeLaunchFiles()) 
            return NO_FILE_INPUTS;
        
        // -------------------- Assign Transfer Tasks --------------------
        // Assign the dtn usage flag.
        final var useDtn = _jobCtx.useDtnOutput();
        
        // Create the list of elements to send to files on the non local
        // move transfer (the only transfer when using a dtn, the 2nd
        // transfer when a dtn is being used).
        var tasks = new ReqTransfer();
        
        // Add the tapis generated files to the task. 
        // This list will never be null.
        List<String> mvLaunchFileList = Collections.emptyList(); // r/o
        if (archiveFilter.getIncludeLaunchFiles()) mvLaunchFileList = addLaunchFiles(tasks, useDtn);
        final int launchTaskCount = tasks.getElements().size();
        
        // DTN local move transfers need a file list derived from File info
        // objects.  This list only contains files from the execSystemOutputDir; 
        // it complements the launchFileList and will always be non-null by the 
        // time the local move method is called.
        List<String> mvOutputFileList = null;
        
        // There's nothing to do if the archive and output directories are 
        // the same or if we have to exclude all output files.  Note that 
        // the sourceURI and source share context are a function of whether
        // or not a dtn is being used.
        //
        // This block schedules the filtered contents of the execSystemOutputDir
        // to be transfered.
        if (!archiveSameAsOutput && !matchesAll(excludes)) {
            // Will any filtering be necessary at all?
            if (excludes.isEmpty() && (includes.isEmpty() || matchesAll(includes))) 
            {
                // We only need to specify the whole output directory subtree 
            	// to archive all files.  The element contains placeholders.
                var task = new ReqTransferElement().
                               sourceURI(makePlaceholderUrl("")).
                               destinationURI(makePlaceholderUrl(""));
                tasks.addElementsItem(task);
                
                // Create a file list for the move operation.
                mvOutputFileList = new ArrayList<>(1); 
                mvOutputFileList.add("");        
            } 
            else 
            {
            	// We need to filter each and every file, so we need to retrieve 
            	// the output directory file listing.  Get the client from the 
            	// context now to catch errors early.  We initialize the unfiltered list.
            	FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
            	var listSubtree = new FilesListSubtree(filesClient, _job.getExecSystemId(), 
            			                               _job.getExecSystemOutputDir());
            	listSubtree.setSharedAppCtx(_shareExecSystemOutputDirAppOwner);
            	var fileInfoList = listSubtree.list(); // Replace empty r/o list
                
            	// Apply the excludes list first since it has precedence, then
            	// the includes list.  The fileList can be modified in both calls.
            	applyArchiveFilters(excludes, fileInfoList, FilterType.EXCLUDES);
            	applyArchiveFilters(includes, fileInfoList, FilterType.INCLUDES);
            
            	// Size the list of names relative to the execSystemOutputDir
            	// to be what's left after filtering when using a DTN.  Otherwise,
            	// leave it as null so that it won't get populated.
            	if (useDtn && fileInfoList.size() > 0) {
            		mvOutputFileList = new ArrayList<>(fileInfoList.size());
            	}
             
            	// Create a task entry for each of the filtered output files.
            	addOutputFiles(tasks, fileInfoList, mvOutputFileList);
            }
        }
        
        // It's possible to get here and have no files to archive.  
        if (tasks.getElements().isEmpty()) return NO_FILE_INPUTS;  // early exit
        
        // DTN pre-processing first issues a local move transfer for
        // job output and launch files to the dtn.
        if (useDtn) {
        	if (mvOutputFileList == null) mvOutputFileList = Collections.emptyList(); // r/o
        	moveDtnOutputs(mvOutputFileList, mvLaunchFileList);
        }
        
        // Complete all task definitions including substituting for
        // placeholders and assigning shared context values.
        completeArchiveTransferTasks(tasks, useDtn, launchTaskCount);
        
        // Return a transfer id if tasks is not empty.
        return submitTransferTask(tasks, tag, JobTransferPhase.ARCHIVE);
    }
    
    /* ---------------------------------------------------------------------- */
    /* moveNewDtnInputs:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Submit a new transfer task that moves all the content of the DTN input
     * directory mounted on the execution system to the execSystemInputDir also
     * on the execution system.
     * 
     * The transfer type is set to LOCAL_MOVE to indicate to Files that this
     * transfer is really an os move from the mounted DTN directory to the 
     * execSystemInputDir.  Previously, all job inputs were transferred to the
     * DTN input directory (the directory mounted on the execution system).
     * 
     * If the execSystemInputDir has been shared with the user, we assume the
     * mounted DTN directory has also been shared with the user.  This is a
     * safe assumption because the user had to have permission to write to the 
     * input directory on the DTN, so they already have access to the data.
     * 
     * @param tag - the correlation id assigned by Jobs
     * @return the transfer id assigned by Files
     * @throws TapisException
     */
    private String moveNewDtnInputs(String tag) throws TapisException
    {
    	// Assign the source and destination paths for a whole directory move operation.
    	var dtnInputDir  = makeSystemUrl(_job.getExecSystemId(), _job.getDtnSystemInputDir(), "");
    	var execInputDir = makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemExecDir(), "");
    	
    	// Define the single element for this transfer task.
        var task = new ReqTransferElement().
                       sourceURI(dtnInputDir).
                       destinationURI(execInputDir);
        task.setTransferType(TransferTypeEnum.SERVICE_MOVE_DIRECTORY_CONTENTS);
        task.setOptional(false);
        task.setSrcSharedCtx(_shareExecSystemInputDirAppOwner);
        task.setDestSharedCtx(_shareExecSystemInputDirAppOwner);
        
        // Create the list of elements to send to files.
        var tasks = new ReqTransfer();
        tasks.addElementsItem(task);
        
        // Return the transfer id.
        return submitTransferTask(tasks, tag, JobTransferPhase.DTN_IN);
    }    
    
    /* ---------------------------------------------------------------------- */
    /* moveNewDtnOutputs:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Submit a new transfer task that moves all the filtered content of the 
     * execution system's execSystemOutputDir or execSystemExecDir directories
     * to the DTN's dtnSystemOutputDir that is mounted on the execution system.
     * 
     * The transfer type is set to LOCAL_MOVE to indicate to Files that this
     * transfer is really an OS move to the mounted DTN directory from the 
     * execSystemOutputDir and--for launch files--the execSystemExecDir.  
     * This local move transfer sets up for a second transfer from the DTN 
     * to the actual archive system.
     * 
     * @param outputFileList - list of fileInfo objects comprised of execSystemOutputDir paths
     * @param launchFileList - list of launch file names
     * @param tag - the correlation id assigned by Jobs
     * @return the transfer id assigned by Files
     * @throws TapisException
     */    
    private String moveNewDtnOutputs(List<String> outputFileList, List<String> launchFileList, 
    		                         String tag)
     throws TapisException
    {
    	// Create a new request for the move operations. 
    	var tasks = new ReqTransfer();
    	
    	// Are there launch files that need to be gathered?
    	if (!launchFileList.isEmpty()) {
    		final boolean isLaunchFile = true;
    		for (var path : launchFileList) {
        		// Create a url for the file in the execSystemExecDir.
        		var srcUrl = makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemExecDir(), 
        				                   path);
    			
        		// Create a url for the file in the mounted dtn directory.
        		var tgtUrl = makeSystemUrl(_job.getExecSystemId(), _job.getDtnSystemOutputDir(),
		                                   path);
         		
        		// Accumulate task elements.
        		tasks.addElementsItem(getMoveDtnOutputElement(srcUrl, tgtUrl, isLaunchFile));
    		}
    	}
    	
    	// Generate a move task for each archive task. Since we are moving
    	// to a locally mounted directory, we allow the user's sharing privileges
    	// associated with the execSystemOutputDir to apply to the dtn directory.
    	// Only files from the execSystemOutputDir should be in the list.
    	final boolean isLaunchFile = false;
    	for (var path : outputFileList) {
    		// Create a url for the file in the execSystemOutputDir.
    		var srcUrl = makeSystemUrl(_job.getExecSystemId(), _job.getExecSystemOutputDir(), 
    				                   path);
    		
    		// Create a url for the file in the mounted dtn directory.  Note that the dtn
    		// and exec systems have the same root directory.  The exec system mounts the
    		// dtn's output directory under its root.  The result is that the dtn output
    		// directory has the SAME absolute path on both systems. 
    		var tgtUrl = makeSystemUrl(_job.getExecSystemId(), _job.getDtnSystemOutputDir(),
    								   path);
     		
    		// Accumulate task elements.
    		tasks.addElementsItem(getMoveDtnOutputElement(srcUrl, tgtUrl, isLaunchFile));
    	}
        
        // Return the transfer id.
        return submitTransferTask(tasks, tag, JobTransferPhase.DTN_OUT);
    }

    /* ---------------------------------------------------------------------- */
    /* getMoveDtnOutputElement:                                               */
    /* ---------------------------------------------------------------------- */
    /** Populate the transfer task element during DTN local move archiving.
     * 
     * If the execSystemOutputDir has been shared with the user, we expect that 
     * the mounted DTN directory has also been shared with the user.
     * 
     * @param srcUrl exec system source path
     * @param tgtUrl dtn system target path
     * @param isLaunchFile source path determinant
     * @return the complete transfer element
     */
    private ReqTransferElement getMoveDtnOutputElement(String srcUrl, String tgtUrl,
    		                                           boolean isLaunchFile)
    {
    	// Launch files reside in the execSystemExecDir, all other archivable 
    	// files reside in the execSystemOutputDir.
    	String srcSharedCtx;
    	if (isLaunchFile) srcSharedCtx = _shareExecSystemExecDirAppOwner;
    	  else srcSharedCtx = _shareExecSystemOutputDirAppOwner;
    	
		// Create a local move task for each file. The target is always in 
    	// the mounted dtn directory.
		var moveTask = new ReqTransferElement();
		moveTask.setSourceURI(srcUrl);
		moveTask.setDestinationURI(tgtUrl);
		moveTask.setOptional(false);
		moveTask.setTransferType(TransferTypeEnum.SERVICE_MOVE_FILE_OR_DIRECTORY);
		moveTask.setSrcSharedCtx(srcSharedCtx);
		moveTask.setDestSharedCtx(_shareDtnSystemOutputDirAppOwner);
		return moveTask;
    }
    
    /* ---------------------------------------------------------------------- */
    /* submitTransferTask:                                                    */
    /* ---------------------------------------------------------------------- */
    private String submitTransferTask(ReqTransfer tasks, String tag,
                                      JobTransferPhase phase)
     throws TapisException
    {
        // Note that failures can occur between the two database calls leaving
        // the job record with the correlation id set but not the transfer id.
        // On recovery, a new correlation id will be issued.
        
        // Get the client from the context now to catch errors early.
        FilesClient filesClient = _jobCtx.getServiceClient(FilesClient.class);
        
        // Database assignment keys.
        TransferValueType tid;
        TransferValueType corrId;
        if (phase == JobTransferPhase.INPUT) {
            tid = TransferValueType.InputTransferId;
            corrId = TransferValueType.InputCorrelationId;
        } 
        else if (phase == JobTransferPhase.ARCHIVE) {
            tid = TransferValueType.ArchiveTransferId;
            corrId = TransferValueType.ArchiveCorrelationId;
        }
        else if (phase == JobTransferPhase.STAGE_APP) {
            tid = TransferValueType.StageAppTransferId;
            corrId = TransferValueType.StageAppCorrelationId;
        }
        else if (phase == JobTransferPhase.DTN_IN) {
            tid = TransferValueType.DtnInputTransferId;
            corrId = TransferValueType.DtnInputCorrelationId;
        }
        else if (phase == JobTransferPhase.DTN_OUT) {
            tid = TransferValueType.DtnOutputTransferId;
            corrId = TransferValueType.DtnOutputCorrelationId;
        }
        else {
        	// This indicates a code compilation/version error.
        	_job.setCondition(JobConditionCode.JOB_INTERNAL_ERROR);
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "submitTransferTask",
            		                     "JobTransferPhase", phase.name());
            throw new TapisImplException(msg, JobExecutionContext.HTTP_INTERNAL_SERVER_ERROR);
        }
        
        // Record the probabilistically unique tag returned in every event
        // associated with this transfer.
        tasks.setTag(tag);
        
        // Save the tag now to avoid any race conditions involving asynchronous events.
        // The in-memory job is updated with the tag value.
        _jobCtx.getJobsDao().updateTransferValue(_job, tag, corrId);
        
        // Submit the transfer request and get the new transfer id.
        String transferId = createTransferTask(filesClient, tasks);
        
        // Save the transfer id and update the in-memory job with the transfer id.
        _jobCtx.getJobsDao().updateTransferValue(_job, transferId, tid);
        
        // Return the transfer id.
        return transferId;
    }
    
    /* ---------------------------------------------------------------------- */
    /* addLaunchFiles:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Add placeholder task entries to copy the generated tapis launch files 
     * to the archive directory.  The useDtn flag determines whether we save
     * the launch file names for use in a local move operation.
     * 
     * @param tasks the task collection into which new transfer tasks are inserted
     * @param useDtn whether the archiving operation uses a dtn
     * @return the non-null, possibly r/o empty, list of simple launch file names
     */
    private List<String> addLaunchFiles(ReqTransfer tasks, boolean useDtn) 
     throws TapisException
    {
        // There's nothing to do if the exec and archive 
        // directories are same and on the same system.
        if (_job.isArchiveSameAsExec()) Collections.emptyList();
        
    	// Create a list for use only in the dtn case.  
        // ALWAYS use guard before referencing list since
        // in non-dtn cases the list will be read-only.
        List<String> launchFileList;
        if (useDtn) launchFileList = new ArrayList<String>();
          else launchFileList = Collections.emptyList(); // r/o
    	        
        // Assign the placeholder tasks for the generated files.
        // Start with the wrapper script, tapisjob.sh
        var task = new ReqTransferElement().
              sourceURI(makePlaceholderUrl(JobExecutionUtils.JOB_WRAPPER_SCRIPT)).
              destinationURI(makePlaceholderUrl(JobExecutionUtils.JOB_WRAPPER_SCRIPT));
        tasks.addElementsItem(task);
        if (useDtn) launchFileList.add(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
        // Add env file tapisjob.env as needed.
        if (_jobCtx.usesEnvFile()) {
            task = new ReqTransferElement().
              sourceURI(makePlaceholderUrl(JobExecutionUtils.JOB_ENV_FILE)).
              destinationURI(makePlaceholderUrl(JobExecutionUtils.JOB_ENV_FILE));
            tasks.addElementsItem(task);
            if (useDtn) launchFileList.add(JobExecutionUtils.JOB_ENV_FILE);
        }
        
        return launchFileList;
    }

    /* ---------------------------------------------------------------------- */
    /* addOutputFiles:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Add each output file in the fileInfoList to the archive tasks.  This
     * method also adds the relative path names to the outputFileList when that
     * list is non-null.  The outputFileList is only used during dtn move operations.
     * 
     * The tasks object may already be populated with launch files that have
     * placeholders embedded in their paths.  The fileInfoList contains all the
     * files other than launch files that have been includes/excludes filtered.
     * It's possible when the execSystemOutputDir and execSystemExecDir are the
     * same to end up with duplicate launch file entries in tasks.  Though generally
     * not harmful, if Files concurrently copies sets of files it's possible that
     * launch files could get corrupted.  
     * 
     * @param tasks (i/o) the archive tasks
     * @param fileInfoList (input) the filtered list of files from the job output directory
     * @param outputFileList (i/o) the list of relative file names, can be null
     */
    private void addOutputFiles(ReqTransfer tasks, List<FileInfo> fileInfoList,
    		                    List<String> outputFileList) 
     throws TapisException
    {
    	// Use a set to detect duplicates. Populate the dupSet with 
    	// any launch files that might already be in tasks.
    	var dupSet = new HashSet<String>(2*fileInfoList.size()+1);
    	for (var task : tasks.getElements()) dupSet.add(task.getSourceURI());
    	    	
        // Add each output file as a placeholder task element,
    	// skipping those we detect as duplicates.
    	for (var f : fileInfoList) {
    		// Avoid placing duplicate source files in tasks.
    		var relativePath = getOutputRelativePath(f.getPath());
    		var srcURI = makePlaceholderUrl(relativePath);
    		if (!dupSet.add(srcURI)) continue;
    	  
    		// Create and record the task.
    		var task = new ReqTransferElement().
    			  			sourceURI(srcURI).
    			  			destinationURI(makePlaceholderUrl(relativePath));
    		tasks.addElementsItem(task);
    		if (outputFileList != null) outputFileList.add(relativePath);
      }
    }
    
    /* ---------------------------------------------------------------------- */
    /* matchesAll:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Determine if the filter list will match any string.  Only the most 
     * common way of specifying a pattern that matches all strings are tested. 
     * In addition, combinations of filters whose effect would be to match all
     * strings are not considered.  Simplistic as it may be, filters specified
     * in a reasonable, straightforward manner to match all strings are identified.   
     * 
     * @param filters the list of glob or regex filters
     * @return true if list contains a filter that will match all strings, false 
     *              if no single filter will match all strings
     */
    private boolean matchesAll(List<String> filters)
    {
        // Check the most common ways to express all strings using glob.
        if (filters.contains("**/*")) return true;
        
        // Check the common way to express all strings using a regex.
        if (filters.contains("REGEX(.*)")) return true;
        
        // No no-op filters found.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* applyArchiveFilters:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Apply either the includes or excludes list to the file list.  In either
     * case, the file list can be modified by having items deleted.
     * 
     * The filter items can either be in glob or regex format.  Each item is 
     * applied to the path of a file info object.  When a match occurs the 
     * appropriate action is taken based on the filter type being processed.
     * 
     * @param filterList the includes or excludes list as identified by the filterType 
     * @param fileList the file list that may have items deleted
     * @param filterType filter indicator
     */
    private void applyArchiveFilters(List<String> filterList, List<FileInfo> fileList, 
                                     FilterType filterType)
    {
        // Is there any work to do?
        if (filterType == FilterType.EXCLUDES) {
            if (filterList.isEmpty()) return;
        } else 
            if (filterList.isEmpty() || matchesAll(filterList)) return;
        
        // Local cache of compiled regexes.  The keys are the filters
        // exactly as defined by users and the values are the compiled 
        // form of those filters.
        HashMap<String,Pattern> regexes   = new HashMap<>();
        HashMap<String,PathMatcher> globs = new HashMap<>();
        
        // Iterate through the file list.
        final int lastFilterIndex = filterList.size() - 1;
        var fileIt = fileList.listIterator();
        while (fileIt.hasNext()) {
            var fileInfo = fileIt.next();
            var path = getOutputRelativePath(fileInfo.getPath());
            for (int i = 0; i < filterList.size(); i++) {
                // Get the current filter.
                String filter = filterList.get(i);
                
                // Use cached filters to match paths.
                boolean matches = matchFilter(filter, path, globs, regexes);
                
                // Removal depends on matches and the filter type.
                if (filterType == FilterType.EXCLUDES) {
                    if (matches) {fileIt.remove(); break;}
                } else {
                    // Remove item only after all include filters have failed to match.
                    if (matches) break; // keep in list 
                    if (!matches && (i == lastFilterIndex)) fileIt.remove();
                }
            }
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getOutputRelativePath:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Strip the job output directory prefix from the absolute pathname before
     * performing a filter matching operation.
     * 
     * @param absPath the absolute path name of a file rooted in the job output directory 
     * @return the path name relative to the job output directory
     */
    private String getOutputRelativePath(String absPath)
    {
        var prefix = getOutputPathPrefix();
        if (absPath.startsWith(prefix))
            return absPath.substring(prefix.length());
        // Special case if Files strips leading slash from output.
        if (!absPath.startsWith("/") && prefix.startsWith("/") &&
            absPath.startsWith(prefix.substring(1)))
            return absPath.substring(prefix.length()-1);
        return absPath;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getOutputPathPrefix:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Assign the filter ignore prefix value for this job.  This value is the
     * path prefix (with trailing slash) that will be removed from all output
     * file path before filtering is carried out.  Users provide glob or regex
     * pattern that are applied to file paths relative to the job output directory. 
     * 
     * @return the prefix to be removed from all paths before filter matching
     */
    private String getOutputPathPrefix()
    {
        // Assign the filter ignore prefix the job output directory including 
        // a trailing slash.
        if (_filterIgnoreOutputPrefix == null) {
            _filterIgnoreOutputPrefix = _job.getExecSystemOutputDir();
            if (!_filterIgnoreOutputPrefix.endsWith("/")) _filterIgnoreOutputPrefix += "/";
        }
        return _filterIgnoreOutputPrefix;
    }
    
    /* ---------------------------------------------------------------------- */
    /* matchFilter:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Determine if the path matches the filter, which can be either a glob
     * or regex.  In each case, the appropriate cache is consulted and, if
     * necessary, updated so that each filter is only compiled once per call
     * to applyArchiveFilters().
     * 
     * @param filter the glob or regex
     * @param path the path to be matched
     * @param globs the glob cache
     * @param regexes the regex cache
     * @return true if the path matches the filter, false otherwise
     */
    private boolean matchFilter(String filter, String path, 
                                HashMap<String,PathMatcher> globs,
                                HashMap<String,Pattern> regexes)
    {
        // Check the cache for glob and regex filters.
        if (filter.startsWith(REGEX_FILTER_PREFIX)) {
            Pattern p = regexes.get(filter);
            if (p == null) {
                p = Pattern.compile(filter.substring(REGEX_FILTER_PREFIX.length()));
                regexes.put(filter, p);
            }
            var m = p.matcher(path);
            return m.matches();
        } else {
            PathMatcher m = globs.get(filter);
            if (m == null) {
                m = FileSystems.getDefault().getPathMatcher("glob:"+filter);
                globs.put(filter, m);
            }
            var pathObj = Paths.get(path);
            return m.matches(pathObj);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* createTransferTask:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Issue a transfer request to Files and return the transfer id.  An
     * exception is thrown if the new transfer id is not attained.
     * 
     * @param tasks the tasks 
     * @return the new, non-null transfer id generated by Files
     * @throws TapisImplException 
     */
    private String createTransferTask(FilesClient filesClient, ReqTransfer tasks)
     throws TapisException
    {
        // Tracing.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("FILES_TRANSFER_TASK_REQ", printTasks(tasks)));
        
        // Submit the transfer request.
        TransferTask task = null;
        try {task = filesClient.createTransferTask(tasks);} 
        catch (Exception e) {
            // Look for a recoverable error in the exception chain. Recoverable
            // exceptions are those that might indicate a transient network
            // or server error, typically involving loss of connectivity.
            Throwable transferException = 
                TapisUtils.findFirstMatchingException(e, TapisConstants.CONNECTION_EXCEPTION_PREFIX);
            if (transferException != null) {
                throw new TapisServiceConnectionException(transferException.getMessage(), 
                            e, RecoveryUtils.captureServiceConnectionState(
                               filesClient.getBasePath(), TapisConstants.FILES_SERVICE));
            }
            
            // Unrecoverable error.
            _job.setCondition(JobConditionCode.JOB_FILES_SERVICE_ERROR);
            if (e instanceof TapisClientException) {
                var e1 = (TapisClientException) e;
                String msg = MsgUtils.getMsg("JOBS_CREATE_TRANSFER_ERROR", "input", _job.getUuid(),
                                             e1.getCode(), e1.getMessage());
                throw new TapisImplException(msg, e1, e1.getCode());
            } else {
                String msg = MsgUtils.getMsg("JOBS_CREATE_TRANSFER_ERROR", "input", _job.getUuid(),
                                             0, e.getMessage());
                throw new TapisImplException(msg, e, 0);
            }
        }
        
        // Get the transfer id.
        String transferId = null;
        if (task != null) {
            var uuid = task.getUuid();
            if (uuid != null) transferId = uuid.toString();
        }
        if (transferId == null) {
        	_job.setCondition(JobConditionCode.JOB_FILES_SERVICE_ERROR);
            String msg = MsgUtils.getMsg("JOBS_NO_TRANSFER_ID", "input", _job.getUuid());
            throw new JobException(msg);
        }
        
        return transferId;
    }
    
    /* ---------------------------------------------------------------------- */
    /* moveDtnInputs:                                                         */
    /* ---------------------------------------------------------------------- */
    /** This method must only be called when _jobCtx.useDtnInput() == true. 
     * 
     * When a DTN input directory is being used, issue a local move from the 
     * mounted DTN input directory to the job's execution system's exec directory 
     * after the initial transfer to the DTN.
     * 
     * The move is performed using an asynchronous Files transfer. After submitting
     * the transfer, this method monitors the transfer by polling the Files service.
     */
    private void moveDtnInputs() throws TapisException
    {
    	// This should never happen, but just to be sure.
    	if (!_jobCtx.useDtnInput()) {
    		_job.setCondition(JobConditionCode.JOB_INTERNAL_ERROR);
    		var cond = TapisImplException.Condition.INTERNAL_SERVER_ERROR;
            String msg = MsgUtils.getMsg("JOBS_CREATE_TRANSFER_ERROR", "moveInput", _job.getUuid(),
                                         500, cond.name());
            throw new TapisImplException(msg, cond);
    	}
    	
    	// Issue the move as an asynchronous transfer.
        // Determine if we are restarting a previous staging request.
        var transferInfo = _jobCtx.getJobsDao().getTransferInfo(_job.getUuid());
        String transferId = transferInfo.dtnInputTransactionId;
        String corrId     = transferInfo.dtnInputCorrelationId;
        
        // See if the transfer id has been set for this job (this implies the
        // correlation id has also been set).  If so, then the job had already 
        // submitted its transfer request and we are now in recovery processing.  
        // There's no need to resubmit the transfer request in this case.  
        // 
        // It's possible that the corrId was set but we died before the transferId
        // was saved.  In this case, we simply generate a new corrId and resubmit.
        if (StringUtils.isBlank(transferId)) {
            corrId = UUID.randomUUID().toString();
            transferId = moveNewDtnInputs(corrId);
        }
    	
        // Debugging.
        _log.info(MsgUtils.getMsg("JOBS_FILE_TRANSFER_INFO", _job.getUuid(), 
                                  _job.getStatus().name(), transferId, corrId));
        
    	// Monitor the move's completion.
        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, corrId);
    }

    /* ---------------------------------------------------------------------- */
    /* moveDtnOutputs:                                                         */
    /* ---------------------------------------------------------------------- */
    /** When a DTN output directory is being used, issue a local move from the 
     * job's output directory to the DTN output directory before issuing the
     * transfer from the DTN to the actual archive directory.
     * 
     * @param outputFileList non-null list of execSystemOutputDir files to be moved
     * @param launchFileList non-null list of launch files to be moved
     * @throws TapisException 
     */
    private void moveDtnOutputs(List<String> outputFileList, List<String> launchFileList) 
     throws TapisException
    {
    	// This should never happen, but just to be sure.
    	if (!_jobCtx.useDtnOutput()) {
    		_job.setCondition(JobConditionCode.JOB_INTERNAL_ERROR);
        	var cond = TapisImplException.Condition.INTERNAL_SERVER_ERROR;
            String msg = MsgUtils.getMsg("JOBS_CREATE_TRANSFER_ERROR", "moveOutput", _job.getUuid(),
                                         500, cond.name());
            throw new TapisImplException(msg, cond);
    	}
    	
    	// Maybe there's nothing to do.
    	if (outputFileList.isEmpty() && launchFileList.isEmpty()) return;
    	
    	// Issue the move as an asynchronous local transfer.
        // Determine if we are restarting a previous archiving request.
        var transferInfo = _jobCtx.getJobsDao().getTransferInfo(_job.getUuid());
        String transferId = transferInfo.dtnOutputTransactionId;
        String corrId     = transferInfo.dtnOutputCorrelationId;
        
        // See if the transfer id has been set for this job (this implies the
        // correlation id has also been set).  If so, then the job had already 
        // submitted its transfer request and we are now in recovery processing.  
        // There's no need to resubmit the transfer request in this case.  
        // 
        // It's possible that the corrId was set but we died before the transferId
        // was saved.  In this case, we simply generate a new corrId and resubmit.
        if (StringUtils.isBlank(transferId)) {
            corrId = UUID.randomUUID().toString();
            transferId = moveNewDtnOutputs(outputFileList, launchFileList, corrId);
        }
        
        // Debugging.
        _log.info(MsgUtils.getMsg("JOBS_FILE_TRANSFER_INFO", _job.getUuid(), 
                                  _job.getStatus().name(), transferId, corrId));
        
    	// Monitor the move's completion.
        // Block until the transfer is complete. If the transfer fails because of
        // a communication, api or transfer problem, an exception is thrown from here.
        var monitor = TransferMonitorFactory.getMonitor();
        monitor.monitorTransfer(_job, transferId, corrId);
    }
    
    /* ---------------------------------------------------------------------- */
    /* completeArchiveTransferTasks:                                          */
    /* ---------------------------------------------------------------------- */
    /** Substitute values for the placeholders in the tasks elements list. The
     * values are adjusted depending on whether or not we are using a dtn.  When
     * a dtn is being used, we are preparing for the 2nd, non-local, archive transfer.
     * When a dtn is not being used, this is the only archive transfer.
     * 
     * @param tasks - a transfer object with placeholders  
     * @param useDtn - whether we are using a dtn
     * @param launchTaskCount the number of launch files to be transfered 
     */
    private void completeArchiveTransferTasks(ReqTransfer tasks, boolean useDtn, 
    		                                  int launchTaskCount)
    {
    	// Get the elements to be transfered.
    	var elements = tasks.getElements();
    	if (elements.isEmpty()) return;
    	
    	// -------------- Assign source/destination values -------------- 
    	// The final destination is always the archive system on the second
    	// transfer (i.e., the transfer after the local move transfer).
    	//
    	// NOTE: Paths are relative to the root directory, so they don't need 
    	// a leading slash. The same stripping of leading slash happens below.
	    final String dstSysId    = _job.getArchiveSystemId();
		final String dstPath     = StringUtils.stripStart(_job.getArchiveSystemDir(), "/");
		final String dstShareCtx = _shareArchiveSystemDirAppOwner;
		
		// -------------- Complete each transfer task -------------------
    	// ---- Complete the launch file transfers.
    	int i = 0;
    	if (launchTaskCount > 0) {
    		
    		// File/directory source information.
    	    final String srcSysId;
    		final String srcPath;
    		final String srcShareCtx;

    		// Values are assigned based on whether this is a remote
    		// transfer that uses a DTN or not. 
    		if (useDtn ) {
    			srcSysId    = _job.getDtnSystemId();
    			srcPath     = StringUtils.stripStart(_job.getDtnSystemOutputDir(), "/");
    			srcShareCtx = _shareDtnSystemOutputDirAppOwner;
    		} else {
    			srcSysId    = _job.getExecSystemId();
    			srcPath     = StringUtils.stripStart(_job.getExecSystemExecDir(), "/");
    			srcShareCtx = _shareExecSystemExecDirAppOwner;
    		}
    		
    		// Update each launch task's fields so that the task is complete.
    		for (; i < launchTaskCount; i++) {
    			var task = elements.get(i);
    			
    			var srcUri = task.getSourceURI().replace(SYSTEM_PLACEHOLER, srcSysId);
    			srcUri = srcUri.replace(PATH_PLACEHOLER, srcPath);
    			task.setSourceURI(srcUri);
    			task.setSrcSharedCtx(srcShareCtx);
    			
    			var dstUri = task.getDestinationURI().replace(SYSTEM_PLACEHOLER, dstSysId);
    			dstUri = dstUri.replace(PATH_PLACEHOLER, dstPath);
    			task.setDestinationURI(dstUri);
    			task.setDestSharedCtx(dstShareCtx);
    		}
    	}
    	
    	// ---- Complete the non-launch file transfers.
    	if (i < elements.size()) {
    		
    		// File/directory source information.
    	    final String srcSysId;
    		final String srcPath;
    		final String srcShareCtx;

    		// Values are assigned based on whether this is a remote
    		// transfer that uses a DTN or not.
    		if (useDtn ) {
    			srcSysId    = _job.getDtnSystemId();
    			srcPath     = StringUtils.stripStart(_job.getDtnSystemOutputDir(), "/");
    			srcShareCtx = _shareDtnSystemOutputDirAppOwner;
    		} else {
    			srcSysId    = _job.getExecSystemId();
    			srcPath     = StringUtils.stripStart(_job.getExecSystemOutputDir(), "/");
    			srcShareCtx = _shareExecSystemOutputDirAppOwner;
    		}
    		
    		// Update each launch task's fields so that the task is complete.
    		for (; i < elements.size(); i++) {
    			var task = elements.get(i);
			
    			var srcUri = task.getSourceURI().replace(SYSTEM_PLACEHOLER, srcSysId);
    			srcUri = srcUri.replace(PATH_PLACEHOLER, srcPath);
    			task.setSourceURI(srcUri);
    			task.setSrcSharedCtx(srcShareCtx);
			
    			var dstUri = task.getDestinationURI().replace(SYSTEM_PLACEHOLER, dstSysId);
    			dstUri = dstUri.replace(PATH_PLACEHOLER, dstPath);
    			task.setDestinationURI(dstUri);
    			task.setDestSharedCtx(dstShareCtx);
    		}
    	}
    }

    /* ---------------------------------------------------------------------- */
    /* makePlaceholderUrl:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Used during archiving to allow for both dtn and non-dtn late binding.
     *  
     * @param pathName the pathname relative to an output or exec directory
     * @return the templated url
     */
    private String makePlaceholderUrl(String pathName)
    {
    	return makeSystemUrl(SYSTEM_PLACEHOLER, PATH_PLACEHOLER, pathName);
    }

    /* ---------------------------------------------------------------------- */
    /* makeStagingInputsDestUrl:                                              */
    /* ---------------------------------------------------------------------- */
    /** Create a tapis url based on the input spec's destination path and either
     * DTN or execution system information.  
     * 
     * Implicit in the tapis protocol is that the Files service will prefix the 
     * path portion of the url with the system's rootDir when actually transferring 
     * files. 
     * 
     * The target is never null or empty.
     * 
     * @param fileInput a file input spec
     * @param useDtn whether a dtn is being used on an input transfer
     * @return the tapis url indicating a path on the exec system.
     */
    private String makeStagingInputsDestUrl(JobFileInput fileInput, boolean useDtn)
    {
        // If a DTN is involved use it for the destination instead of the exec system.
    	String destSysId, destInputDir;
    	if (useDtn) {
    		destSysId = _job.getDtnSystemId();
    		destInputDir = _job.getDtnSystemInputDir();
    	} else {
    		destSysId = _job.getExecSystemId();
    		destInputDir = _job.getExecSystemInputDir();
    	}
        return makeSystemUrl(destSysId, destInputDir, fileInput.getTargetPath());
    }
    
    /* ---------------------------------------------------------------------- */
    /* makePath:                                                              */
    /* ---------------------------------------------------------------------- */
    /** Make a path from components with proper treatment of slashes.
     * 
     * @param first non-null start of path
     * @param more 0 or more additional segments
     * @return the path as a string
     */
    private String makePath(String first, String... more)
    {
        return Paths.get(first, more).toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* printTasks:                                                            */
    /* ---------------------------------------------------------------------- */
    private String printTasks(ReqTransfer tasks)
    {
        var buf = new StringBuilder(1024);
        buf.append("Requesting TransferTask with tag ");
        buf.append(tasks.getTag());
        buf.append(" and ");
        buf.append(tasks.getElements().size());
        buf.append(" elements:");
        for (var element : tasks.getElements()) {
            buf.append("\n  src: ");
            buf.append(element.getSourceURI());
            buf.append(", dst: ");
            buf.append(element.getDestinationURI());
            buf.append(", transferType=");
            buf.append(element.getTransferType() == null ? "null" : element.getTransferType().name());
            buf.append(", optional=");
            buf.append(element.getOptional());
            buf.append(", srcSharedCtx=");
            buf.append(element.getSrcSharedCtx());
            buf.append(", dstSharedCtx=");
            buf.append(element.getDestSharedCtx());
        }
        return buf.toString();
    }
}
