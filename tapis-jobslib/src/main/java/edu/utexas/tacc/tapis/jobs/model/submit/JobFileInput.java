package edu.utexas.tacc.tapis.jobs.model.submit;

import edu.utexas.tacc.tapis.apps.client.gen.model.AppFileInput;
import edu.utexas.tacc.tapis.apps.client.gen.model.FileInputModeEnum;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.uri.TapisLocalUrl;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.regex.Pattern;

public class JobFileInput 
{
    // Regex pattern for validating sourceUrl.
    // Copied from Files service. edu.utexas.tacc.tapis.files.lib.models.TransferURI
    public static final Pattern URL_PATTERN = Pattern.compile("(http:\\/\\/|https:\\/\\/|tapis:\\/\\/)([\\w -\\.]+)\\/?(.*)");

    private String  name;
    private String  description;
    private String  envKey;
    private Boolean autoMountLocal;
    private String  sourceUrl;
    private String  targetPath;
    private Object  notes;
    
    @Schema(hidden = true)
    private boolean optional = false;
    @Schema(hidden = true)
    private String srcSharedAppCtx = Job.DEFAULT_SHARED_APP_CTX;
    @Schema(hidden = true)
    private String destSharedAppCtx = Job.DEFAULT_SHARED_APP_CTX;
    
    // Import an app input into a request input.
    public static JobFileInput importAppInput(AppFileInput appInput)
    {
        var reqInput = new JobFileInput();
        reqInput.setName(appInput.getName());
        reqInput.setDescription(appInput.getDescription());

        reqInput.setAutoMountLocal(appInput.getAutoMountLocal());
        
        reqInput.setSourceUrl(appInput.getSourceUrl());
        reqInput.setTargetPath(appInput.getTargetPath());
        
        reqInput.setNotes(appInput.getNotes());
        
        reqInput.setEnvKey(appInput.getEnvKey());
        
        // The default input mode is optional.
        if (appInput.getInputMode() == null ||
            appInput.getInputMode() == FileInputModeEnum.OPTIONAL)
            reqInput.setOptional(true);
        return reqInput;
    }
    
    @Schema(hidden = true)
    public boolean isTapisLocal() {
        if (sourceUrl == null) return false;
        if (sourceUrl.startsWith(TapisLocalUrl.TAPISLOCAL_PROTOCOL_PREFIX)) return true;
        return false;
    }
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
	public String getEnvKey() {
		return envKey;
	}
	public void setEnvKey(String envKey) {
		this.envKey = envKey;
	}
    public Boolean getAutoMountLocal() {
        return autoMountLocal;
    }
    public void setAutoMountLocal(Boolean autoMountLocal) {
        this.autoMountLocal = autoMountLocal;
    }
    public String getSourceUrl() {
        return sourceUrl;
    }
    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }
    public String getTargetPath() {
        return targetPath;
    }
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }
    @Schema(hidden = true)
    public boolean isOptional() {
        return optional;
    }
    @Schema(hidden = true)
    public void setOptional(boolean optional) {
        this.optional = optional;
    }
    @Schema(hidden = true)
    public String getSrcSharedAppCtx() {
        return srcSharedAppCtx;
    }
    @Schema(hidden = true)
    public void setSrcSharedAppCtx(String srcSharedAppCtx) {
        this.srcSharedAppCtx = srcSharedAppCtx;
    }
    @Schema(hidden = true)
    public String getDestSharedAppCtx() {
        return destSharedAppCtx;
    }
    @Schema(hidden = true)
    public void setDestSharedAppCtx(String destSharedAppCtx) {
        this.destSharedAppCtx = destSharedAppCtx;
    }
	public Object getNotes() {
		return notes;
	}
	public void setNotes(Object notes) {
		this.notes = notes;
	}

	public static Pattern getUrlPattern() {
		return URL_PATTERN;
	}
}
