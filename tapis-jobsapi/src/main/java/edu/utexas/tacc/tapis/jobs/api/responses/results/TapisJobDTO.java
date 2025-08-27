package edu.utexas.tacc.tapis.jobs.api.responses.results;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobConditionCode;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static edu.utexas.tacc.tapis.jobs.api.resources.JobGetResource.SUMMARY_ATTRS;
import static edu.utexas.tacc.tapis.jobs.model.Job.*;

/*
  Classes representing a TapisJob result to be returned
 */
public final class TapisJobDTO
{
  private static final Gson gson = TapisGsonUtils.getGson();

  public String sharedAppCtx;
  public boolean isPublic;
  public Set<String> sharedWithUsers;

  public String uuid;
  public String name;
  public String description;
  public String owner;
  public String tenant;
  public String appId;
  public String appVersion;
  public JobType jobType;
  public JobStatusType status;
  public JobConditionCode condition;
  public String lastMessage;
  public Instant created;
  public Instant ended;
  public Instant lastUpdated;
  public String execSystemId;
  public String execSystemExecDir;
  public String execSystemInputDir;
  public String execSystemOutputDir;
  public String execSystemLogicalQueue;
  public String archiveSystemId;
  public String archiveSystemDir;
  public boolean archiveOnAppError;
  public String dtnSystemId;
  public String dtnSystemInputDir;
  public String dtnSystemOutputDir;
  public int nodeCount;
  public int coresPerNode;
  public int memoryMB;
  public int maxMinutes;
  public boolean isMpi;
  public String mpiCmd;
  public String cmdPrefix; // 32

  public boolean enabled;
  public boolean versionEnabled;
  public boolean locked;
  public List<RuntimeOption> runtimeOptions;
  public String containerImage;
  public int maxJobs;
  public int maxJobsPerUser;
  public boolean strictFileInputs;
  public JobAttributes jobAttributes;
  public String[] tags;
  public JsonObject notes;

  public TapisJobDTO(Job tj)
  {
    uuid = tj.getUuid();
    name = tj.getName();
    description = tj.getDescription();
    owner = tj.getOwner();
    tenant = tj.getTenant();
    appId = tj.getAppId();
    appVersion = tj.getAppVersion();
    jobType = tj.getJobType();
    status = tj.getStatus();
    condition = tj.getCondition();
    lastMessage = tj.getLastMessage();
    created = tj.getCreated();
    ended = tj.getEnded();
    lastUpdated = tj.getLastUpdated();
    execSystemId = tj.getExecSystemId();
    execSystemExecDir = tj.getExecSystemExecDir();
    execSystemInputDir = tj.getExecSystemInputDir();
    execSystemOutputDir = tj.getExecSystemOutputDir();
    execSystemLogicalQueue = tj.getExecSystemLogicalQueue();
    archiveSystemId = tj.getArchiveSystemId();
    archiveSystemDir = tj.getArchiveSystemDir();
    archiveOnAppError = tj.isArchiveOnAppError();
    dtnSystemId = tj.getDtnSystemId();
    dtnSystemInputDir = tj.getDtnSystemInputDir();
    dtnSystemOutputDir = tj.getDtnSystemOutputDir();
    nodeCount = tj.getNodeCount();
    coresPerNode = tj.getCoresPerNode();
    memoryMB = tj.getMemoryMB();
    maxMinutes = tj.getMaxMinutes();
    isMpi = tj.isMpi();
    mpiCmd = tj.getMpiCmd();
    cmdPrefix = tj.getCmdPrefix(); // 32

    runtimeOptions = tj.getRuntimeOptions();
    containerImage = tj.getContainerImage();
    jobType = tj.getJobType();
    maxJobs = tj.getMaxJobs();
    maxJobsPerUser = tj.getMaxJobsPerUser();
    strictFileInputs = tj.isStrictFileInputs();
    jobAttributes = new JobAttributes(tj);
    tags = tj.getTags();
    notes = tj.getNotes();
    // Check for -1 in max values and return Integer.MAX_VALUE instead.
    //   As requested by Jobs service.
    if (maxJobs < 0) maxJobs = Integer.MAX_VALUE;
    if (maxJobsPerUser < 0) maxJobsPerUser = Integer.MAX_VALUE;
    sharedAppCtx = tj.getSharedAppCtx();
    isPublic = tj.isPublic();
    sharedWithUsers = tj.getSharedWithUsers();
  }

  /**
   * Create a JsonObject containing the id attribute and any attribute in the selectSet that matches the name
   * of a public field in this class
   * If selectSet is null or empty then all attributes are included.
   * If selectSet contains "allAttributes" then all attributes are included regardless of other items in set
   * If selectSet contains "summaryAttributes" then summary attributes are included regardless of other items in set
   * @return JsonObject containing attributes in the select list.
   */
  public JsonObject getDisplayObject(List<String> selectList)
  {
    // Check for special case of returning all attributes
    if (selectList == null || selectList.isEmpty() || selectList.contains(SEL_ALL_ATTRS))
    {
      return allAttrs();
    }

    var retObj = new JsonObject();

    // If summaryAttrs included then add them
    if (selectList.contains(SEL_SUMMARY_ATTRS)) addSummaryAttrs(retObj);

    // Include specified list of attributes
    // If ID not in list we add it anyway.
    if (!selectList.contains(ID_FIELD)) addDisplayField(retObj, ID_FIELD);
    for (String attrName : selectList)
    {
      addDisplayField(retObj, attrName);
    }
    return retObj;
  }

  // Build a JsonObject with all displayable attributes
  private JsonObject allAttrs()
  {
    String jsonStr = gson.toJson(this);
    return gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
  }

  // Add summary attributes to a json object
  private void addSummaryAttrs(JsonObject jsonObject)
  {
    for (String attrName: SUMMARY_ATTRS)
    {
      addDisplayField(jsonObject, attrName);
    }
  }

  /**
   * Add specified attribute name to the JsonObject that is to be returned as the displayable object.
   * If attribute does not exist in this class then it is a no-op.
   *
   * @param jsonObject Base JsonObject that will be returned.
   * @param attrName Attribute name to add to the JsonObject
   */
  private void addDisplayField(JsonObject jsonObject, String attrName)
  {
    String jsonStr;
    switch (attrName) {
      case UUID_FIELD -> jsonObject.addProperty(UUID_FIELD, uuid.toString());
      case NAME_FIELD -> jsonObject.addProperty(NAME_FIELD, name);
      case DESCRIPTION_FIELD ->jsonObject.addProperty(DESCRIPTION_FIELD, description);
      case OWNER_FIELD -> jsonObject.addProperty(OWNER_FIELD, owner);
      case TENANT_FIELD -> jsonObject.addProperty(TENANT_FIELD, tenant);
      case APP_ID_FIELD -> jsonObject.addProperty(APP_ID_FIELD, appId);
      case APP_VERSION_FIELD -> jsonObject.addProperty(APP_VERSION_FIELD, appVersion);
      case STATUS_FIELD -> jsonObject.addProperty(STATUS_FIELD, status.name());
      case CONDITION_FIELD -> jsonObject.addProperty(CONDITION_FIELD, condition.name());
      case LAST_MESSAGE_FIELD -> jsonObject.addProperty(LAST_MESSAGE_FIELD, lastMessage);
      case JOB_TYPE_FIELD -> jsonObject.addProperty(JOB_TYPE_FIELD, jobType.name());
      case CREATED_FIELD -> jsonObject.addProperty(CREATED_FIELD, created.toString());
      case ENDED_FIELD -> jsonObject.addProperty(ENDED_FIELD, ended.toString());
      case UPDATED_FIELD -> jsonObject.addProperty(UPDATED_FIELD, lastUpdated.toString());
      case EXECSYSID_FIELD -> jsonObject.addProperty(EXECSYSID_FIELD, execSystemId);
      case EXECSYSEXECDIR_FIELD -> jsonObject.addProperty(EXECSYSEXECDIR_FIELD, execSystemExecDir);
      case EXECSYSINDIR_FIELD -> jsonObject.addProperty(EXECSYSINDIR_FIELD, execSystemInputDir);
      case EXECSYSOUTDIR_FIELD -> jsonObject.addProperty(EXECSYSOUTDIR_FIELD, execSystemOutputDir);
      case EXECSYSLOGICALQ_FIELD -> jsonObject.addProperty(EXECSYSLOGICALQ_FIELD, execSystemLogicalQueue);
      case ARCHIVESYSID_FIELD -> jsonObject.addProperty(ARCHIVESYSID_FIELD, archiveSystemId);
      case ARCHIVESYSDIR_FIELD -> jsonObject.addProperty(ARCHIVESYSDIR_FIELD, archiveSystemDir);
      case ARCHIVEONAPPERROR_FIELD -> jsonObject.addProperty(ARCHIVEONAPPERROR_FIELD, archiveOnAppError);
      case DTNSYSID_FIELD -> jsonObject.addProperty(DTNSYSID_FIELD, dtnSystemId);
      case DTNSYSINDIR_FIELD -> jsonObject.addProperty(DTNSYSINDIR_FIELD, dtnSystemInputDir);
      case DTNSYSOUTDIR_FIELD -> jsonObject.addProperty(DTNSYSOUTDIR_FIELD, dtnSystemOutputDir);
      case NODE_COUNT_FIELD -> jsonObject.addProperty(NODE_COUNT_FIELD, nodeCount);
      case CORES_PER_NODE_FIELD -> jsonObject.addProperty(CORES_PER_NODE_FIELD, coresPerNode);
      case MEMORY_MB_FIELD -> jsonObject.addProperty(MEMORY_MB_FIELD, memoryMB);
      case MAX_MINUTES_FIELD -> jsonObject.addProperty(MAX_MINUTES_FIELD, maxMinutes);
      case ISMPI_FIELD -> jsonObject.addProperty(ISMPI_FIELD, isMpi);
      case MPI_CMD_FIELD -> jsonObject.addProperty(MPI_CMD_FIELD, mpiCmd);
      case CMD_PREFIX_FIELD -> jsonObject.addProperty(CMD_PREFIX_FIELD, cmdPrefix); // 32


      case ENABLED_FIELD -> jsonObject.addProperty(ENABLED_FIELD, Boolean.toString(enabled));
      case RUNTIMEOPTS_FIELD -> jsonObject.add(RUNTIMEOPTS_FIELD, gson.toJsonTree(runtimeOptions));
      case CONTAINERIMG_FIELD -> jsonObject.addProperty(CONTAINERIMG_FIELD, containerImage);
      case MAX_JOBS_FIELD -> jsonObject.addProperty(MAX_JOBS_FIELD, maxJobs);
      case MAX_JOBS_PER_USER_FIELD -> jsonObject.addProperty(MAX_JOBS_PER_USER_FIELD, maxJobsPerUser);
      case STRICT_FILE_INPUTS_FIELD -> jsonObject.addProperty(STRICT_FILE_INPUTS_FIELD, String.valueOf(strictFileInputs));
      case JOB_ATTRS_FIELD -> {
        jsonStr = gson.toJson(jobAttributes);
        jsonObject.add(JOB_ATTRS_FIELD, gson.fromJson(jsonStr, JsonObject.class));
      }
      case TAGS_FIELD -> jsonObject.add(TAGS_FIELD, gson.toJsonTree(tags));
      case NOTES_FIELD -> jsonObject.add(NOTES_FIELD, notes);
      case DELETED_FIELD -> jsonObject.addProperty(DELETED_FIELD, Boolean.toString(deleted));
      case SHARED_APP_CTX_FIELD -> jsonObject.addProperty(SHARED_APP_CTX_FIELD, sharedAppCtx);
      case IS_PUBLIC_FIELD -> jsonObject.addProperty(IS_PUBLIC_FIELD, Boolean.toString(isPublic));
      case SHARED_WITH_USERS_FIELD -> jsonObject.add(SHARED_WITH_USERS_FIELD, gson.toJsonTree(sharedWithUsers));
    }
  }
}
