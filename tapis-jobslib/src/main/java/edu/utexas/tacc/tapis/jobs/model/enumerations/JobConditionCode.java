package edu.utexas.tacc.tapis.jobs.model.enumerations;

/** Terminal condition codes are set whenever a job reaches a terminal status.
 * Before that, the condition is expected to be null in the Job object and in
 * the database.  When adding new conditions, check the length limit in the 
 * database.
 * 
 * @author rcardone
 */
public enum JobConditionCode 
{
	CANCELLED_BY_USER("Job cancelled by user"),
	JOB_ARCHIVING_FAILED("Job error while archiving outputs"),
	JOB_DATABASE_ERROR("Jobs is unable to access its database"),
	JOB_EXECUTION_MONITORING_ERROR("An error occurred during application monitoring"),
	JOB_EXECUTION_MONITORING_TIMEOUT("Tapis execution monitoring time expired"),
	JOB_FILES_SERVICE_ERROR("An error involving the File service occurred"),
	JOB_INTERNAL_ERROR("Jobs service internal error"),
	JOB_INVALID_DEFINITION("Invalid job record"),
	JOB_LAUNCH_FAILURE("Tapis unable to launch job"),
	JOB_QUEUE_MONITORING_ERROR("An error occurred during application queue monitoring"),
	JOB_RECOVERY_FAILURE("Tapis unable to recover job"),
	JOB_RECOVERY_TIMEOUT("Tapis recovery time expired"),
	JOB_REMOTE_ACCESS_ERROR("Jobs could not access a resource on a remote system"),
	JOB_REMOTE_OUTCOME_ERROR("User application returned a non-zero exit code"),
	JOB_UNABLE_TO_STAGE_INPUTS("Unable to stage application input files"),
	JOB_UNABLE_TO_STAGE_JOB("Unable to stage application assets"),
	JOB_TRANSFER_FAILED_OR_CANCELLED("A file transfer failed or was cancelled"),
	JOB_TRANSFER_MONITORING_TIMEOUT("Jobs transfer monitoring expired"),
	NORMAL_COMPLETION("Job completed normally"),
	SCHEDULER_CANCELLED("Batch scheduler cancelled job"),
	SCHEDULER_DEADLINE("Batch scheduler deadline exceeded"),
	SCHEDULER_OUT_OF_MEMORY("Batch scheduler out of memory error"),
	SCHEDULER_STOPPED("Batch scheduler stopped job"),
	SCHEDULER_TIMEOUT("Batch scheduler timed out job"),
	SCHEDULER_TERMINATED("Batch scheduler terminated job");
	
    // ---- Fields
	private final String _description;
	
	// ---- Constructor
	JobConditionCode(String description){_description = description;}
	
	// ---- Instance Methods
	public String getDescription(){return _description;}
}
