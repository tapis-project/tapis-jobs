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
	NORMAL_COMPLETION("Job completed normally"),
	SCHEDULER_TIMEOUT("Batch scheduler timed out job"),
	SCHEDULER_TERMINATION("Batch scheduler terminated job"),
	TAPIS_EXECUTION_TIMEOUT("Tapis execution time expired"),
	TAPIS_LAUNCH_FAILURE("Tapis unable to launch job"),
	TAPIS_RECOVERY_TIMEOUT("Tapis recovery time expired");
	
    // ---- Fields
	private final String _description;
	
	// ---- Constructor
	JobConditionCode(String description){_description = description;}
	
	// ---- Instance Methods
	public String getDescription(){return _description;}
}
