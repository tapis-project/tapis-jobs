package edu.utexas.tacc.tapis.jobs.model.dto;

public class JobStatusDisplay {
	private String status;
	private String condition;
	
	public JobStatusDisplay() {}
	
	public void setStatus(String status){
		this.status=status;
	}
	public String getStatus() {
		return status;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
}
