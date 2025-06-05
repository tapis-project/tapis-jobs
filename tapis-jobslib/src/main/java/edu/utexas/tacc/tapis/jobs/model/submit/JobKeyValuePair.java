package edu.utexas.tacc.tapis.jobs.model.submit;

/*
 * Class for KeyValuePair in a Job submit request
 */
public class JobKeyValuePair
{
  private String  key;
  private String  value;
  private String  description;
  private Boolean include;
  private Object  notes;

  public String getKey(){ return key; }
  public void setKey(String key) { this.key = key; }

  public String getValue() { return value; }
  public void setValue(String value) { this.value = value; }

  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }

  public Boolean getInclude() { return include; }
  public void setInclude(Boolean include) { this.include = include; }

  public Object getNotes() { return notes; }
  public void setNotes(Object notes) { this.notes = notes; }
}