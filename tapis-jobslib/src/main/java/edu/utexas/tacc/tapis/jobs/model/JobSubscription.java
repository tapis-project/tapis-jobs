package edu.utexas.tacc.tapis.jobs.model;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.shared.model.NotifDeliveryTarget;

/*
 * Notification subscription associated with a job instance.
 * See also class edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe
 * NOTE: The class ReqSubscribe should probably be replaced with this one.
 *       One reason: The lib class can be used from the api but not vice versa.
 */
public class JobSubscription 
{
    // Basic identity fields.
    private String  description;
    private boolean enabled;
    
    // Search and delivery values.
    private int     ttlMinutes;
    private String  typeFilter;
    private String  subjectFilter;
    private List<NotifDeliveryTarget> deliveryTargets = new ArrayList<>();
    
    // Constructors.
    public JobSubscription() {}
    
    // Accessors
    public String getDescription() { return description; }
    public void setDescription(String s) {description = s; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean b) { enabled = b; }
    public int getTtlMinutes() { return ttlMinutes; }
    public void setTtlMinutes(int i) { ttlMinutes = i; }
    public String getTypeFilter() { return typeFilter; }
    public void setTypeFilter(String s) { typeFilter = s; }
    public String getSubjectFilter() { return subjectFilter; }
    public void setSubjectFilter(String s) { this.subjectFilter = s; }
    public List<NotifDeliveryTarget> getDeliveryTargets() { return deliveryTargets; }
    public void setDeliveryTargets(List<NotifDeliveryTarget> targets) { deliveryTargets = targets; }
}
