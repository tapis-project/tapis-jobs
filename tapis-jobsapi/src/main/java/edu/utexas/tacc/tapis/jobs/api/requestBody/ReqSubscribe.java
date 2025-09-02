package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventCategoryFilter;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.NotifDeliveryTarget;

/*
 * Notification Subscription request in the context of a Job submission
 * Contains filter and list of notification delivery targets
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 */
public class ReqSubscribe
 implements IReqBody
{
    // Default values
    public static final Boolean DEFAULT_ENABLED = true;
    // Default ttl is 1 week and the maximum is 4 weeks.
    public static final Integer DEFAULT_TTL_MINUTES = 10080; // 60*24*7
    public static final Integer MAX_TTL_MINUTES = DEFAULT_TTL_MINUTES * 4;
    
    // Informational values.
    private final String  description;
    private final Boolean enabled;
    
    // Search and delivery values.
    private final Integer                   ttlMinutes;
    private final JobEventCategoryFilter    eventCategoryFilter;
    private final List<NotifDeliveryTarget> deliveryTargets;
    
    // Constructors.
    // Default constructor to set defaults. This appears to be needed for when object is created from json using gson.
    public ReqSubscribe()
    {
        description = null;
        enabled = DEFAULT_ENABLED;
        eventCategoryFilter = null;
        deliveryTargets = new ArrayList<>();
        ttlMinutes = DEFAULT_TTL_MINUTES;
    }

    public ReqSubscribe(edu.utexas.tacc.tapis.apps.client.gen.model.ReqSubscribe appReq)
     throws JobException
    {
        // Marshal the app request to a job request object.  
        // Exceptions can be thrown during some conversions.
        try {
            // Description
            description = appReq.getDescription();
            // Enabled
            if (appReq.getEnabled() != null)
                enabled = appReq.getEnabled();
            else
                enabled = DEFAULT_ENABLED;
            // TTL minutes
            Integer appTtlMinutes = appReq.getTtlMinutes();
            if (appTtlMinutes == null || appTtlMinutes < 1) ttlMinutes = DEFAULT_TTL_MINUTES;
            else if (appTtlMinutes > MAX_TTL_MINUTES) ttlMinutes = MAX_TTL_MINUTES;
            else ttlMinutes = appTtlMinutes;

            // EventCategoryFilter
            var appFilter = appReq.getJobEventCategoryFilter();
            if (appFilter != null)
                eventCategoryFilter = JobEventCategoryFilter.valueOf(appFilter.name());
            else
                eventCategoryFilter = null;
            // DeliveryTargets
            deliveryTargets = new ArrayList<NotifDeliveryTarget>();
            if (appReq.getDeliveryTargets() != null)
            {
                for (var appTarget : appReq.getDeliveryTargets())
                {
                    var target = new NotifDeliveryTarget(appTarget);
                    deliveryTargets.add(target);
                }
            }
        } catch (Exception e) {
            var msg = JobUtils.getMsg("JOBS_INITIALIZATION_ERROR", e.getMessage());
            throw new JobException(msg);
        }
        
        // Now validate this object's contents and assign ttl default if needed.
        String msg = validate();
        if (msg != null) throw new JobException(msg);
    }
    
    @Override
    public String validate() {
        // Even though the schema defines these fields as required, we doublecheck
        // here to support code paths that don't apply the schema.
        if (eventCategoryFilter == null) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "eventCategoryFilter");
        if (deliveryTargets == null || deliveryTargets.isEmpty())
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "deliveryTargets");
        
        // Check each delivery target.
        for (var target : deliveryTargets) {
            if (StringUtils.isBlank(target.getDeliveryAddress()))
                return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "deliveryTarget.deliveryAddress");
            if (target.getDeliveryMethod() == null)
                return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "deliveryTarget.deliveryMethod");
        }
        // Success.
        return null; 
    }
    
    // Accessors.
    public String getDescription() { return description; }
    public Boolean getEnabled() { return enabled; }
    public Integer getTTLMinutes() { return ttlMinutes; }
    public JobEventCategoryFilter getEventCategoryFilter() { return eventCategoryFilter; }
    public List<NotifDeliveryTarget> getDeliveryTargets() { return deliveryTargets; }
}
