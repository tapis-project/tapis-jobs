package edu.utexas.tacc.tapis.jobs.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.notifications.client.NotificationsClient;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryMethod;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryTarget;
import edu.utexas.tacc.tapis.notifications.client.gen.model.ReqPostSubscription;
import edu.utexas.tacc.tapis.notifications.client.gen.model.TapisSubscription;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

public final class NotificationLiveness 
 implements Thread.UncaughtExceptionHandler
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(NotificationLiveness.class);
    
    // Thread names.
    private static final String THREADGROUP_NAME    = "NotifLiveGroup";
    private static final String SENDER_THREAD_NAME  = "NotifLiveSenderThread";
    private static final String CHECKER_THREAD_NAME = "NotifLiveCheckerThread";
    
    // Subscription information.
    private static final String SUBSCRIPTION_OWNER   = "JobsService";
    private static final String FAKE_JOBID = "00000000-0000-0000-0000-000000000000-007";
    private static final String SUBJECT_FILTER = FAKE_JOBID;
    private static final String SUBSCRIPTION_DETAIL = "Liveness";
    private static final String EVENT_MSG = "Jobs notification liveness test.";
    
    // Time periods in milliseconds.
    private static final long   SEND_EVENT_WAIT_MILLIS  = 180 * 1000;
    private static final long   CHECK_EVENT_WAIT_MILLIS = 100 * 1000;
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Get our site.
    private final String _site;
    private final String _siteAdminTenant;
    private final String _subscriptionName;
    
    // Threads are started on construction.
    private final SenderThread  _senderThread;
    private final CheckerThread _checkerThread;
    
    // Initialize the event count that gets 
    // incremented every time we send an event.
    private int _eventnum = 0;
    
    // Shutdown flag consulted on thread interrupt.
    private boolean _shutdown;
    
    /* ********************************************************************** */
    /*                       SingletonInitializer class                       */
    /* ********************************************************************** */
    /** Bill Pugh method of singleton initialization. */
    private static final class SingletonInitializer
    {
        private static final NotificationLiveness _instance = new NotificationLiveness(); 
    }
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Start the liveness checking thread. */
    private NotificationLiveness()
    {
    	// Assign site and tenant information.
    	_site = RuntimeParameters.getInstance().getSiteId(); 
    	_siteAdminTenant = TenantManager.getInstance().getSiteAdminTenantId(_site);
    	
    	// Create the subscription name after site/tenant assignments.
    	_subscriptionName = getSubscriptionName();
    	
    	// Start the worker threads.
    	var threadGroup = new ThreadGroup(THREADGROUP_NAME);
    	_senderThread   = startSenderThread(threadGroup);
    	_checkerThread  = startCheckerThread(threadGroup);
    	
    	// Announce ourselves.
    	_log.info(MsgUtils.getMsg("JOBS_EVENT_LIVENESS_STARTED", SEND_EVENT_WAIT_MILLIS/1000));
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Force the construction of the NotificationLiveness instance. */
	public static NotificationLiveness getInstance() 
	{return SingletonInitializer._instance;}
	
    /* ---------------------------------------------------------------------- */
    /* shutdown:                                                              */
    /* ---------------------------------------------------------------------- */
	public void shutdown()
	{
		_shutdown = true;
		_senderThread.interrupt();
		_checkerThread.interrupt();
	}
	
    /* ---------------------------------------------------------------------- */
    /* uncaughtException:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Note the unexpected death of our refresh thread.  We just let it die
     * after logging our final message.
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) 
    {
        // Record the error.
        _log.error(MsgUtils.getMsg("TAPIS_THREAD_UNCAUGHT_EXCEPTION", 
                                   t.getName(), e.toString()));
        e.printStackTrace(); // stderr for emphasis
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* startSenderThread:                                                     */
    /* ---------------------------------------------------------------------- */
    private SenderThread startSenderThread(ThreadGroup group)
    {
    	var t = new SenderThread(group);
		t.setDaemon(true);
		t.setUncaughtExceptionHandler(this);
		t.start();
		return t;
    }
    
    /* ---------------------------------------------------------------------- */
    /* startCheckerThread:                                                    */
    /* ---------------------------------------------------------------------- */
    private CheckerThread startCheckerThread(ThreadGroup group)
    {
    	var t = new CheckerThread(group);
		t.setDaemon(true);
		t.setUncaughtExceptionHandler(this);
		t.start();
		return t;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSubscriptionName:                                                   */
    /* ---------------------------------------------------------------------- */
    private String getSubscriptionName()
    {
    	return "Jobs_liveness_for_" + _siteAdminTenant + "_at_" + _site;
    }
    
    /* ********************************************************************** */
    /*                           class SenderThread                           */
    /* ********************************************************************** */
    private final class SenderThread 
     extends Thread
    {
    	// Constructor
    	private SenderThread(ThreadGroup group){super(group, SENDER_THREAD_NAME);}
    	
        /* ---------------------------------------------------------------------- */
        /* run:                                                                   */
        /* ---------------------------------------------------------------------- */
    	@Override
    	public void run()
    	{
    		// Create the liveness subscription if it doesn't already exist.
    		checkSubscription();
    		
    		// Periodically send a liveness event.
            while (true) {
            	// Wait the configured number of milliseconds between event sends.
            	if (_shutdown) return;
                try {Thread.sleep(SEND_EVENT_WAIT_MILLIS);} 
                catch (InterruptedException e) {
                    if (_log.isDebugEnabled()) {
                        String msg = MsgUtils.getMsg("JOBS_MONITOR_INTERRUPTED", FAKE_JOBID, 
                                                     getClass().getSimpleName());
                        _log.debug(msg);
                    }
                    if (_shutdown) return;
                }
            	
                // Create the user payload json.
            	var eventData = JobEventData.getInternalUserEventData(FAKE_JOBID, _subscriptionName,
        		          SUBSCRIPTION_OWNER, EVENT_MSG, ++_eventnum);

            	// Send the event.
            	JobEventManager.getInstance().sendInternalUserEvent(FAKE_JOBID, _siteAdminTenant, 
            		          SUBSCRIPTION_OWNER, eventData, SUBSCRIPTION_DETAIL);
            }
    	}
    	
        /* ---------------------------------------------------------------------- */
        /* checkSubscription:                                                     */
        /* ---------------------------------------------------------------------- */
        private void checkSubscription() throws TapisRuntimeException
        {
        	// Get a notification client. We don't explicitly close 
        	// the client since it's cached and could be used again. 
        	NotificationsClient client;
            try {client = JobUtils.getNotificationsClient(_siteAdminTenant);} 
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Notifications",
                                             _siteAdminTenant, TapisConstants.SERVICE_NAME_JOBS);
                _log.error(msg, e);
                throw new TapisRuntimeException(msg, e);
            }
            
            // See if our subscription has already been created.
            TapisSubscription sub;
            try {
    			sub = client.getSubscriptionByName(_subscriptionName, SUBSCRIPTION_OWNER);
    		} catch (TapisClientException e) {
                String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "Notifications",
                                             _siteAdminTenant, TapisConstants.SERVICE_NAME_JOBS);
                _log.error(msg, e);
                throw new TapisRuntimeException(msg, e);
    		}
     
            // Already subscribed?
            if (sub != null) return;
            
            // Create the subscription input.
            var subreq = new ReqPostSubscription();
            subreq.setDescription("Jobs liveness event subscription.");
            subreq.setEnabled(true);
            subreq.setName(_subscriptionName);
            subreq.setOwner(SUBSCRIPTION_OWNER);
            subreq.setTtlMinutes(0);  // never expire
            subreq.setSubjectFilter(SUBJECT_FILTER);
            subreq.setTypeFilter(JobUtils.makeNotifTypeToken(
            		              JobEventType.JOB_USER_EVENT, SUBSCRIPTION_DETAIL));
            
            // Set our webhook address.
            DeliveryTarget target = new DeliveryTarget();
            target.setDeliveryMethod(DeliveryMethod.WEBHOOK);
            target.setDeliveryAddress(getDeliveryAddress());
            subreq.setDeliveryTargets(Lists.asList(target, null));
            
            // Create the subscription.
            try {client.postSubscription(subreq);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", FAKE_JOBID,
                                 SUBSCRIPTION_OWNER, _siteAdminTenant, e.getMessage());
                _log.error(msg, e);
                throw new TapisRuntimeException(msg, e);
            }
        }
        
        /* ---------------------------------------------------------------------- */
        /* getDeliveryAddress:                                                    */
        /* ---------------------------------------------------------------------- */
        private String getDeliveryAddress() throws TapisRuntimeException
        {
            // Get the admin tenant's base url; exceptions have already been logged.
            Tenant adminTenant;
            try {adminTenant = TenantManager.getInstance().getTenant(_siteAdminTenant);}
    		catch (TapisRuntimeException e) {throw e;}
            catch (Exception e) {
            	throw new TapisRuntimeException(e.getMessage(), e);
            }
            
            // The tenant will not be null.
            String baseUrl = adminTenant.getBaseUrl();
            if (baseUrl == null) {
            	// This should never happen.
            	throw new TapisRuntimeException(MsgUtils.getMsg("JOBS_TENANT_NO_BASE_URL", adminTenant, 
            			                        FAKE_JOBID, "Invalid tenant definition."));
            }
        	
            // Assign our webhook address, the receiving Jobs endpoint for liveness notifications.
            if (baseUrl.endsWith("/")) baseUrl = baseUrl.substring(0, baseUrl.length()-1); 
            return baseUrl + "/v3/jobs/eventLiveness";
        }
        
    }
    
    /* ********************************************************************** */
    /*                         class CheckerThread                            */
    /* ********************************************************************** */
    private final class CheckerThread 
     extends Thread
    {
    	// Constructor
    	private CheckerThread(ThreadGroup group){super(group, CHECKER_THREAD_NAME);}
    	
        /* ---------------------------------------------------------------------- */
        /* run:                                                                   */
        /* ---------------------------------------------------------------------- */
    	@Override
    	public void run()
    	{
    		// Periodically check when the last liveness event arrived.
    		
    	}
    }
    
}
