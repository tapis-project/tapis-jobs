package edu.utexas.tacc.tapis.jobs.events;

import java.time.Instant;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.events.JobEventData.JobEventLivenessData;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.notifications.client.NotificationsClient;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryMethod;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryTarget;
import edu.utexas.tacc.tapis.notifications.client.gen.model.ReqPostSubscription;
import edu.utexas.tacc.tapis.notifications.client.gen.model.TapisSubscription;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientFactory;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.utils.HTMLizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

/** This class implements liveness checking between Jobs and Notifications.  The
 * goal is to detect when events sent by Jobs are for any reason not causing the
 * expected notifications to be sent by the Notifications service.
 * 
 * This class is a key component in a liveness facility that includes the /eventLiveness
 * endpoint implemented in the GeneralResource of tapis-jobsapi.  The two threads 
 * managed by this class, the SenderThread and the CheckThread, send events and 
 * process the expected notifications, respectively.  These threads are started 
 * as part of JobApplication initialization at which time the SenderThread will
 * create a non-expiring notification subscription if it does not already exist.
 * 
 * If the checker thread does not receive an expected notification within a certain 
 * time period, an error is logged  and an email is sent to the support address.  
 * Output throttling avoids log and email flooding. 
 * 
 * When a problem is detected a condition code indicates the specific problem. 
 * The fix will usually involve the Notifications service, the Jobs EventReader
 * or this class itself.
 * 
 * @author rcardone
 */
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
    private static final String SUBSCRIPTION_OWNER  = TapisConstants.SERVICE_NAME_JOBS;
    private static final String FAKE_JOBID = "00000000-0000-0000-0000-000000000000-007";
    private static final String SUBJECT_FILTER = FAKE_JOBID;
    private static final String SUBSCRIPTION_DETAIL = "EventLiveness";
    private static final String EVENT_MSG = "Jobs notification liveness test.";
    
    // Number of check cycles before sending alert email and logging error.
    private static final int    QUIET_CHECKING_CYCLE = 60; // quiet modulus
    
    // Time periods in milliseconds.  Making the check wait time half that
    // of the send directs the checking thread to wake up twice as often as
    // the thread that sends events.  Combining this ratio with the maximum
    // number of failed checks of 5 means that two missed notifications
    // will be detected as a failure.  The staleness threshold is what's 
    // actually used to compare the current time with the timestamp on the 
    // last notification received.  
    private static final long   GET_SUBCRIPTION_RETRY_MILLIS =  30 * 1000;
    private static final long   SEND_EVENT_WAIT_MILLIS       = 180 * 1000;
    private static final long   CHECK_EVENT_WAIT_MILLIS      =  90 * 1000;
    private static final int    MAX_FAILED_CHECKS            = 5;
    private static final long   STALENESS_THRESHOLD_MILLIS = 
    		                       MAX_FAILED_CHECKS * CHECK_EVENT_WAIT_MILLIS;

    /* ********************************************************************** */
    /*                                Enums                                   */
    /* ********************************************************************** */
    // Conditions that cause alerts to be sent.
    private enum AlertCondition {
    	NO_NOTIFICATIONS_EVER_RECEIVED,
    	NO_RECENT_NOTIFICATION_RECEIVED,
    	INVALID_NOTIFICATION_RECEIVED,
    	NO_EVENTS_SENT
    }
    
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
    
    // Shutdown flag consulted on thread interrupt.
    private boolean _shutdown;
    
    // Initialize the event count that gets incremented 
    // every time we send an event.  This is a monotonically 
    // increasing value that must always be accessed by the 
    // CheckThread in a synchronized block.  The value is 
    // only updated by the SenderThread in an atomic operation
    // that also update the last sent timestamp.  The SenderThread 
    // can read this value without locking it since it's the only
    // thread that writes it.
    private int _eventnum = 0;
    
    // Incoming event data access and outgoing timestapm
    // must be limited to 1 thread at a time.  Use only 
    // synchronized accessors.
    private JobEventLivenessData _lastEventRecv;
    private Instant              _lastSentEventTS;
    
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
    /* recordLivenessData:                                                    */
    /* ---------------------------------------------------------------------- */
	/** The parse the payload that's made up of the original event data that
	 * the SenderThread sent.  Assign the last received event data field.
	 * 
	 * @param jsonStr the data member of the notification's event field
	 * @throws JobException missing or invalid data
	 */
    public void recordLivenessData(String jsonStr)
      throws JobException
    {
    	// Parse the notification's payload containing the event data we sent.
    	var livenessData = getEventLivenessData(jsonStr);
    	setLastEventRecv(livenessData);
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
    /* Accessors:                                                             */
    /* ---------------------------------------------------------------------- */
    private synchronized int getEventNum() {return _eventnum;}
    private synchronized JobEventLivenessData getLastEventRecv() {return _lastEventRecv;}
    private synchronized Instant getLastSentEventTS() {return _lastSentEventTS;}

    /* ---------------------------------------------------------------------- */
    /* setLastSentEventInfo:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Increment the eventnum and set the last outgoing event's timestamp in
     * one atomic operation.
     * 
     * @param ts last event's timestamp
     */
    private synchronized void setLastSentEventInfo(Instant ts) 
    {_eventnum++; _lastSentEventTS = ts;}
    
    /* ---------------------------------------------------------------------- */
    /* setLastEventRecv:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Only set the last event if it was actually created after the currently
     * assigned one.  We insulate ourselves from indeterminate ordering that
     * due to asynchronous processing in Notification and our front end.  
     * 
     * @param d newly received event
     */
    private synchronized void setLastEventRecv(JobEventLivenessData d)
    {
    	// Only replace existing data objects with newer ones.
    	if (_lastEventRecv == null || _lastEventRecv.eventnum < d.eventnum)
    	   _lastEventRecv = d;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getEventLivenessData:                                                  */
    /* ---------------------------------------------------------------------- */
    private JobEventLivenessData getEventLivenessData(String jsonStr)
      throws JobException
    {
    	// Bad data.
  	  	if (jsonStr == null) {
  	  		String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getEventLivenessData", "jsonStr");
  	  		throw new JobException(msg);
  	  	}
    	
  	  	// The result object.
  	    JobEventLivenessData d = null;
  	  	Gson gson = TapisGsonUtils.getGson();
  	  	try {d = gson.fromJson(jsonStr, JobEventLivenessData.class);}
  	  	catch (Exception e) {
  	  		String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "getEventLivenessData",
                                         jsonStr, e.getMessage());
  	  		_log.error(msg, e);
  	  		throw new JobException(msg, e);
  	  	}

        // Basic checking doesn't stop spoofing but might exposed unexpected behavior.
        if (!FAKE_JOBID.equals(d.jobUuid)) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "getEventLivenessData", 
            		                     "jobUuid", d.jobUuid);
            throw new JobException(msg);
        }
        
        if (!_subscriptionName.equals(d.jobName)) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "getEventLivenessData", 
            						     "jobName", d.jobName);
            throw new JobException(msg);
        }
        
        if (!SUBSCRIPTION_OWNER.equals(d.jobOwner)) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "getEventLivenessData", 
            		                     "jobOwner", d.jobOwner);
            throw new JobException(msg);
        }
        
        if (!EVENT_MSG.equals(d.message)) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "getEventLivenessData", 
            		                     "message", d.message);
            throw new JobException(msg);
        }
        
        if (d.eventnum < 1) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "getEventLivenessData", 
            		                     "eventnum", d.eventnum);
            throw new JobException(msg);
        }
        
        if (d.createtime == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getEventLivenessData", "createtime");
            throw new JobException(msg);
        }
        
        // The fully initialized object.
    	return d;
    }
    
    /* ---------------------------------------------------------------------- */
    /* startSenderThread:                                                     */
    /* ---------------------------------------------------------------------- */
    private SenderThread startSenderThread(ThreadGroup group)
    {
    	var t = new SenderThread(group);
    	t.setName(SENDER_THREAD_NAME);
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
    	t.setName(CHECKER_THREAD_NAME);
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
    	return "Jobs_liveness_subscription_" + _siteAdminTenant + "_at_" + _site;
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
    		// Announce ourselves.
    		if (_log.isInfoEnabled())
    			_log.info(MsgUtils.getMsg("JOBS_WEBAPP_THREAD_STARTED", 
    					                  Thread.currentThread().getName()));
    		
    		// Create the liveness subscription if it doesn't already exist.
    		// We wait forever until Notifications becomes available and we
    		// can successfully retrieve or create our subscription.  The
    		// checker thread will also wait forever if no events have been
    		// sent yet.
    		while (true) {
    			if (_shutdown) return;
    			try {
    				checkSubscription();
    				break;
    			}
    			catch (TapisRuntimeException e) {
    				// These are unrecoverable errors that have already been logged.
    				// Throwing them kills the thread.
    				throw e;
    			}
    			catch (Exception e) {
    				// Retry after waiting a while.
                    try {Thread.sleep(GET_SUBCRIPTION_RETRY_MILLIS);} 
                    catch (InterruptedException e1) {
                        if (_log.isDebugEnabled()) {
                            String msg = MsgUtils.getMsg("JOBS_LIVENESS_INTERRUPTED",  
                                                         getClass().getSimpleName(),
                                                         Thread.currentThread().getName());
                            _log.debug(msg); 
                            // By continuing we expect to terminate.
                        }
                    }
    			} 
    		}
    		
    		// Periodically send a liveness event.
            while (true) {
            	// Wait the configured number of milliseconds between event sends.
            	if (_shutdown) return;
                try {Thread.sleep(SEND_EVENT_WAIT_MILLIS);} 
                catch (InterruptedException e) {
                    if (_log.isDebugEnabled()) {
                        String msg = MsgUtils.getMsg("JOBS_LIVENESS_INTERRUPTED",  
                                                     getClass().getSimpleName(),
                                                     Thread.currentThread().getName());
                        _log.debug(msg);
                    }
                    if (_shutdown) return; // We expect to terminate.
                }
            	
                // Create the user payload json.
                var eventnum = _eventnum + 1; // Direct reads from sender thread are thread-safe. 
                var ts = Instant.now();
            	var eventData = JobEventData.getEventLivenessData(FAKE_JOBID, _subscriptionName,
        		          		SUBSCRIPTION_OWNER, EVENT_MSG, eventnum, ts);

            	// Send the event.  A non-null jobevent is returned on success,
            	// null is returned if the event could not be posted.
            	var jobEvent = JobEventManager.getInstance().sendInternalUserEvent(FAKE_JOBID, 
            			        _siteAdminTenant, SUBSCRIPTION_OWNER, eventData, SUBSCRIPTION_DETAIL);
            	
            	// On success update the actual number of queued events
            	// and the last event's timestamp as one atomic operation.
            	// This also increments the eventnum field.
            	if (jobEvent != null) setLastSentEventInfo(ts);
            }
    	}
    	
        /* ---------------------------------------------------------------------- */
        /* checkSubscription:                                                     */
        /* ---------------------------------------------------------------------- */
    	/** Create or retrieve the long term jobs subscription.
    	 * 
    	 * @throws TapisException errors for which we should retry after a delay
    	 * @throws TapisRuntimeException unrecoverable errors
    	 */
        private void checkSubscription() 
         throws TapisException, TapisRuntimeException
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
            TapisSubscription sub = null;
            try {
    			sub = client.getSubscriptionByName(_subscriptionName, SUBSCRIPTION_OWNER);
    		} catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "Notifications",
                                             "getSubscriptionByName", _siteAdminTenant, 
                                             TapisConstants.SERVICE_NAME_JOBS);
                _log.warn(msg, e);
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
            subreq.setDeliveryTargets(Arrays.asList(target));
            
            // Create the subscription.
            try {client.postSubscription(subreq);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_SUBSCRIPTION_ERROR", FAKE_JOBID,
                                 SUBSCRIPTION_OWNER, _siteAdminTenant, e.getMessage());
                _log.error(msg, e);
                throw new TapisException(msg, e);
            }
            
            // Write the log on success.
            if (_log.isInfoEnabled()) {
            	_log.info(MsgUtils.getMsg("JOBS_LIVENESS_SUBSCRIPTION_CREATED", _subscriptionName));
            }
        }
        
        /* ---------------------------------------------------------------------- */
        /* getDeliveryAddress:                                                    */
        /* ---------------------------------------------------------------------- */
        /** Construct this Jobs service instance's liveness endpoint URL.
         * 
         * @return this Jobs instance's liveness endpoint
    	 * @throws TapisException errors for which we should retry after a delay
         */
        private String getDeliveryAddress() 
         throws TapisException, TapisRuntimeException
        {
            // Get the admin tenant's base url.  Null causes an exception; 
        	// all exceptions have already been logged.
            Tenant adminTenant;
            try {adminTenant = TenantManager.getInstance().getTenant(_siteAdminTenant);}
            catch (Exception e) {
            	var msg = MsgUtils.getMsg("TAPIS_SITE_UNKNOWN_ADMIN_TENANT", _site, _siteAdminTenant);
            	_log.error(msg, e);
            	throw new TapisException(msg, e);
            }
            
            // The tenant will not be null.
            String baseUrl = adminTenant.getBaseUrl();
            if (baseUrl == null) {
            	// This should never happen.
            	var msg = MsgUtils.getMsg("JOBS_TENANT_NO_BASE_URL", adminTenant, FAKE_JOBID, 
            			                  "Invalid tenant definition.");
            	_log.error(msg);
            	throw new TapisRuntimeException(msg);
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
    	// Number of checks that occurred when no events
    	// have been sent.  This usually indicates a problem
    	// with the sender thread or the 
    	private int _noEventsSentChecks;
    	
    	// Number of checks after at least one event was
    	// sent and before any notification was received.
    	// This field can be reset to zero.
    	private int _noLastEventChecks;
    	
    	// Number of failed checks since the last successful 
    	// check or the beginning of operation.  This field 
    	// can be reset to zero.
    	private int _failedCheckSinceLastSuccess;
    	
    	// Constructor
    	private CheckerThread(ThreadGroup group){super(group, CHECKER_THREAD_NAME);}
    	
        /* ---------------------------------------------------------------------- */
        /* run:                                                                   */
        /* ---------------------------------------------------------------------- */
    	/** Wake up after a configured amount of time and check that we've received
    	 * a notification recently.  Recently is defined as the number of milliseconds
    	 * in STALENESS_THRESHOLD_MILLIS.  This threshold allows for a number of 
    	 * events to have been sent and time for the Notifications service to act on
    	 * them.
    	 * 
    	 * This method handles the initial case when no events have been sent by 
    	 * simply waiting until an event is sent.  After at least one event has been
    	 * sent, this method checks that a notification has been received before
    	 * the staleness threshold is reached.
    	 */
    	@Override
    	public void run()
    	{
    		// Announce ourselves.
    		if (_log.isInfoEnabled())
    			_log.info(MsgUtils.getMsg("JOBS_WEBAPP_THREAD_STARTED", 
    					                  Thread.currentThread().getName()));
    		
    		// Periodically check when the last liveness event arrived.
    		while (true) {
            	// Wait the configured number of milliseconds between event verifications.
            	if (_shutdown) return;
                try {Thread.sleep(CHECK_EVENT_WAIT_MILLIS);} 
                catch (InterruptedException e) {
                    if (_log.isDebugEnabled()) {
                        String msg = MsgUtils.getMsg("JOBS_LIVENESS_INTERRUPTED",  
                                                     getClass().getSimpleName(),
                                                     Thread.currentThread().getName());
                        _log.debug(msg);
                    }
                    if (_shutdown) return; // We expect to terminate.
                }
                
                // If we haven't sent any events yet there's nothing
                // read, so we just go back to waiting.
                var eventnum = getEventNum();
    			if (eventnum < 1) {
    				_noEventsSentChecks++;
    				if (_noEventsSentChecks > MAX_FAILED_CHECKS)
    					raiseAlert(null, AlertCondition.NO_EVENTS_SENT);
    				continue;
    			} else _noEventsSentChecks = 0;
    			
    			// Get the last read notification.
    			var lastEvent = getLastEventRecv();
    			if (lastEvent == null) {
    				_noLastEventChecks++;
    				if (_noLastEventChecks > MAX_FAILED_CHECKS)
    					raiseAlert(lastEvent, AlertCondition.NO_NOTIFICATIONS_EVER_RECEIVED);
    				continue;
    			} else _noLastEventChecks = 0; // reset counter after receiving 1st notification
    			
    			// Has time expired before we received a new notification?
    			// Create time must be expressed as UTC in zulu format.
    			Instant lastEventTS = null;
    			try {lastEventTS = Instant.parse(lastEvent.createtime);}
    			catch (Exception e) {
    				// This should never happen.
                    String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER",  
                                           getClass().getSimpleName() + ".run",
                                           "lastEvent.createtime", lastEvent.createtime);
                    _log.error(msg, e);
                    raiseAlert(lastEvent, AlertCondition.INVALID_NOTIFICATION_RECEIVED);
                    continue;
    			}
    			
    			// Have we exceeded the threshold for not receiving a notification?
    			var epochMillis = Instant.now().toEpochMilli();
    			if ((epochMillis - lastEventTS.toEpochMilli()) > STALENESS_THRESHOLD_MILLIS) {
    				raiseAlert(lastEvent, AlertCondition.NO_RECENT_NOTIFICATION_RECEIVED);
    				_failedCheckSinceLastSuccess++;      // increment after alert
    			} else _failedCheckSinceLastSuccess = 0; // reset recent failure count 
    		}
    	}
    	
        /* ---------------------------------------------------------------------- */
        /* raiseAlert:                                                            */
        /* ---------------------------------------------------------------------- */
    	/** Avoid excessive logging and email alerts by ignoring most calls to this
    	 * method.  The number of failed checks since the last time this method was
    	 * called determines whether this method executes.
    	 * 
    	 * When executed, this method creates an error message based on the detected 
    	 * condition, logs the message and then sends an email to alert the operator.
    	 * 
    	 * @param lastEventRecv can be null depending on condition
    	 * @param condition the detected error condition
    	 */
    	private void raiseAlert(JobEventLivenessData lastEventRecv, AlertCondition condition)
    	{
    		// Don't create alot of noise.  Every cycle number of failures causes
    		// logging and an email to be sent.  The amount of time this amounts to
    		// is:  QUIET_CHECKING_CYCLE * CHECK_EVENT_WAIT_MILLIS. The failed count
    		// is the previous number of failures not counting this one.
    		if ((_failedCheckSinceLastSuccess % QUIET_CHECKING_CYCLE) != 0) return;
    		
    		// Create message content based on condition.
    		String emsg = switch (condition) {
    			case INVALID_NOTIFICATION_RECEIVED:
    				yield("Unable to parse timestamp in notification.");
    			case NO_NOTIFICATIONS_EVER_RECEIVED:
    				yield("No notification received after sending " + MAX_FAILED_CHECKS + 
    					  " events and waiting for " + 
    					  (MAX_FAILED_CHECKS * CHECK_EVENT_WAIT_MILLIS / 1000) + " seconds.");
    			case NO_RECENT_NOTIFICATION_RECEIVED:
    				yield("The last notification was received more than " +
    				      (STALENESS_THRESHOLD_MILLIS/1000) + 
    				      " seconds ago. The cooresponding event (#" + lastEventRecv.eventnum +
    				      ") was sent at " + lastEventRecv.createtime + 
    				      " and should have generated a notification by now.");
    			case NO_EVENTS_SENT:
    				yield("No events sent after waiting for " +
      					  (MAX_FAILED_CHECKS * CHECK_EVENT_WAIT_MILLIS / 1000) + " seconds.");
    		};
    		
    		// Add information to the message where possible.
    		var sentTS = getLastSentEventTS();
    		if (sentTS != null) emsg += " The last event was sent at " + sentTS.toString() + ".";
    		
    		// Log the error message and send email.
            String msg = MsgUtils.getMsg("JOBS_MISSING_NOTIFICATION_ERROR", emsg);
    		_log.error(msg);
    		sendLivenessEmail(msg);
    	}
    	
        /* ---------------------------------------------------------------------------- */
        /* sendLivenessEmail:                                                           */
        /* ---------------------------------------------------------------------------- */
        /** Send an email to alert support that a zombie job exists.
         * 
         * @param job the job whose status update failed
         * @param livenessMsg failure message
         */
        private static void sendLivenessEmail(String livenessMsg)
        {
            String subject = "Expected notifications not received by Jobs." ;
            try {
                  RuntimeParameters runtime = RuntimeParameters.getInstance();
                  EmailClient client = EmailClientFactory.getClient(runtime);
                  client.send(runtime.getSupportName(),
                          runtime.getSupportEmail(),
                          subject,
                          livenessMsg, HTMLizer.htmlize(livenessMsg));
            }
            catch (Exception e1) {
                  // log msg that we tried to send email notice to support.
                  RuntimeParameters runtime = RuntimeParameters.getInstance();
                  String recipient = runtime == null ? "unknown" : runtime.getSupportEmail();
                  String msg = MsgUtils.getMsg("TAPIS_SUPPORT_EMAIL_ERROR", recipient, subject, e1.getMessage());
                  _log.error(msg, e1);
            }
        }
    }
}
