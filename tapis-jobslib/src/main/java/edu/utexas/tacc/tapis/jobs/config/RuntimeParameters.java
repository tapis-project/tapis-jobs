package edu.utexas.tacc.tapis.jobs.config;

import java.text.NumberFormat;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConnectionFactory;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.parameters.TapisInput;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientParameters;
import edu.utexas.tacc.tapis.shared.providers.email.enumeration.EmailProviderType;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;

/** This class contains the complete and effective set of runtime parameters
 * for this service.  Each service has it own version of this file that
 * contains the resolved values of configuration parameters needed to
 * initialize and run this service alone.  By resolved, we mean the values
 * assigned in this class are from the highest precedence source as
 * computed by TapisInput.  In addition, this class does not contain values 
 * used to initialize services other than the one in which it appears.
 * 
 * The getInstance() method of this singleton class will throw a runtime
 * exception if a required parameter is not provided or if any parameter
 * assignment fails, such as on a type conversion error.  This behavior
 * can be used to fail-fast services that are not configured correctly by
 * calling getInstance() early in a service's initialization sequence.
 * 
 * @author rcardone
 */
public final class RuntimeParameters 
 implements EmailClientParameters
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RuntimeParameters.class);
  
    // Parameter defaults.
    private static final int CONNECTION_POOL_SIZE = 10;
  
    // Maximum size of a instance name string.
    private static final int MAX_INSTANCE_NAME_LEN = 26;
  
    // Default database metering interval in minutes.
    private static final int DEFAULT_DB_METER_INTERVAL_MINUTES = 60 * 24;
    
    // Email defaults.
    private static final String DEFAULT_EMAIL_PROVIDER = "LOG";
    private static final int    DEFAULT_EMAIL_PORT = 25;
    private static final String DEFAULT_EMAIL_FROM_NAME = "Tapis Jobs Service";
    private static final String DEFAULT_EMAIL_FROM_ADDRESS = "no-reply@nowhere.com";
    
    // Support defaults.
    private static final String DEFAULT_SUPPORT_NAME = "Oracle of Delphi";
    
    // DB run migration default
    private static final boolean DEFAULT_RUN_DB_MIGRATION = false;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Globally unique id that identifies this JVM instance.
    private static final TapisUUID id = new TapisUUID(UUIDType.JOB); 
  
    // Singleton instance.
    private static RuntimeParameters _instance = initInstance();
  
    // Distinguished user-chosen name of this runtime instance.
    private String  instanceName;
    
    // Credentials to get a token from the tokens service.
    private String  servicePassword;
    
    // The site in which this service is running.
    private String  siteId;
    
    // Tenant service location.
    private String  tenantBaseUrl;
    
    // Local node name.
    private String  localNodeName;
  
	// Database configuration.
	private String  dbConnectionPoolName;
	private int     dbConnectionPoolSize;
	private String  dbUser;
	private String  dbPassword;
	private String  jdbcURL;
	private int     dbMeterMinutes;
	
    // RabbitMQ configuration.
    private String  queueAdminUser;
    private String  queueAdminPassword;
    private int     queueAdminPort;
    private String  queueUser;
    private String  queuePassword;
    private String  queueHost;
    private int     queuePort;
    private boolean queueSSLEnabled;
    private boolean queueAutoRecoveryEnabled = true;
    
	// Mail configuration.
	private EmailProviderType emailProviderType;
	private boolean emailAuth;
	private String  emailHost;
	private int     emailPort;
	private String  emailUser;
	private String  emailPassword;
	private String  emailFromName;
	private String  emailFromAddress;
	
	// Support.
	private String  supportName;
	private String  supportEmail;
	
	// Allow test query parameters to be used.
	private boolean allowTestHeaderParms;
	
	// The slf4j/logback target directory and file.
	private String  logDirectory;
	private String  logFile;
	private boolean auditingEnabled;
	
	//Allow run db migration
	private boolean runDBMigration = DEFAULT_RUN_DB_MIGRATION;
	
	/* ********************************************************************** */
	/*                              Constructors                              */
	/* ********************************************************************** */
	/** This is where the work happens--either we can successfully create the
	 * singleton object or we throw an exception which should abort service
	 * initialization.  If an object is created, then all required input 
	 * parameters have been set in a syntactically valid way.
	 * 
	 * @throws TapisRuntimeException on error
	 */
	private RuntimeParameters()
	 throws TapisRuntimeException
	{
	  // Announce parameter initialization.
	  _log.info(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVICE", TapisConstants.SERVICE_NAME_JOBS));
	    
	  // --------------------- Get Input Parameters ---------------------
	  // Get the input parameter values from resource file and environment.
	  TapisInput tapisInput = new TapisInput(TapisConstants.SERVICE_NAME_JOBS);
	  Properties inputProperties = null;
	  try {inputProperties = tapisInput.getInputParameters();}
	  catch (TapisException e) {
	    // Very bad news.
	    String msg = MsgUtils.getMsg("TAPIS_SERVICE_INITIALIZATION_FAILED",
	                                 TapisConstants.SERVICE_NAME_JOBS,
	                                 e.getMessage());
	    _log.error(msg, e);
	    throw new TapisRuntimeException(msg, e);
	  }

	  // --------------------- Non-Configurable Parameters --------------
	  // We decide the pool name.
	  setDbConnectionPoolName(TapisConstants.SERVICE_NAME_JOBS + "Pool");
    
	  // --------------------- General Parameters -----------------------
	  // The name of this instance of the jobs library that has meaning to
	  // humans, distinguishes this instance of the job service, and is 
	  // short enough to use to name runtime artifacts.
	  String parm = inputProperties.getProperty(EnvVar.TAPIS_INSTANCE_NAME.getEnvName());
	  if (StringUtils.isBlank(parm)) {
	      // Default to some string that's not too long and somewhat unique.
	      // We check the current value to avoid reassigning on reload.  The
	      // integer suffix can add up to 10 characters to the string.
	      if (getInstanceName() == null)
	          setInstanceName(TapisConstants.SERVICE_NAME_JOBS + 
                              Math.abs(new Random(System.currentTimeMillis()).nextInt()));
	  } 
	  else {
	      // Validate string length.
	      if (parm.length() > MAX_INSTANCE_NAME_LEN) {
	          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_JOBS,
                                           "instanceName",
                                           "Instance name exceeds " + MAX_INSTANCE_NAME_LEN + "characters: " + parm);
	          _log.error(msg);
	          throw new TapisRuntimeException(msg);
      }
      if (!StringUtils.isAlphanumeric(parm)) {
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_JOBS,
                                           "instanceName",
                                           "Instance name contains non-alphanumeric characters: " + parm);
              _log.error(msg);
              throw new TapisRuntimeException(msg);
      }
    }
	
	// The site must always be provided.
	parm = inputProperties.getProperty(EnvVar.TAPIS_SITE_ID.getEnvName());
	if (StringUtils.isBlank(parm)) {
	    String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
	            TapisConstants.SERVICE_NAME_JOBS,
	            "siteId",
	            "No siteId specified.");
	    _log.error(msg);
	     throw new TapisRuntimeException(msg);
	}
	setSiteId(parm);
		  
    // Logging level of the Maverick libary code
    parm = inputProperties.getProperty(EnvVar.TAPIS_LOG_DIRECTORY.getEnvName());
    if (!StringUtils.isBlank(parm)) setLogDirectory(parm);
                 
    // Logging level of the Maverick libary code
    parm = inputProperties.getProperty(EnvVar.TAPIS_LOG_FILE.getEnvName());
    if (!StringUtils.isBlank(parm)) setLogFile(parm);
    
    // Set audit logging on or off (off by default).
    parm = inputProperties.getProperty(EnvVar.TAPIS_AUDITING_ENABLED.getEnvName());
    if (!StringUtils.isBlank(parm))
        try {setAuditingEnabled(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "auditingEnabled",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
    
    // Get the local name of the node that we are running on as set
    // by the deploying framework, such as Kubernetes.  If this set
    // then more extensive SSH logging will take place.
    parm = inputProperties.getProperty(EnvVar.TAPIS_LOCAL_NODE_NAME.getEnvName());
    if (!StringUtils.isBlank(parm)) setLocalNodeName(parm);
                 
    // Optional test header parameter switch.
    parm = inputProperties.getProperty(EnvVar.TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS.getEnvName());
    if (StringUtils.isBlank(parm)) setAllowTestHeaderParms(false);
      else {
        try {setAllowTestHeaderParms(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "allowTestQueryParms",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // Required parameter that allows us to aquire our JWTs.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SERVICE_PASSWORD.getEnvName());
    if (StringUtils.isBlank(parm)) {
        // Stop on bad input.
        String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                     TapisConstants.SERVICE_NAME_JOBS,
                                     "servicePassword");
        _log.error(msg);
        throw new TapisRuntimeException(msg);
      }
    setServicePassword(parm);
    
    // --------------------- Tenant Parameters ------------------------
    // We need to know where the tenant service is locaated.
    parm = inputProperties.getProperty(EnvVar.TAPIS_TENANT_SVC_BASEURL.getEnvName());
    if (!StringUtils.isBlank(parm)) setTenantBaseUrl(parm);
    
	// --------------------- DB Parameters ----------------------------
    // User does not have to provide a pool size.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_CONNECTION_POOL_SIZE.getEnvName());
    if (StringUtils.isBlank(parm)) setDbConnectionPoolSize(CONNECTION_POOL_SIZE);
      else {
        try {setDbConnectionPoolSize(Integer.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "dbConnectionPoolSize",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // DB user is required.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_USER.getEnvName());
    if (StringUtils.isBlank(parm)) {
      // Stop on bad input.
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                   TapisConstants.SERVICE_NAME_JOBS,
                                   "dbUser");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setDbUser(parm);

    // DB user password is required.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_PASSWORD.getEnvName());
    if (StringUtils.isBlank(parm)) {
      // Stop on bad input.
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                   TapisConstants.SERVICE_NAME_JOBS,
                                   "dbPassword");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setDbPassword(parm);
    
    // JDBC url is required.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_JDBC_URL.getEnvName());
    if (StringUtils.isBlank(parm)) {
      // Stop on bad input.
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                   TapisConstants.SERVICE_NAME_JOBS,
                                   "jdbcUrl");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setJdbcURL(parm);

    // Specify zero or less minutes to turn off database metering.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_METER_MINUTES.getEnvName());
    if (StringUtils.isBlank(parm)) setDbMeterMinutes(DEFAULT_DB_METER_INTERVAL_MINUTES);
      else {
        try {setDbConnectionPoolSize(Integer.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "dbMeterMinutes",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // --------------------- RabbitMQ Parameters ----------------------
    // The broker's administrator credentials used to set up vhost.
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_ADMIN_USER.getEnvName());
    if (!StringUtils.isBlank(parm)) setQueueAdminUser(parm);
       else {
           // Stop on bad input.
           String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                        TapisConstants.SERVICE_NAME_JOBS,
                                        "queueAdminUser");
           _log.error(msg);
           throw new TapisRuntimeException(msg);
       }
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_ADMIN_PASSWORD.getEnvName());
    if (!StringUtils.isBlank(parm)) setQueueAdminPassword(parm);
        else {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "queueAdminPassword");
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }
    
    // Optional broker port.
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_ADMIN_PORT.getEnvName());
    if (StringUtils.isBlank(parm)) setQueueAdminPort(isQueueSSLEnabled() ? 15671 : 15672);
      else {
        try {setQueueAdminPort(Integer.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "queuePort",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // This service's normal runtime credentials.
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_USER.getEnvName());
    if (!StringUtils.isBlank(parm)) setQueueUser(parm);
        else {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "queueUser");
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_PASSWORD.getEnvName());
    if (!StringUtils.isBlank(parm)) setQueuePassword(parm);
        else {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "queuePassword");
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }

    // Optional ssl enabled.  Compute this value before assigning a default port.
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_SSL_ENABLE.getEnvName());
    if (StringUtils.isBlank(parm)) setQueueSSLEnabled(false);
      else {
        try {setQueueSSLEnabled(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "queueSSLEnabled",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // Broker host defaults to localhost.
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_HOST.getEnvName());
    if (!StringUtils.isBlank(parm)) setQueueHost(parm);
      else setQueueHost("localhost");

    // Optional broker port.
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_PORT.getEnvName());
    if (StringUtils.isBlank(parm)) 
        setQueuePort(isQueueSSLEnabled() ? ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT : 
                                           ConnectionFactory.DEFAULT_AMQP_PORT);
      else {
        try {setQueuePort(Integer.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "queuePort",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // Optional auto-recovery enabled by default.
    parm = inputProperties.getProperty(EnvVar.TAPIS_QUEUE_AUTO_RECOVERY.getEnvName());
    if (StringUtils.isBlank(parm)) setQueueAutoRecoveryEnabled(true);
      else {
        try {setQueueAutoRecoveryEnabled(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "queueAutoRecoveryEnabled",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // --------------------- Email Parameters -------------------------
    // Currently LOG or SMTP.
    parm = inputProperties.getProperty(EnvVar.TAPIS_MAIL_PROVIDER.getEnvName());
    if (StringUtils.isBlank(parm)) parm = DEFAULT_EMAIL_PROVIDER;
    try {setEmailProviderType(EmailProviderType.valueOf(parm));}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "emalProviderType",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
        }
    
    // Is authentication required?
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_AUTH.getEnvName());
    if (StringUtils.isBlank(parm)) setEmailAuth(false);
      else {
          try {setEmailAuth(Boolean.valueOf(parm));}
              catch (Exception e) {
                  // Stop on bad input.
                  String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                               TapisConstants.SERVICE_NAME_JOBS,
                                               "emailAuth",
                                               e.getMessage());
                  _log.error(msg, e);
                  throw new TapisRuntimeException(msg, e);
              }
      }
    
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_HOST.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailHost(parm);
      else if (getEmailProviderType() == EmailProviderType.SMTP) {
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                       TapisConstants.SERVICE_NAME_JOBS,
                                       "emailHost");
          _log.error(msg);
          throw new TapisRuntimeException(msg);
      }
        
    // Get the email server port.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_PORT.getEnvName());
    if (StringUtils.isBlank(parm)) setEmailPort(DEFAULT_EMAIL_PORT);
      else
        try {setEmailPort(Integer.valueOf(parm));}
          catch (Exception e) {
              // Stop on bad input.
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_JOBS,
                                           "emailPort",
                                           e.getMessage());
              _log.error(msg, e);
              throw new TapisRuntimeException(msg, e);
          }

    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_USER.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailUser(parm);
      else if (isEmailAuth()) {
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                       TapisConstants.SERVICE_NAME_JOBS,
                                       "emailUser");
          _log.error(msg);
          throw new TapisRuntimeException(msg);
      }
        
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_PASSWORD.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailPassword(parm);
      else if (isEmailAuth()) {
        String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                     TapisConstants.SERVICE_NAME_JOBS,
                                     "emailPassword");
        _log.error(msg);
        throw new TapisRuntimeException(msg);
      }
        
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_FROM_NAME.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailFromName(parm);
      else setEmailFromName(DEFAULT_EMAIL_FROM_NAME);
        
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_FROM_ADDRESS.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailFromAddress(parm);
      else setEmailFromAddress(DEFAULT_EMAIL_FROM_ADDRESS);
    
    // --------------------- Support Parameters -----------------------
    // Chose a name for support or one will be chosen.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SUPPORT_NAME.getEnvName());
    if (!StringUtils.isBlank(parm)) setSupportName(parm);
     else setSupportName(DEFAULT_SUPPORT_NAME);
    
    // Empty support email means no support emails will be sent.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SUPPORT_EMAIL.getEnvName());
    if (!StringUtils.isBlank(parm)) setSupportEmail(parm);
      else if (DEFAULT_EMAIL_PROVIDER.equals(getEmailProviderType().name()))
          setSupportEmail(DEFAULT_EMAIL_PROVIDER);
    
	// ------------------- DB Migration ------------------------------
    // Optional. Default value for TAPIS_JOBS_RUN_MIGRATION is false.
    // Set TAPIS_JOBS_DB_RUN_MIGRATION to true for release < = 1.3.1
    parm = inputProperties.getProperty(EnvVar.TAPIS_JOBS_RUN_DB_MIGRATION.getEnvName());
    if (StringUtils.isBlank(parm)) setJobsRunDBMigration(false);
      else {
        try {setJobsRunDBMigration(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_JOBS,
                                         "jobsRunDBMigration",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
	}
	
	/* ---------------------------------------------------------------------- */
    /* getRuntimeInfo:                                                        */
    /* ---------------------------------------------------------------------- */
	/** Augment the buffer with printable text based mostly on the parameters
	 * managed by this class but also OS and JVM information.  The intent is 
	 * that the various job programs and utilities that rely on this class can
	 * print their configuration parameters, including those from this class, 
	 * when they start up.  
	 * 
	 * @param buf
	 */
	public void getRuntimeInfo(StringBuilder buf)
	{
		buf.append("\n------- Build -------------------------------------");
		buf.append("\ntapis version: ");
		buf.append(TapisUtils.getTapisVersion());
		buf.append("\ntapis build time: ");
		buf.append(TapisUtils.getBuildTime());
		buf.append("\ngit branch: ");
		buf.append(TapisUtils.getGitBranch());
		buf.append("\ngit commit: ");
		buf.append(TapisUtils.getGitCommit());
		
	    buf.append("\n------- Logging -----------------------------------");
        buf.append("\ntapis.log.directory: ");
        buf.append(this.getLogDirectory());
        buf.append("\ntapis.log.file: ");
        buf.append(this.getLogFile());
        
        buf.append("\n------- Network -----------------------------------");
        buf.append("\nHost Addresses: ");
        buf.append(getNetworkAddresses());
        
        buf.append("\n------- Tenants -----------------------------------");
        buf.append("\ntapis.site.id: ");
        buf.append(this.getSiteId());
        buf.append("\nservice: ");
        buf.append(TapisConstants.SERVICE_NAME_JOBS);
        buf.append("\ntapis.tenant.svc.baseurl: ");
        buf.append(this.getTenantBaseUrl());
	    
	    buf.append("\n------- DB Configuration --------------------------");
	    buf.append("\ntapis.db.jdbc.url: ");
	    buf.append(this.getJdbcURL());
	    buf.append("\ntapis.db.user: ");
	    buf.append(this.getDbUser());
	    buf.append("\ntapis.db.connection.pool.size: ");
	    buf.append(this.getDbConnectionPoolSize());
	    buf.append("\ntapis.db.meter.minutes: ");
	    buf.append(this.getDbMeterMinutes());
	    
        buf.append("\n------- RabbitMQ Configuration --------------------");
        buf.append("\ntapis.queue.host: ");
        buf.append(this.getQueueHost());
        buf.append("\ntapis.queue.admin.user: ");
        buf.append(this.getQueueAdminUser());
        buf.append("\ntapis.queue.admin.port: ");
        buf.append(this.getQueueAdminPort());
        buf.append("\ntapis.queue.user: ");
        buf.append(this.getQueueUser());
        buf.append("\ntapis.queue.port: ");
        buf.append(this.getQueuePort());
        buf.append("\ntapis.queue.ssl.enable: ");
        buf.append(this.isQueueSSLEnabled());
        buf.append("\ntapis.queue.auto.recovery: ");
        buf.append(this.isQueueAutoRecoveryEnabled());
        
	    buf.append("\n------- Email Configuration -----------------------");
	    buf.append("\ntapis.mail.provider: ");
	    buf.append(this.getEmailProviderType().name());
	    buf.append("\ntapis.smtp.auth: ");
	    buf.append(this.isEmailAuth());
	    buf.append("\ntapis.smtp.host: ");
	    buf.append(this.getEmailHost());
	    buf.append("\ntapis.smtp.port: ");
	    buf.append(this.getEmailPort());
	    buf.append("\ntapis.smtp.user: ");
	    buf.append(this.getEmailUser());
	    buf.append("\ntapis.smtp.from.name: ");
	    buf.append(this.getEmailFromName());
	    buf.append("\ntapis.smtp.from.address: ");
	    buf.append(this.getEmailFromAddress());
	    
	    buf.append("\n------- Support Configuration ---------------------");
	    buf.append("\ntapis.support.name: ");
	    buf.append(this.getSupportName());
	    buf.append("\ntapis.support.email: ");
	    buf.append(this.getSupportEmail());

	    buf.append("\n------- EnvOnly Configuration ---------------------");
	    buf.append("\ntapis.envonly.log.security.info: ");
	    buf.append(RuntimeParameters.getLogSecurityInfo());
	    buf.append("\ntapis.envonly.allow.test.header.parms: ");
	    buf.append(this.isAllowTestHeaderParms());
	    buf.append("\ntapis.envonly.jwt.optional: ");
	    buf.append(TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL));
	    buf.append("\ntapis.envonly.skip.jwt.verify: ");
	    buf.append(TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_SKIP_JWT_VERIFY));

	    buf.append("\n------- Java Configuration ------------------------");
	    buf.append("\njava.version: ");
	    buf.append(System.getProperty("java.version"));
	    buf.append("\njava.vendor: ");
	    buf.append(System.getProperty("java.vendor"));
	    buf.append("\njava.vm.version: ");
	    buf.append(System.getProperty("java.vm.version"));
	    buf.append("\njava.vm.vendor: ");
	    buf.append(System.getProperty("java.vm.vendor"));
	    buf.append("\njava.vm.name: ");
	    buf.append(System.getProperty("java.vm.name"));
	    buf.append("\nos.name: ");
	    buf.append(System.getProperty("os.name"));
	    buf.append("\nos.arch: ");
	    buf.append(System.getProperty("os.arch"));
	    buf.append("\nos.version: ");
	    buf.append(System.getProperty("os.version"));
	    buf.append("\nuser.name: ");
	    buf.append(System.getProperty("user.name"));
	    buf.append("\nuser.home: ");
	    buf.append(System.getProperty("user.home"));
	    buf.append("\nuser.dir: ");
	    buf.append(System.getProperty("user.dir"));
	    
	    buf.append("\n------- JVM Runtime Values ------------------------");
	    NumberFormat formatter = NumberFormat.getIntegerInstance();
	    buf.append("\navailableProcessors: ");
	    buf.append(formatter.format(Runtime.getRuntime().availableProcessors()));
	    buf.append("\nmaxMemory: ");
	    buf.append(formatter.format(Runtime.getRuntime().maxMemory()));
        buf.append("\ntotalMemory: ");
        buf.append(formatter.format(Runtime.getRuntime().totalMemory()));
        buf.append("\nfreeMemory: ");
        buf.append(formatter.format(Runtime.getRuntime().freeMemory()));
        
        buf.append("\n------- Jobs DB run migration ------------------------");
        buf.append("\ntapis.jobs.run.db.migration: ");
	    buf.append(TapisEnv.getBoolean(EnvVar.TAPIS_JOBS_RUN_DB_MIGRATION));
	}
	
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
	/* ---------------------------------------------------------------------- */
	/* initInstance:                                                          */
	/* ---------------------------------------------------------------------- */
	/** Initialize the singleton instance of this class.
	 * 
	 * @return the non-null singleton instance of this class
	 */
	private static synchronized RuntimeParameters initInstance()
	{
		if (_instance == null) _instance = new RuntimeParameters();
		return _instance;
	}
	
    /* ---------------------------------------------------------------------- */
    /* getNetworkAddresses:                                                   */
    /* ---------------------------------------------------------------------- */
	/** Best effort attempt to get the network addresses of this host for 
	 * logging purposes.
	 * 
	 * @return the comma separated string of IP addresses or null
	 */
    private String getNetworkAddresses()
    {
        // Comma separated result string.
        String addresses = null;
        
        // Best effort attempt to get this host's ip addresses.
        try {
            List<String> list = TapisUtils.getIpAddressesFromNetInterface();
            if (!list.isEmpty()) { 
                String[] array = new String[list.size()];
                array = list.toArray(array);
                addresses = String.join(", ", array);
            }
        }
        catch (Exception e) {/* ignore exceptions */}
        
        // Can be null.
        return addresses;
    }
    
	/* ********************************************************************** */
	/*                             Public Methods                             */
	/* ********************************************************************** */
	/* ---------------------------------------------------------------------- */
	/* reload:                                                                */
	/* ---------------------------------------------------------------------- */
	/** Reload the parameters from scratch.  Should not be called too often,
	 * but does allow updates to parameter files and environment variables
	 * to be recognized.  
	 * 
	 * Note that concurrent calls to getInstance() will either return the 
	 * new or old parameters object, but whichever is returned it will be
	 * consistent.  Calls to specific parameter methods will also be 
	 * consistent, but the instance on which they are called may be stale
	 * if it was acquired before the last reload operation.  
	 * 
	 * @return a new instance of the runtime parameters
	 */
	public static synchronized RuntimeParameters reload()
	{
	  _instance = new RuntimeParameters();
	  return _instance;
	}
	
	/* ---------------------------------------------------------------------- */
	/* getLogSecurityInfo:                                                    */
	/* ---------------------------------------------------------------------- */
	/** Go directly to the environment to get the latest security info logging
	 * value.  This effectively disregards any setting the appears in a 
	 * properties file or on the JVM command line.
	 * 
	 * @return the current environment variable setting 
	 */
	public static boolean getLogSecurityInfo()
	{
	    // Always return the latest environment value.
	    return TapisEnv.getLogSecurityInfo();
	}
  
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
	public static RuntimeParameters getInstance() {
		return _instance;
	}

	public String getSiteId() {
		return siteId;
	}

	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}

    public String getLocalNodeName() {
        return localNodeName;
    }

    public void setLocalNodeName(String localNodeName) {
        this.localNodeName = localNodeName;
    }
	
	public String getDbConnectionPoolName() {
		return dbConnectionPoolName;
	}

	private void setDbConnectionPoolName(String dbConnectionPoolName) {
		this.dbConnectionPoolName = dbConnectionPoolName;
	}

	public int getDbConnectionPoolSize() {
		return dbConnectionPoolSize;
	}

	private void setDbConnectionPoolSize(int dbConnectionPoolSize) {
		this.dbConnectionPoolSize = dbConnectionPoolSize;
	}

	public String getDbUser() {
		return dbUser;
	}

	private void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	private void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String getJdbcURL() {
		return jdbcURL;
	}

	private void setJdbcURL(String jdbcURL) {
		this.jdbcURL = jdbcURL;
	}

	public String getInstanceName() {
	    return instanceName;
	}

	private void setInstanceName(String name) {
	    this.instanceName = name;
	}

	public static TapisUUID getId() {
	    return id;
	}

	public boolean isAllowTestHeaderParms() {
	    return allowTestHeaderParms;
	}

	private void setAllowTestHeaderParms(boolean allowTestHeaderParms) {
	    this.allowTestHeaderParms = allowTestHeaderParms;
	}

	public int getDbMeterMinutes() {
	    return dbMeterMinutes;
	}

	private void setDbMeterMinutes(int dbMeterMinutes) {
	    this.dbMeterMinutes = dbMeterMinutes;
	}

    public String getQueueAdminUser() {
        return queueAdminUser;
    }

    public void setQueueAdminUser(String queueAdminUser) {
        this.queueAdminUser = queueAdminUser;
    }

    public String getQueueAdminPassword() {
        return queueAdminPassword;
    }

    public void setQueueAdminPassword(String queueAdminPassword) {
        this.queueAdminPassword = queueAdminPassword;
    }

    public int getQueueAdminPort() {
        return queueAdminPort;
    }

    public void setQueueAdminPort(int queueAdminPort) {
        this.queueAdminPort = queueAdminPort;
    }
    
    public String getQueueUser() {
        return queueUser;
    }

    public void setQueueUser(String queueUser) {
        this.queueUser = queueUser;
    }

    public String getQueuePassword() {
        return queuePassword;
    }

    public void setQueuePassword(String queuePassword) {
        this.queuePassword = queuePassword;
    }

    public String getQueueHost() {
        return queueHost;
    }

    public void setQueueHost(String queueHost) {
        this.queueHost = queueHost;
    }

    public int getQueuePort() {
        return queuePort;
    }

    public void setQueuePort(int queuePort) {
        this.queuePort = queuePort;
    }

    public boolean isQueueSSLEnabled() {
        return queueSSLEnabled;
    }

    public void setQueueSSLEnabled(boolean queueSSLEnabled) {
        this.queueSSLEnabled = queueSSLEnabled;
    }

    public boolean isQueueAutoRecoveryEnabled() {
        return queueAutoRecoveryEnabled;
    }

    public void setQueueAutoRecoveryEnabled(boolean queueAutoRecoveryEnabled) {
        this.queueAutoRecoveryEnabled = queueAutoRecoveryEnabled;
    }
    
    public EmailProviderType getEmailProviderType() {
        return emailProviderType;
    }

    public void setEmailProviderType(EmailProviderType emailProviderType) {
        this.emailProviderType = emailProviderType;
    }

    public boolean isEmailAuth() {
        return emailAuth;
    }

    public void setEmailAuth(boolean emailAuth) {
        this.emailAuth = emailAuth;
    }

    public String getEmailHost() {
        return emailHost;
    }

    public void setEmailHost(String emailHost) {
        this.emailHost = emailHost;
    }

    public int getEmailPort() {
        return emailPort;
    }

    public void setEmailPort(int emailPort) {
        this.emailPort = emailPort;
    }

    public String getEmailUser() {
        return emailUser;
    }

    public void setEmailUser(String emailUser) {
        this.emailUser = emailUser;
    }

    public String getEmailPassword() {
        return emailPassword;
    }

    public void setEmailPassword(String emailPassword) {
        this.emailPassword = emailPassword;
    }

    public String getEmailFromName() {
        return emailFromName;
    }

    public void setEmailFromName(String emailFromName) {
        this.emailFromName = emailFromName;
    }

    public String getEmailFromAddress() {
        return emailFromAddress;
    }

    public void setEmailFromAddress(String emailFromAddress) {
        this.emailFromAddress = emailFromAddress;
    }

    public String getSupportName() {
        return supportName;
    }

    public void setSupportName(String supportName) {
        this.supportName = supportName;
    }

    public String getSupportEmail() {
        return supportEmail;
    }

    public void setSupportEmail(String supportEmail) {
        this.supportEmail = supportEmail;
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public void setLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

	public boolean isAuditingEnabled() {
		return auditingEnabled;
	}

	public void setAuditingEnabled(boolean auditingEnabled) {
		this.auditingEnabled = auditingEnabled;
	}

    public String getTenantBaseUrl() {
        return tenantBaseUrl;
    }

    public void setTenantBaseUrl(String tenantBaseUrl) {
        this.tenantBaseUrl = tenantBaseUrl;
    }

	public String getServicePassword() {
		return servicePassword;
	}

	public void setServicePassword(String servicePassword) {
		this.servicePassword = servicePassword;
	}
	
	public boolean getJobsRunDBMigration() {
		return runDBMigration;
	}
	public void setJobsRunDBMigration(boolean runDBMigration ) {
		this.runDBMigration = runDBMigration ;
	}	
}
