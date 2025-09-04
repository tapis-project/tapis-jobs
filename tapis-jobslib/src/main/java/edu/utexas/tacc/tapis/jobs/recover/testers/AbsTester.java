package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.concurrent.ExecutionException;

import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTester;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisRecoverableException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection.AuthMethod;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient.AuthnMethod;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public abstract class AbsTester 
 implements RecoverTester
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbsTester.class);
    
    // Batch size of recovered jobs.
    protected static final int NO_RESUBMIT_BATCHSIZE = 0;
    protected static final int DEFAULT_RESUBMIT_BATCHSIZE = 10;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // The recovery job to which this policy will be applied.
    protected final JobRecovery _jobRecovery;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected AbsTester( JobRecovery jobRecovery)
    {
        _jobRecovery = jobRecovery;
    }

    /* **************************************************************************** */
    /*                              Protected Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getSystem:                                                                   */
    /* ---------------------------------------------------------------------------- */
    // NOTE that we always use the default authnMethod for the system.
    // If that has changed, or credentials have change/deleted, etc, that could cause problems.
    protected TapisSystem getSystem(String username, String tenant, String systemId, String sharedAppCtx)
     throws RuntimeException, TapisException, ExecutionException, TapisClientException
    {
        // Get the Systems service client.
        SystemsClient client = ServiceClients.getInstance().getClient(username, tenant, SystemsClient.class);
            
        // Lookup the system. By assuming a shared application context we maximize our
        // chances for success. This assumption is safe for the following reasons:
        //
        //   If the user was unable to connect or authenticate to the actual host,
        //   they must have already been able to retrieve the system definition. If
        //   they were able to retrieve the definition without being in a shared
        //   application context, then they should still be able to do so when 
        //   sharing is specified. If sharing was required, we're covered.
        //
        final AuthnMethod authnMethod = null;
        final boolean returnCreds = true;
        final boolean requireExecPerm = false;
        final String  selectAll = "allAttributes";
        final String  impersonationId = null;
        return client.getSystem(systemId, authnMethod, requireExecPerm, selectAll, returnCreds, impersonationId,
                                sharedAppCtx);
    }

    /* ---------------------------------------------------------------------- */
    /* getSSHConnection:                                                      */
    /* ---------------------------------------------------------------------- */
    protected SSHConnection getSSHConnection(String username, String systemId, 
                                             AuthMethod authMethod, String sharedAppCtx) 
     throws JobRecoveryAbortException
    {
        // We currently support two authentication methods. Anything else is an error
        if (authMethod != AuthMethod.PASSWORD_AUTH && authMethod != AuthMethod.PUBLICKEY_AUTH) {
            String msg = JobUtils.getMsg("JOBS_RECOVERY_UNKNOWN_SSH_AUTHN", authMethod.name());
            throw new JobRecoveryAbortException(msg);
        }
        
        // Get the system definition with credentials.
        TapisSystem system = null;
        try {system = getSystem(username, _jobRecovery.getTenantId(), systemId, sharedAppCtx);}
            catch (TapisRecoverableException e) {
                // Record problem.
                String msg = JobUtils.getMsg("JOBS_RECOVERY_TEST_SETUP_ERROR", _jobRecovery.getId(), e.getMessage());
                _log.warn(msg);
                return null;  // Try again later.
            }
            catch (Exception e) {
                // Fatal unhandled error
                String msg = JobUtils.getMsg("JOBS_RECOVERY_TEST_FATAL_ERROR", _jobRecovery.getId(), e.getMessage());
                throw new JobRecoveryAbortException(msg, e);
            }
        
        // Try to connect to the system. Note that we use the host, port and username from system even though
        // they are also in the tester parameters. They should be the same, but ones in the system are the ones
        // that will be used during job processing.
        // NOTE that we always use the default authnMethod for the system.
        // If that has changed, or credentials have change/deleted, etc, that could cause problems.
        SSHConnection conn = null;
        try {
            var runCmd = new TapisRunCommand(system);
            conn = runCmd.getConnection();
        } catch (TapisRecoverableException e) {
            // The error condition has not cleared, but we live to fight another day.
            return null;
        } catch (Exception e) {
            // Fatal unhandled error
            String msg = JobUtils.getMsg("JOBS_RECOVERY_TEST_CONN_FATAL_ERROR", _jobRecovery.getId(), system.getId(),
                                         e.getMessage());
            throw new JobRecoveryAbortException(msg, e);
        }
        
        // Successful connection.
        return conn;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSystemHostMessage:                                                  */
    /* ---------------------------------------------------------------------- */
    protected String getSystemHostMessage(TapisSystem system)
    {return system.getId() + " (" + system.getHost() + ")";}
}
