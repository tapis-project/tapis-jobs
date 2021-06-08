package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.util.Properties;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;

public class GetSystem 
{
    /** Driver
     * 
     * @param args should contain the name of a file in the ./requests directory
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // Try to submit a job.
        var getSystem = new GetSystem();
        getSystem.get(args);
    }
    
    /** Create a tapis application from input in a file.
     * 
     * @param args contains the name of a request file
     * @throws IOException 
     * @throws TapisClientException 
     */
    public void get(String[] args) throws IOException, TapisClientException
    {
        // Check the input.
        if (args.length < 2) {
            System.out.println("Supply the system name and the profile name.");
            System.out.println("Generated profile path: " + TestUtils.getProfilePathTemplate());
            return;
        }
        
        // Read base url and jwt from file.
        Properties props = TestUtils.getTestProfile(args[1]);
        
        // Create the app.
        var sysClient = new SystemsClient(props.getProperty("BASE_URL"), props.getProperty("USER_JWT"));
        var sys = sysClient.getSystem(args[0]);
        System.out.println(sys.toString());
    }
}
