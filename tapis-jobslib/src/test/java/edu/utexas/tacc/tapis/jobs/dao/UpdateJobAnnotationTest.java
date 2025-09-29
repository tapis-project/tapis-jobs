package edu.utexas.tacc.tapis.jobs.dao;
import java.util.Set;
import java.util.TreeSet;

import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobAnnotation;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class UpdateJobAnnotationTest {
    @Test
	public void jobTest() throws TapisException
	{
		// Access the database.
		var dao = new JobsDao();
				
		// Insert job record.
		Job job = initJob();
		dao.createJob(job);

        var notes = """
            {"a": "b", "c": {"d": "e", "f": 3}, "g": [1, 2, 3], "h": {"i": {"j": "k"}}}
                """;
        Gson gson = new Gson();
        var notesObj = gson.fromJson(notes, JsonObject.class);
        var tags = new TreeSet<>(Set.of("tag1", "tag2", "tag3", "tag4"));
        JobAnnotation patched = dao.updateJobAnnotations(job.getUuid(), job.getTenant(), 
            job.getOwner(), tags, notesObj, false);
        System.out.println("Patched job annotation: " + patched);

        notes = """
            {}
                """;
        tags = new TreeSet<>(Set.of("tag1", "tag5"));
        notesObj = gson.fromJson(notes, JsonObject.class);
        JobAnnotation replaced = dao.updateJobAnnotations(job.getUuid(), job.getTenant(), 
            job.getOwner(), tags, notesObj, true);
        System.out.println("Replaced job annotation: " + replaced);
	}


    /* ********************************************************************** */
	/*                            Private Methods                             */
	/* ********************************************************************** */
	private Job initJob()
	{
		var job = new Job();
		
		// Required fields
		job.setName("test1job");
		job.setOwner("bud");
		job.setTenant("fakeTenant");
		job.setDescription("This is a fake job that will never run");

	    job.setAppId("fakeAppId");
	    job.setAppVersion("1.0");
	    
	    job.setExecSystemId("fakeExecSystemId");
	    
	    job.setTapisQueue("fakeTapisQueue");
	    job.setCreatedby("mary");
	    job.setCreatedbyTenant("maryTenant");
		
	    // Optional fields.
	    String json = 
	    		"{\"parameterSet\": {\"appArgs\": [{\"arg\": \"x\"}, {\"arg\": \"-f y.txt\"}], "
				+ "                \"containerArgs\": [{\"arg\": \"-v 3\", "
				+ "                                     \"meta\": {\"name\": \"bud\", \"required\": true, "
				+ "                                        \"kv\": [{\"key\": \"k1\", \"value\": \"v1\"}, "
				+ "                                                 {\"key\": \"k2\", \"value\": \"v2\"}]}}],"
				+ "                \"schedulerOptions\": [{\"arg\": \"-A 34493\"}], "
				+ "                \"envVariables\": [{\"key\": \"TAPIS_SERVICE\", \"value\": \"jobs\"}]"
				+ "}}";
	    job.setParameterSet(json);
        job.setJobType(JobType.FORK);
        job.setExecSystemExecDir("json");
        job.setExecSystemInputDir("json");
        job.setExecSystemOutputDir("json");
        job.setArchiveSystemId("json");
        job.setArchiveSystemDir("json");


        String notes = """
            {"a": "b", "c": {"d": "e", "f": 3}, "g": [1, 2, 3]}
                """;
        Gson gson = new Gson();
        var notesObj = gson.fromJson(notes, JsonObject.class);
        job.setNotes(notesObj);

        TreeSet<String> tags = new TreeSet<>(Set.of("tag1", "tag2", "tag3"));
        job.setTags(tags);
	    
		return job;
	}
}
