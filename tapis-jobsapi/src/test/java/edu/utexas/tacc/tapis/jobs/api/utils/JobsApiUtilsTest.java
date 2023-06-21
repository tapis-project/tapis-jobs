package edu.utexas.tacc.tapis.jobs.api.utils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response.Status;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public class JobsApiUtilsTest 
{
    @Test
    public void statusTest() {
        
        // Test that all conditions translate into their corresponding
        // status types as performed by JobsApiUtils.toHttpStatus().
        Condition condition = Condition.FORBIDDEN;
        Status status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.FORBIDDEN, status);
        
        condition = Condition.UNAUTHORIZED;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.UNAUTHORIZED, status);
        
        condition = Condition.NOT_FOUND;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.NOT_FOUND, status);
                
        condition = Condition.BAD_REQUEST;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.BAD_REQUEST, status);
        
        condition = Condition.INTERNAL_SERVER_ERROR;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.INTERNAL_SERVER_ERROR, status);
    }

    @Test
    public void implTest() {
        
        // Test that all conditions translate into their corresponding
        // status types as performed by JobsApiUtils.toHttpStatus().
        Status status = JobsApiUtils.toHttpStatus(Condition.FORBIDDEN);
        Assert.assertEquals(Status.FORBIDDEN, status);
        
        status = JobsApiUtils.toHttpStatus(Condition.UNAUTHORIZED);
        Assert.assertEquals(Status.UNAUTHORIZED, status);
        
        status = JobsApiUtils.toHttpStatus(Condition.NOT_FOUND);
        Assert.assertEquals(Status.NOT_FOUND, status);
                
        status = JobsApiUtils.toHttpStatus(Condition.BAD_REQUEST);
        Assert.assertEquals(Status.BAD_REQUEST, status);
        
        status = JobsApiUtils.toHttpStatus(Condition.INTERNAL_SERVER_ERROR);
        Assert.assertEquals(Status.INTERNAL_SERVER_ERROR, status);
    }
    
    @Test(enabled=false)
    public void canonicalizeDirectoryPathnamesTest() {
    	
    	// Experiment with different values to make sure we get what we expect.
    	var p = Path.of("/").toString();
    	Assert.assertEquals("/", p);
    	p = Path.of("//").toString();
    	Assert.assertEquals("/", p);
    	p = Path.of("///").toString();
    	Assert.assertEquals("/", p);
    	p = Path.of("////").toString();
    	Assert.assertEquals("/", p);
    	
    	p = Path.of("..").toString();
    	Assert.assertEquals("..", p);
    	p = Path.of("xx").toString();
    	Assert.assertEquals("xx", p);
    	p = Path.of("x/y/z").toString();
    	Assert.assertEquals("x/y/z", p);
    	p = Path.of("x//y///z/").toString();
    	Assert.assertEquals("x/y/z", p);
    	p = Path.of("").toString();
    	Assert.assertEquals("", p);
//    	System.out.println(p);
    }

    @Test(enabled=false)
    public void streamTest() {
    	var p1 = new edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair();
    	p1.setKey("p1");
    	var p2 = new edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair();
    	p2.setKey("p2");

    	var mergedAppKvList = new ArrayList<edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair>();
    	mergedAppKvList.add(p1);
    	mergedAppKvList.add(p2);
    	
        var mergedAppKvMap =
            	mergedAppKvList.stream().collect(
            		Collectors.toMap(edu.utexas.tacc.tapis.apps.client.gen.model.KeyValuePair::getKey,
            				         Function.identity()));
        
        System.out.println(mergedAppKvMap.size());
    }
    
    @Test(enabled=false)
    public void jsonSpaces() {
    	// Test whether keys with embedded spaces are accepted.
    	String json = """
    		{
    			"key with spaces": "value and space",
    			"nested x": {"key2 with spaces": "value2 and space"}
    		}""";
    	
    	TapisGsonUtils.getGson().fromJson(json, JsonObject.class);
    }
}
