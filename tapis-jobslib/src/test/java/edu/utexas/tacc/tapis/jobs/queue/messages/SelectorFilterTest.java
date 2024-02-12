package edu.utexas.tacc.tapis.jobs.queue.messages;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.jobs.queue.SelectorFilter;

/** Rudimentary testing of boolean expression evaluation.
 * 
 * @author rcardone
 */
@Test(groups={"unit"})
public class SelectorFilterTest 
{
	@Test
	public void filterTest1() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 > 0 AND i2 = 88";
		
		boolean result = SelectorFilter.match(filter, properties);
//		System.out.println("test1: " + result);
		Assert.assertEquals(result, true, "WRONG: '" + filter + "'");
	}
	
	@Test
	public void filterTest2() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 > 0 AND i2 = 89";
		
		boolean result = SelectorFilter.match(filter, properties);
		Assert.assertEquals(result, false, "WRONG: '" + filter + "'");
	}

	@Test
	public void filterTest3() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("s1", "hello");
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 > 0 AND i2 = 88 AND s1 LIKE 'hel%'";
		
		boolean result = SelectorFilter.match(filter, properties);
		Assert.assertEquals(result, true, "WRONG: '" + filter + "'");
	}
	
	@Test
	public void filterTest4() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("s1", "hello");
		properties.put("s2", "goodbye");
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 > 0 AND i2 = 88 AND s1 LIKE 'hel%' AND s2 NOT LIKE 'x%'";
		
		boolean result = SelectorFilter.match(filter, properties);
		Assert.assertEquals(result, true, "WRONG: '" + filter + "'");
	}
	
	@Test
	public void filterTest5() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 BETWEEN 0 AND 100";
		
		boolean result = SelectorFilter.match(filter, properties);
		Assert.assertEquals(result, true, "WRONG: '" + filter + "'");
	}

	@Test
	public void filterTest6() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 BETWEEN 0 AND 5";
		
		boolean result = SelectorFilter.match(filter, properties);
		Assert.assertEquals(result, false, "WRONG: '" + filter + "'");
	}
}
