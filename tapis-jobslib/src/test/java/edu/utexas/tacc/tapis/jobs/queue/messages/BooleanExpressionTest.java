package edu.utexas.tacc.tapis.jobs.queue.messages;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.filter.BooleanExpression;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.jobs.queue.SelectorFilter;

/** Rudimentary testing of boolean expression evaluation.
 * 
 * @author rcardone
 */
@Test(groups={"unit"})
public class BooleanExpressionTest 
{
	@Test
	public void filterTest1() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 > 0 AND i2 = 88";
		
		BooleanExpression result = SelectorFilter.parse(filter);
//		System.out.println(result);
		Assert.assertEquals(result.toString(), "( (i1 > 0) AND (i2 = 88) )");
	}
	
	@Test
	public void filterTest2() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 > 0 AND i2 = 89";
		
		BooleanExpression result = SelectorFilter.parse(filter);
		Assert.assertEquals(result.toString(), "( (i1 > 0) AND (i2 = 89) )");
	}

	@Test
	public void filterTest3() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("s1", "hello");
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 > 0 AND i2 = 88 AND s1 LIKE 'hel%'";
		
		BooleanExpression result = SelectorFilter.parse(filter);
		Assert.assertEquals(result.toString(), "((i1 > 0) AND (i2 = 88) AND (LIKE s1))");
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
		
		BooleanExpression result = SelectorFilter.parse(filter);
		Assert.assertEquals(result.toString(), "((i1 > 0) AND (i2 = 88) AND (LIKE s1) AND (NOT (LIKE s2)))");
	}
	
	@Test
	public void filterTest5() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 BETWEEN 0 AND 100";
		
		BooleanExpression result = SelectorFilter.parse(filter);
		Assert.assertEquals(result.toString(), "( (i1 >= 0) AND (i1 <= 100) )");
	}

	@Test
	public void filterTest6() throws Exception 
	{
		Map<String, Object> properties = new HashMap<>();
		properties.put("i1", 6);
		properties.put("i2", 88);
		
		String filter = "i1 BETWEEN 0 AND 5";
		
		BooleanExpression result = SelectorFilter.parse(filter);
		Assert.assertEquals(result.toString(), "( (i1 >= 0) AND (i1 <= 5) )");
	}
}
