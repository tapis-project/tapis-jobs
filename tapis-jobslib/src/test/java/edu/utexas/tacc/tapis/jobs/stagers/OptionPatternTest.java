package edu.utexas.tacc.tapis.jobs.stagers;

import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups={"unit"})
public class OptionPatternTest 
{
    // Get the option pattern regex.
    private static final Pattern _optionPattern = AbstractJobExecStager._optionPattern;
    //private static final Pattern _optionPattern = Pattern.compile("\\s*(--?[^=\\s]*)\\s*=?\\s*(\\S*)\\s*");
    
    // Arg [parms] splitter.
    private static final Pattern _spaceSplitter = Pattern.compile("(?U)\\s+");
    
    @Test
    public void ParseTest1()
    {
        // Happy path test...
        String arg = "-w /TapisInput";
        var m = _optionPattern.matcher(arg);
        boolean matches = m.matches();
        Assert.assertEquals(matches, true);
        
        int groupCount = m.groupCount();
        Assert.assertEquals(groupCount, 2, "Expected 2 groups, got " + groupCount);
        Assert.assertEquals(m.group(1), "-w");
        Assert.assertEquals(m.group(2), "/TapisInput");
        
//        System.out.println("groupCount: " + groupCount);
//        for (int i = 0; i <= groupCount; i++) System.out.println(" " + i + ": " + m.group(i));
    }
    
    @Test
    public void ParseTest2()
    {
        // Happy path test...
    	String arg = "--x v1 v2;3";
    	var split1 = _spaceSplitter.split(arg, 1);
    	var split2 = _spaceSplitter.split(arg, 2);
    	System.out.println("len1 = " + split1.length);
    	System.out.println("len2 = " + split2.length);
    	
    	var split3 = _spaceSplitter.split("--x", 2);
    	var split4 = _spaceSplitter.split("--x  ".strip(), 2);
    	var split5 = _spaceSplitter.split(" --x".strip(), 2);
    	
    	System.out.println();
    	
//        String arg = "-w /TapisInput";
//        arg.split(arg, 0);
//        arg.strip();
//        //StringUtils.split(arg);
//        var m = _optionPattern.matcher(arg);
//        boolean matches = m.matches();
//        Assert.assertEquals(matches, true);
//        
//        int groupCount = m.groupCount();
//        Assert.assertEquals(groupCount, 2, "Expected 2 groups, got " + groupCount);
//        Assert.assertEquals(m.group(1), "-w");
//        Assert.assertEquals(m.group(2), "/TapisInput");
        
//        System.out.println("groupCount: " + groupCount);
//        for (int i = 0; i <= groupCount; i++) System.out.println(" " + i + ": " + m.group(i));
    }
    
    
}
