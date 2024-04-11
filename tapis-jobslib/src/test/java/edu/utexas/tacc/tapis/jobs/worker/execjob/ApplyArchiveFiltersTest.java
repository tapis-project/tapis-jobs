package edu.utexas.tacc.tapis.jobs.worker.execjob;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.files.client.gen.model.FileInfo;

/** This class test the archiving code from the applyArchiveFilters() method using
 * the unsophisticated approach of duplicating the original code here and tweaking
 * it to run as a unit test.  If the actual production code that is replicated here
 * changes, this test code will also have to change to stay pertinent.
 * 
 * @author rcardone
 */
@Test(groups={"unit"})
public class ApplyArchiveFiltersTest 
{
	// Test fields assigned before or during testing.
	private String         _outputPathPrefix;
	private List<String>   _excludes;
	private List<String>   _includes;
	private List<FileInfo> _fileInfoList;
	private int            _excludedCount;
	
    /* ---------------------------------------------------------------------- */
    /* beforeTest:                                                           */
    /* ---------------------------------------------------------------------- */
	@BeforeTest                                              
	public void beforeTest()  
	{  
		// Assign the test data field values.
	    // System.out.println("Runnung @BeforeTest setup");
	    _outputPathPrefix = "/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/";
	    
	    // Excludes in same order as original job.
	    _excludes = new ArrayList<String>();
	    _excludes.add("tapisjob.env");
	    _excludes.add("openfoam_v9.zip");
	    _excludes.add("inputDirectory/pisoFoam.log");
	    
	    // Includes.
	    _includes = new ArrayList<String>();
	    
	    // File list assignments.
	    String[] srcFiles = {
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/tapisjob.sh",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/tapisjob.env",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/openfoam_v9.zip",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/tapisjob.out",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/tapisjob.sh",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/tapisjob_app.sh",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/2_nodes_vis.slurm",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/U.jpg",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/blockMesh.log",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/decomposePar.log",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/pisoFoam.log",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/reconstructPar.log",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/system/controlDict",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/system/decomposeParDict",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/system/fvSchemes",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/system/fvSolution",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/system/sampleDict",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/constant/LESProperties",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/constant/transportProperties",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/constant/turbulenceProperties",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/constant/polyMesh/blockMeshDict",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/0/U",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/0/boundary",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/0/nuSgs",
	    	"/scratch1/06099/sal/tapis/1d0ede00-5b4e-4268-a78f-9a4458526fd3-007/inputDirectory/0/p",
	    };
	    
	    // Wrap each file path in an info object and add to list.
	    _fileInfoList = new ArrayList<FileInfo>();
	    for (var src : srcFiles) {
	    	var info = new FileInfo();
	    	info.setPath(src);
	    	_fileInfoList.add(info);
	    }
	}  
	
    /* ---------------------------------------------------------------------- */
    /* testExcludes:                                                          */
    /* ---------------------------------------------------------------------- */
	@Test
	public void testExcludes()
	{
		// System.out.println("Starting test1"); 
    	applyArchiveFilters(_excludes, _fileInfoList, FilterType.EXCLUDES);
    	applyArchiveFilters(_includes, _fileInfoList, FilterType.INCLUDES);
    	
    	// We should have excluded 3 files from the hardcode list.
    	Assert.assertEquals(_excludedCount, 3, 
    			            "We didn't exclude as many files as expected!"); 
	}
	
	// ************************************************************************
	// CODE COPIED FROM JobFileManager 
	// ************************************************************************
	
    // Filters are interpreted as globs unless they have this prefix.
    public static final String REGEX_FILTER_PREFIX = "REGEX:";
    
    // Archive filter types.
    private enum FilterType {INCLUDES, EXCLUDES}
    
    /* ---------------------------------------------------------------------- */
    /* applyArchiveFilters:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Apply either the includes or excludes list to the file list.  In either
     * case, the file list can be modified by having items deleted.
     * 
     * The filter items can either be in glob or regex format.  Each item is 
     * applied to the path of a file info object.  When a match occurs the 
     * appropriate action is taken based on the filter type being processed.
     * 
     * @param filterList the includes or excludes list as identified by the filterType 
     * @param fileList the file list that may have items deleted
     * @param filterType filter indicator
     */
    private void applyArchiveFilters(List<String> filterList, List<FileInfo> fileList, 
                                     FilterType filterType)
    {
        // Is there any work to do?
        if (filterType == FilterType.EXCLUDES) {
            if (filterList.isEmpty()) return;
        } else 
            if (filterList.isEmpty() || matchesAll(filterList)) return;
        
        // Local cache of compiled regexes.  The keys are the filters
        // exactly as defined by users and the values are the compiled 
        // form of those filters.
        HashMap<String,Pattern> regexes   = new HashMap<>();
        HashMap<String,PathMatcher> globs = new HashMap<>();
        
        // Iterate through the file list.
        final int lastFilterIndex = filterList.size() - 1;
        var fileIt = fileList.listIterator();
        while (fileIt.hasNext()) {
            var fileInfo = fileIt.next();
            var path = getOutputRelativePath(fileInfo.getPath());
            for (int i = 0; i < filterList.size(); i++) {
                // Get the current filter.
                String filter = filterList.get(i);
                
                // Use cached filters to match paths.
                boolean matches = matchFilter(filter, path, globs, regexes);
                if (matches) _excludedCount++;
                // System.out.println(matches + " filter=" + filter + ", path=" + path);
                
                // Removal depends on matches and the filter type.
                if (filterType == FilterType.EXCLUDES) {
                    if (matches) {fileIt.remove(); break;}  //************ fixed
                } else {
                    // Remove item only after all include filters have failed to match.
                    if (matches) break; // keep in list 
                    if (!matches && (i == lastFilterIndex)) fileIt.remove();
                }
            }
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getOutputRelativePath:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Strip the job output directory prefix from the absolute pathname before
     * performing a filter matching operation.
     * 
     * @param absPath the absolute path name of a file rooted in the job output directory 
     * @return the path name relative to the job output directory
     */
    private String getOutputRelativePath(String absPath)
    {
        var prefix = _outputPathPrefix;
        if (absPath.startsWith(prefix))
            return absPath.substring(prefix.length());
        // Special case if Files strips leading slash from output.
        if (!absPath.startsWith("/") && prefix.startsWith("/") &&
            absPath.startsWith(prefix.substring(1)))
            return absPath.substring(prefix.length()-1);
        return absPath;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getOutputPathPrefix:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Assign the filter ignore prefix value for this job.  This value is the
     * path prefix (with trailing slash) that will be removed from all output
     * file path before filtering is carried out.  Users provide glob or regex
     * pattern that are applied to file paths relative to the job output directory. 
     * 
     * @return the prefix to be removed from all paths before filter matching
     */
// 
//   *** THIS ORIGINAL CODE IS REPLACED BY A HARDCODED PATH IN _outputPathPrefix ***
//
//    private String getOutputPathPrefix()
//    {
//        // Assign the filter ignore prefix the job output directory including 
//        // a trailing slash.
//        if (_filterIgnoreOutputPrefix == null) {
//            _filterIgnoreOutputPrefix = _job.getExecSystemOutputDir();
//            if (!_filterIgnoreOutputPrefix.endsWith("/")) _filterIgnoreOutputPrefix += "/";
//        }
//        return _filterIgnoreOutputPrefix;
//    }
    
    /* ---------------------------------------------------------------------- */
    /* matchFilter:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Determine if the path matches the filter, which can be either a glob
     * or regex.  In each case, the appropriate cache is consulted and, if
     * necessary, updated so that each filter is only compiled once per call
     * to applyArchiveFilters().
     * 
     * @param filter the glob or regex
     * @param path the path to be matched
     * @param globs the glob cache
     * @param regexes the regex cache
     * @return true if the path matches the filter, false otherwise
     */
    private boolean matchFilter(String filter, String path, 
                                HashMap<String,PathMatcher> globs,
                                HashMap<String,Pattern> regexes)
    {
        // Check the cache for glob and regex filters.
        if (filter.startsWith(REGEX_FILTER_PREFIX)) {
            Pattern p = regexes.get(filter);
            if (p == null) {
                p = Pattern.compile(filter.substring(REGEX_FILTER_PREFIX.length()));
                regexes.put(filter, p);
            }
            var m = p.matcher(path);
            return m.matches();
        } else {
            PathMatcher m = globs.get(filter);
            if (m == null) {
                m = FileSystems.getDefault().getPathMatcher("glob:"+filter);
                globs.put(filter, m);
            }
            var pathObj = Paths.get(path);
            return m.matches(pathObj);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* matchesAll:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Determine if the filter list will match any string.  Only the most 
     * common way of specifying a pattern that matches all strings are tested. 
     * In addition, combinations of filters whose effect would be to match all
     * strings are not considered.  Simplistic as it may be, filters specified
     * in a reasonable, straightforward manner to match all strings are identified.   
     * 
     * @param filters the list of glob or regex filters
     * @return true if list contains a filter that will match all strings, false 
     *              if no single filter will match all strings
     */
    private boolean matchesAll(List<String> filters)
    {
        // Check the most common ways to express all strings using glob.
        if (filters.contains("**/*")) return true;
        
        // Check the common way to express all strings using a regex.
        if (filters.contains("REGEX(.*)")) return true;
        
        // No no-op filters found.
        return false;
    }
}
