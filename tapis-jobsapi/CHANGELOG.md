# Change Log for Tapis Jobs Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/jobs.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

-----------------------
## 1.7.0 - 2024-09-17

### Bug fixes:
1. Error message fix.

-----------------------
## 1.6.4 - 2024-08-20

### New Features:
1. Allow docker --entrypoint container parameter to be specified.
2. Kill FORK jobs whose maxMinutes have expired.
3. Resolve macros that appear in archiveFilter lists.

### Bug fixes:
1. Fix ZIP bug when staging applications and extraneous text is written to stdout.

-----------------------
## 1.6.3 - 2024-06-25

### New Features:
1. Support use of spaces in environment variable values for ZIP runtime type.

### Bug fixes:
1. Allow spaces in execSystemExecDir paths when uploading application assets.
2. Properly set the remoteSubmitted timestamp.
3. Properly override application INCLUDE_BY_DEFAULT and INCLUDE_ON_DEMAND arguments. 

-----------------------
## 1.6.2 - 2024-05-19

### New Features:
1. Update maven repository reference.
2. Define non-root image in Dockerfile.

### Bug fixes:
1. Avoid calling mkdir on S3 systems.

-----------------------
## 1.6.1 - 2024-04-11

### New Features:
1. Data Transfer Node (DTN) support.
2. Introduced a condition code set when a job terminates to impart more information about job disposition.
3. Introduced the envKey field in FileInputs and FileInputArrays to allow users to easily pass input file 
   pathnames into their applications via environment variables.
4. Support for application enable/disable at the application version level.  

### Bug fixes:
1. Fixed job output listing when limit/skip are used.
2. Fixed unrecognized parameter problem on Slurm apptainer (singularity) jobs.
3. Fixed LogConfig for Slurm/Singularity jobs.
4. Fixed archiveFilter excludes processing.
5. Fixed error when loading certain exception classes reflectively.

-----------------------
## 1.6.0 - 2024-01-24

### New Features:
1. Increment release number.

-----------------------
## 1.5.11 - 2024-01-10

### New features:
1. Support for ZIP runtime.
2. Improve defense against command injection in job submission.
3. Allow multiple, separate --bind options to be specified in singularity containerArgs.

### Bug fixes:
1. Fix unrecognized sharing when application-defined system is overridden.
2. Allow spaces and reserved characters in path names expressed as URLs.  

-----------------------
## 1.5.10 - 2023-11-20

### Bug fixes:
1. Rebuild with latest shared code to fix JWT validation issue.

-----------------------
## 1.5.0 - 2023-10-06

### New features:
1. Write a source-able environment file when running slurm singularity jobs.
2. Added an optional environment variable tapis.jobs.run.db.migration to allow DB migration

### Bug fixes:
1. Fix problem when a certain combination of inputs would incorrectly process FIXED arguments.

-----------------------
## 1.4.3 - 2023-09-28

### New features:
1. Macro expand appArgs & containerArgs on job requests.
2. Implement events probes to test for notification liveness.

### Bug fixes:
1. Recognize recoverable SSH exceptions that occur during job monitoring.

-----------------------
## 1.4.2 - 2023-08-31

### Bug fixes:
1. Fix JWT timeout on calls to Files while monitoring transfers.
2. Add connection retries to monitoring calls that experience channel errors.

-----------------------
## 1.4.1 - 2023-08-16

### New features:
1. Added the logConfig option to the job submission requests to allow stdout and stderr to written to different files when using Singularity containers.  Also allows those files to have user-specified names. 


### Bug fixes:
1. Canonicalize job request paths to start with a slash. This allows path equality tests to not return false negatives, which is especially important when setting up file transfers.

2. Handle the new FileInfo enum type to avoid missing some files during archiving and output downloading.

3. Fix DTN management during archiving.

4. Stop application sharing migration error message from displaying when not necessary.

-----------------------
## 1.4.0 - 2023-07-17

### New features:
1. Tapis 1.4.0 release.

-----------------------
## 1.3.6 - 2023-06-22

### New features:
1. Enable the *--rm* container option in the docker container runtime.  If present, this option is passed on the *docker run* command forcing the container to be deleted after it runs.  Logs and other application output should be explicitly saved to file by applications when such information needs to outlive container execution.   

2. Support for extended environment variable definitions in Apps and Systems, as well as the addition of the notes and include fields in Jobs environment variable definitions.

3. Increased SSH and FileClient timeouts to avoid canceling slow network operations that will probably succeed.

-----------------------
## 1.3.5 - 2023-06-01

### New features:

### Bug fixes:
1. Fixed improper assignment of slurm --gpus, --begin and --nodelist parameters.

-----------------------
## 1.3.4 - 2023-05-16

### New features:
1. Added allowIfRunning query parameter to job output list and download end-points. Setting allowIfRunning=true will allow users to download the output files when the job is still running. If allowIfRunning=false and job is running, it will now return a strict 400 error.

### Bug fixes:
1. Fixed bug in application subscription merge.

## 1.3.3 - 2023-04-19

### New features:

1. Incorporated Files client with updated timeouts.

### Bug fixes:
1. Use latest version of tapis-client-java library which contains
   adjust timeout for SSH communication. 

-----------------------
## 1.3.2 - 2023-03-27

### New features:

### Bug fixes:
1. Sharing privilege escalation fix.
2. Upgrade to JDK 17.0.6-tem and tomcat:9.0.73-jdk17-temurin-jammy

-----------------------
## 1.3.1 - 2023-03-22

### New features:
1. Allow cmdPrefix strings to be up to 1024 characters.

### Bug fixes:
1. Guard against -1 quota values for batch queues.
2. Allow spaces in singularity environment variable values

-----------------------
## 1.3.0 - 2023-03-03

### New features:
1. Added listing of shared jobs in POST search end-point
2. Enhanced SchedulerProfile format
3. Added select feature to search endpoints

### Bug fixes:
1. Increased timeout to 10 minutes on job submission calls.
2. Improved docker output parsing
3. Various simple bug fixes

-----------------------
## 1.2.4 - 2023-02-07

### New features:
1. Allow macro substitution in scheduler's job name field
2. Support for Systems enableCmdPrefix flag
3. Improved job search using a query parameter

-----------------------
## 1.2.3 - 2022-09-22

### New features:
1. Application shared context support 
2. Added Description field to KeyValuePair
3. JOB_USER_EVENT implemented
4. Improved Job submit livedocs
5. Allowed 0 ttlMinutes in subscriptions
6. Tightened job subscription checking
7. Added notes field to job

### Bug fixes:
1. Various simple bug fixes

-----------------------
## 1.2.2 - 2022-08-23

Fixes and preview feature release
### New features:

### Bug fixes:
1. Allow JobOutput listing for jobs that ran in shared app context

## 1.2.2 - 2022-08-23

Fixes and preview feature release

### Breaking Changes:


### New features:

### Bug fixes:
1. Added --force flag for docker container removal for FORK jobs on job cancel
2. Jobs OpenAPI spec schema fixed

## 1.2.1 - 2022-07-25

Fixes and preview feature release

### Breaking Changes:
1. Changed jobuuid parameter to jobUuid in resubmission APIs for consistency. 

### New features:
1. Job subscription APIs
2. Job event generation and transmission
3. Job shared history, resubmit request and output APIs
4. Updated 3rd party libraries

### Bug fixes:
1. Better test for batchqueue assignment to avoid NPE

-----------------------

## 1.2.0 - 2022-05-31

Maintenance release

### Breaking Changes:
- Refer to renamed notification subscription classes in shared library.

### New features:

### Bug fixes:

-----------------------

## 1.1.3 - 2022-05-09

Maintenance release

### Breaking Changes:
- none.

### New features:
1. Adjust JVM memory options and other deployment file clean up.
2. Improve JWT validation and authentication logging.

### Bug fixes:

-----------------------

## 1.1.2 - 2022-03-04

Java 17 upgrade

### Breaking Changes:
- none.

### New features:
1. Upgrade code and images to Java 17.0.2.
2. Generalized job event table to handle non-status events.
3. Added more throttling to Jobs service to better withstand job request bursts.
4. Tighten SK write secret endpoint validation.

### Bug fixes:

-----------------------

## 1.1.1 - 2022-02-01

Bug fix release

### Breaking Changes:
- none.

### Bug fixes:
1. Applied application limits to recovery as intended.
2. Adjusted how PENDING jobs are accounted for during recovery.
3. Fixed singularity --mount and --fusemount assignements.

-----------------------

## 1.1.0 - 2022-01-07

New minor release.

### Breaking Changes:
- none

### New features:
1. Fail-fast support for jobs that experience unrecoverable Docker errors.
2. Support for renamed task status enum in Apps client. 

-----------------------

## 1.0.6 - 2021-12-17

MPI and command prefix support.

### Breaking Changes:
- none

### New features:
1. MPI support.
2. Command prefix support.
3. Support for latest App and Systems interfaces.

### Bug fixes:
1. Hide/unhide flag fix.

-----------------------

## 1.0.5 - 2021-12-09

New job request interface release.

### Breaking Changes:
- JobType migration may be required.

### New features:
1. Job request args and inputs tweaks.
2. Use updated TapisSystem.getCanRunBatch() call.
3. Implemented JobType support.
4. Implemented batchSchedulerProfile support.
5. Implemented hide/unhide apis.

### Bug fixes:
1. Fix jobType assignment.
2. Fix openapi specification generation.

-----------------------

## 1.0.4 - 2021-11-10

New job request interface release.

### Breaking Changes:
- Job request interface changed.

### New features:
1. Added JSON schema for search request.
2. Support for new FileInputs design and interface.
3. Support for new FileInputArrays design and interface.
4. Support for new JobArgsSpec design and interface.
5. Support for new tapisLocal design and interface.
6. Updated job request tests.
7. Support for ArgInputModeEnum name change.

-----------------------

## 1.0.3 - 2021-10-12

Bug fix release.

### Breaking Changes:
- none

### New features:
1. Added ListFiles2 test program.

### Bug fixes:
1. Fix version in non-standard POM files.
2. Fixes to job search along with code clean up.
3. Fix includeLaunchFiles bug.

-----------------------

## 1.0.2 - 2021-09-17

Bug fix release.

### Breaking Changes:
- none

### Bug fixes:
1. Fix job listing conditional statement.
2. Fix TooManyFailures problem. 

-----------------------

## 1.0.1 - 2021-09-15

Incremental improvement and bug fix release.

### Breaking Changes:
- none

### New features:
1. Provided a default job description if one is not specified on job submission.

### Bug fixes:
1. Quoted environment variable values when they appear on command line
   for job submission to Slurm scheduler.
2. Account for file listing output pathnames that don't preserve the
   input specification's leading slash.
3. Fixed empty job listing case.
4. Added support for the singularity run --pwd option.
5. Added missing error message. 


-----------------------

## 1.0.0 - 2021-07-16

Initial release supporting basic CRUD operations on Tapis Job resources
as well as Job submission.

1. Zero-install remote job execution that uses SSH to process applications packaged in containers. 
2. Remote job lifecycle support including input staging, job staging, job execution, job monitoring
   and output archiving of user-specified container applications. 
3. Support for running Docker container applications on remote hosts.
4. Support for running Singularity container applications on remote hosts using either
   singularity start or singularity run.
5. Support for running Singularity container applications under Slurm.

### Breaking Changes:
- Initial release.

### New features:
 - Initial release.

### Bug fixes:
- None.
