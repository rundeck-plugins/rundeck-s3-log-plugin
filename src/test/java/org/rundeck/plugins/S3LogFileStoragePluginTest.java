package org.rundeck.plugins;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

/**
 * $INTERFACE is ... User: greg Date: 6/11/13 Time: 1:59 PM
 */
@RunWith(JUnit4.class)
public class S3LogFileStoragePluginTest {

    @Test
    public void expandPathLeadingSlashIsRemoved() {
        Assert.assertEquals("monkey", S3LogFileStoragePlugin.expandPath("/monkey", testContext()));
    }

    @Test
    public void expandPathMultiSlashRemoved() {
        Assert.assertEquals("monkey/test/blah", S3LogFileStoragePlugin.expandPath("/monkey//test///blah",
                testContext()));
    }

    @Test
    public void $ID() {
        Assert.assertEquals("monkey/testexecid/blah", S3LogFileStoragePlugin.expandPath("monkey/$ID/blah",
                testContext()));
    }

    @Test
    public void $PROJECT() {
        Assert.assertEquals("monkey/testproject/blah", S3LogFileStoragePlugin.expandPath("monkey/$PROJECT/blah",
                testContext()));
    }

    @Test
    public void missingKey() {
        Assert.assertEquals("monkey/blah", S3LogFileStoragePlugin.expandPath("monkey/$GROUP/blah", testContext()));
    }

    @Test
    public void $GROUP() {
        Assert.assertEquals("monkey/testgroup/blah", S3LogFileStoragePlugin.expandPath("monkey/$GROUP/blah",
                testContext2()));
    }

    @Test
    public void $JOB() {
        Assert.assertEquals("monkey/testname/blah", S3LogFileStoragePlugin.expandPath("monkey/$JOB/blah",
                testContext2()));
    }

    @Test
    public void $JOBID() {
        Assert.assertEquals("monkey/testjobid/blah", S3LogFileStoragePlugin.expandPath("monkey/$JOBID/blah",
                testContext2()));
    }

    @Test
    public void $RUN() {
        Assert.assertEquals("monkey/run/testexecid/blah", S3LogFileStoragePlugin.expandPath
                ("monkey/$GROUP/$JOB/$RUN/$ID/blah", testContext()));
        Assert.assertEquals("monkey/testgroup/testname/testexecid/blah", S3LogFileStoragePlugin.expandPath
                ("monkey/$GROUP/$JOB/$RUN/$ID/blah", testContext2()));
    }

    private HashMap<String, Object> testContext() {
        HashMap<String, Object> stringHashMap = new HashMap<String, Object>();
        stringHashMap.put("execid", "testexecid");
        stringHashMap.put("project", "testproject");
        return stringHashMap;
    }

    private HashMap<String, ?> testContext2() {
        HashMap<String, Object> stringHashMap = testContext();
        stringHashMap.put("id", "testjobid");
        stringHashMap.put("group", "testgroup");
        stringHashMap.put("name", "testname");
        return stringHashMap;
    }

}
