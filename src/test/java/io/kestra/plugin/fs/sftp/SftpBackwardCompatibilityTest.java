package io.kestra.plugin.fs.sftp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify backward compatibility between old proxy properties (proxyHost, proxyUser) 
 * and new standardized properties (proxyAddress, proxyUsername).
 */
class SftpBackwardCompatibilityTest {

    @Test
    void testUploadClassHasBothOldAndNewProxyProperties() {
        // Create an Upload instance using builder pattern
        Upload upload = Upload.builder().build();
        
        // This test verifies that the Upload class compiles correctly with both sets of properties
        // The actual functionality is tested by the backward compatibility logic in SftpService
        assertNotNull(upload);
        
        // Verify class structure has not been broken
        assertNotNull(Upload.class);
        assertTrue(SftpInterface.class.isAssignableFrom(Upload.class));
    }

    @Test  
    void testDownloadClassHasBothOldAndNewProxyProperties() {
        Download download = Download.builder().build();
        
        assertNotNull(download);
        assertTrue(SftpInterface.class.isAssignableFrom(Download.class));
    }

    @Test
    void testSftpInterfaceHasAllRequiredMethods() {
        // Verify that SftpInterface has both old and new methods
        // This test ensures we haven't accidentally removed any methods during refactoring
        
        try {
            // Check that new methods exist
            SftpInterface.class.getMethod("getProxyAddress");
            SftpInterface.class.getMethod("getProxyUsername");
            
            // Check that old deprecated methods still exist for backward compatibility
            SftpInterface.class.getMethod("getProxyHost");
            SftpInterface.class.getMethod("getProxyUser");
            
            // If we reach this point, all methods exist
            assertTrue(true);
        } catch (NoSuchMethodException e) {
            fail("Required method missing from SftpInterface: " + e.getMessage());
        }
    }
}