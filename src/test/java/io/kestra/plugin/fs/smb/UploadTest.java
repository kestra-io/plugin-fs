package io.kestra.plugin.fs.smb;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class UploadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Test
    void upload_destinationIsFolder_shouldRaiseAnIssue() throws Exception {
        final String destinationFolder = "/destinationFolder" + IdUtils.create();

        //Upload 2 files to a remote folder
        uploadFilesToRemoteFolder(destinationFolder);

        //Try to upload one file to destination folder should raise an exception
        URI uri3 = smbUtils.uploadToStorage();

        Upload upload = Upload.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadTest.class.getName())
            .from(Property.ofValue(uri3.toString()))
            .to(Property.ofValue(SmbUtils.SHARE_NAME + destinationFolder))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, upload, Map.of());
        KestraRuntimeException exception = assertThrows(KestraRuntimeException.class, () -> upload.run(runContext));
        assertThat(exception.getMessage(), containsString(destinationFolder));
    }

    @Test
    void upload_destinationIsFolder_overwriteSetToTrue() throws Exception {
        final String parentFolder = "/parentOverwriteFolder" + IdUtils.create();
        final String destinationFolder = "/destinationFolder" + IdUtils.create();
        final String folder = parentFolder + destinationFolder;

        //Upload 2 files to a remote folder
        uploadFilesToRemoteFolder(folder);

        io.kestra.plugin.fs.smb.List listFiles = io.kestra.plugin.fs.smb.List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue(SmbUtils.SHARE_NAME + folder))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var listResult = listFiles.run(TestsUtils.mockRunContext(runContextFactory, listFiles, Map.of()));
        assertThat(listResult.getFiles().size(), is(2));

        for (File file : listResult.getFiles()) {
            assertThat(file.getName(), not(containsString(destinationFolder)));
            assertThat(file.getPath().toString(), containsString(folder));
        }

        //Try to upload one file to destination folder should raise an exception
        URI uri3 = smbUtils.uploadToStorage();

        Upload upload = Upload.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadTest.class.getName())
            .from(Property.ofValue(uri3.toString()))
            .to(Property.ofValue(SmbUtils.SHARE_NAME + folder))
            .host(Property.ofValue("localhost"))
            .overwrite(Property.ofValue(true))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        upload.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        //Folder should be replaced by file
        listResult = listFiles.run(TestsUtils.mockRunContext(runContextFactory, listFiles, Map.of()));
        assertThat(listResult.getFiles().size(), is(1));
        File file = listResult.getFiles().getFirst();
        assertThat(file.getName(), is(destinationFolder.replace("/","")));
        assertThat(file.getPath().toString(), containsString(parentFolder));
    }

    private void uploadFilesToRemoteFolder(String destinationFolder) throws Exception {
        URI uri1 = smbUtils.uploadToStorage();
        URI uri2 = smbUtils.uploadToStorage();

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(Property.ofValue(SmbUtils.SHARE_NAME + destinationFolder))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));
    }
}
