package org.kestra.task.fs;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.*;
import org.kestra.core.models.annotations.Documentation;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Upload file from local FS to SFTP",
    body = "This task reads on local file system a file that will be uploaded to the target SFTP with given inputs"
)
public abstract class VfsDownload extends VfsTask{

    @Override
    protected void doCopy(FileObject remote, FileObject local) throws FileSystemException {
        local.copyFrom(remote, Selectors.SELECT_SELF);
    }

    @Override
    protected VfsTask.Output buildOutput(String remotePath, String localPath) {
        return VfsTask.Output.builder()
            .child(new OutputChild(localPath))
            .build();
    }
}
