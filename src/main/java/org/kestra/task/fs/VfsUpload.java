package org.kestra.task.fs;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class VfsUpload extends VfsTask {
    @Override
    protected void doCopy(FileObject remote, FileObject local) throws FileSystemException {
        remote.copyFrom(local, Selectors.SELECT_SELF);
    }

    @Override
    protected VfsTask.Output buildOutput(String remotePath, String localPath) {
        return VfsTask.Output.builder()
            .child(new OutputChild(remotePath))
            .build();
    }
}
