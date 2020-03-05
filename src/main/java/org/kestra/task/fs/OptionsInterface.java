package org.kestra.task.fs;


import lombok.Getter;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.IOException;


public interface OptionsInterface {

    public void addFsOptions() throws IOException;

}