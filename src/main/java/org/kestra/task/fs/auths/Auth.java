package org.kestra.task.fs.auths;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.runners.RunContext;
import org.kestra.task.fs.VfsTask;
import org.kestra.task.fs.sftp.SftpDownload;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.File;

@Getter
public class Auth {

    private String username;
    private String password;
    private String keyfile;
    private String passPhrase;
    private String host;
    private String port;

    public Auth(RunContext runContext, VfsTask task) throws IllegalVariableEvaluationException {
        username = runContext.render(task.getUsername());
        password = runContext.render(task.getPassword());
        keyfile = runContext.render(task.getKeyfile());
        passPhrase = runContext.render(task.getPassPhrase());
        host = runContext.render(task.getHost());
        port = runContext.render(task.getPort());
    }

    public String getBasicAuth() {
        return password != null ? username + ":" + password + "@" : username + "@";
    }

    public String getSftpUri(String filepath) {
        return "sftp://" + getBasicAuth() + host + ":" + port + "/" + filepath;
    }
}