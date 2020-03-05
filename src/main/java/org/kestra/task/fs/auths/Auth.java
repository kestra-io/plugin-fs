package org.kestra.task.fs.auths;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.File;

//@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type", visible = true, include = JsonTypeInfo.As.EXISTING_PROPERTY)
//@SuperBuilder
@Getter
//@NoArgsConstructor
public class Auth {

    private String username;
    private String password;
    private String keyPath;
    private String passPhrase;

    public Auth(String username, String password, String keyPath, String passPhrase) throws FileSystemException {
        this.username = username;
        this.password = password;
        this.keyPath = keyPath;
    }


    public String getBasicAuth() {
        return !username.equals("") && !password.equals("") ? username + ":" + password + "@" : "";
    }
}