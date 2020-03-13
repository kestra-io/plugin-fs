package org.kestra.task.fs;

public class VfsTaskException extends Throwable {
    public VfsTaskException(String error) {
        System.err.println(error);
    }
}
