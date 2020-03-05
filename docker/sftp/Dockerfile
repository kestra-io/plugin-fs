FROM atmoz/sftp

RUN mkdir -p /home/foo/upload
RUN adduser foo
RUN echo "foo:pass" | chpasswd
RUN chown foo /home/foo/upload
