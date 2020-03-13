FROM atmoz/sftp

RUN mkdir -p /home/foo/upload
RUN ssh-keygen -f /root/.ssh/id_rsa -q -N "" -P "testPassPhrase"
RUN adduser foo
RUN echo "foo:pass" | chpasswd
RUN cp /root/.ssh/id_rsa /home/foo/upload/id_rsa
RUN mkdir /home/foo/.ssh
RUN echo testSftpFileContent > /home/foo/upload/testSftpSource
RUN chown foo /home/foo/upload -R

