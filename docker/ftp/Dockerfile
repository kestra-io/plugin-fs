FROM debian:latest

RUN mkdir -p /home/foo/upload
RUN adduser foo
RUN echo "foo:pass" | chpasswd
RUN chown foo /home/foo/upload
RUN apt update && apt-get install -y vsftpd
RUN sed s/#write_enable=YES/write_enable=YES/ /etc/vsftpd.conf -i
COPY go /usr/bin
ENTRYPOINT [ "/usr/bin/go" ]
