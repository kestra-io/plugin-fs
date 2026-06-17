# How to use the File System plugin

Transfer and manage files over FTP, FTPS, SFTP, SMB, SSH, TCP, UDP, NFS, and the local filesystem from Kestra flows.

## Authentication

**FTP / FTPS / SFTP / SMB**: set `host` (required) and optionally `username` and `password`. Default ports are 21 (FTP), 990 (FTPS), 22 (SFTP), and 445 (SMB).

**SFTP** additionally supports key-based auth — set `keyfile` (PEM private key) and `passphrase`.

**FTPS** additional options: set `mode` to `EXPLICIT` (default) or `IMPLICIT`, and `insecureTrustAllCertificates: true` for self-signed certificates.

**SSH**: set `host`, `username`, and either `password` for password auth or `privateKey` and `privateKeyPassphrase` for key auth. Set `authMethod` to `PASSWORD` (default), `PUBLIC_KEY`, or `OPEN_SSH`.

Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

**FTP / FTPS / SFTP / SMB** — each protocol exposes the same set of operations:

`<protocol>.Download` retrieves a single file — set `from` to the remote path.

`<protocol>.Downloads` retrieves multiple files from a directory — set `from` to the remote directory. Filter with `regExp`. Set `action` to `MOVE` or `DELETE` to handle files after download; set `moveDirectory` when using `MOVE`.

`<protocol>.Upload` writes a file to the remote server — set `from` (a `kestra://` URI) and optionally `to` for the destination path.

`<protocol>.Uploads` uploads multiple files — set `from` as a list or map of URIs and `to` as the destination directory. Cap with `maxFiles` (default 25).

`<protocol>.List` lists a remote directory — set `from`. Filter with `regExp` and set `recursive: true` to traverse subdirectories.

`<protocol>.Move` renames or moves a remote file — set `from` and `to`.

`<protocol>.Delete` removes a remote file — set `uri`.

`<protocol>.Trigger` polls a remote directory on a schedule and starts one execution per batch of matching files. Set `from`, optionally `regExp`, and `action` to manage files after triggering.

**SSH** — `ssh.Command` executes one or more shell `commands` (array, required) on a remote host.

**TCP** — `tcp.Send` sends a `payload` to a `host` and `port`. Set `encoding` to `UTF-8` (default) or `BASE64`.

**UDP** — `udp.Send` sends a `payload` to a `host` and `port`.

**Local** — `local.Download`, `local.Upload`, `local.Copy`, `local.Move`, `local.Delete`, `local.List`, and `local.Trigger` mirror the remote operations but operate on the local filesystem. Access is restricted to paths configured in `allowed-paths`.

**NFS** — `nfs.List`, `nfs.Copy`, `nfs.Move`, `nfs.Delete`, `nfs.Trigger`, and `nfs.CheckMount` operate on NFS-mounted paths. Use `nfs.CheckMount` to validate the mount before running file operations.
