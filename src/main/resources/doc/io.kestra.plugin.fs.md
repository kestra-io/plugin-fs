# How to use the File System plugin

Transfer and manage files over FTP, FTPS, SFTP, SMB, SSH, TCP, UDP, NFS, and the local filesystem from Kestra flows.

## Authentication

**FTP / FTPS / SFTP / SMB**: set `host` (required) and optionally `username` and `password`. Default ports are 21 (FTP), 990 (FTPS), 22 (SFTP), and 445 (SMB).

**SFTP** additionally supports key-based auth — set `keyfile` (PEM private key) and `passphrase`.

**FTPS** additional options: set `mode` to `EXPLICIT` (default) or `IMPLICIT`, and `insecureTrustAllCertificates: true` for self-signed certificates.

**SSH**: set `host`, `username`, and either `password` for password auth or `privateKey` and `privateKeyPassphrase` for key auth. Set `authMethod` to `PASSWORD` (default), `PUBLIC_KEY`, or `OPEN_SSH`.

Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

## Tasks

**FTP / FTPS / SFTP / SMB** — each protocol exposes the same set of operations:

`<protocol>.Download` retrieves a single file — set `from` to the remote path.

`<protocol>.Downloads` retrieves multiple files from a directory — set `from` to the remote directory. Filter with `regExp`. Set `action` to `MOVE` or `DELETE` to handle files after download; set `moveDirectory` when using `MOVE`.

`<protocol>.Upload` writes a file to the remote server — set `from` (a `kestra://` URI) and optionally `to` for the destination path.

`<protocol>.Uploads` uploads multiple files — set `from` as a list or map of URIs and `to` as the destination directory. Cap with `maxFiles` (default 25).

`<protocol>.List` lists a remote directory — set `from`. Filter with `regExp` and set `recursive: true` to traverse subdirectories. Order results with `sort` (`NONE` default, `LAST_MODIFIED_ASC`/`LAST_MODIFIED_DESC`, `NAME_ASC`/`NAME_DESC`) before `maxFiles` truncation is applied.

`<protocol>.Move` renames or moves a remote file — set `from` and `to`.

`<protocol>.Delete` removes a remote file — set `uri`.

`<protocol>.Trigger` polls a remote directory on a schedule and starts one execution per batch of matching files. Set `from`, optionally `regExp`, and `action` to manage files after triggering. Set `sort` to order pending files before `maxFiles` truncation, applied after stateful deduplication.

**SSH** — `ssh.Command` executes one or more shell `commands` (array, required) on a remote host.

**TCP** — `tcp.Send` sends a `payload` to a `host` and `port`. Set `encoding` to `UTF-8` (default) or `BASE64`.

**UDP** — `udp.Send` sends a `payload` to a `host` and `port`.

**Local** — `local.Download`, `local.Upload`, `local.Copy`, `local.Move`, `local.Delete`, `local.List`, `local.Trigger`, and `local.RealtimeTrigger` operate on the local filesystem. Access is restricted to paths configured in `allowed-paths`. `local.List` supports `sort` like the remote `List` tasks.

**NFS** — `nfs.List`, `nfs.Copy`, `nfs.Move`, `nfs.Delete`, `nfs.Trigger`, and `nfs.CheckMount` operate on NFS-mounted paths. Use `nfs.CheckMount` to validate the mount before running file operations. `nfs.List` supports `sort` like the remote `List` tasks.

`local.RealtimeTrigger` watches a local directory for filesystem events (create, modify, delete) using Java NIO WatchService and triggers immediately when matching events occur, without polling. Access is restricted to paths configured in `allowed-paths` under `kestra.plugins.configurations.io.kestra.plugin.fs.local.RealtimeTrigger`. Set `from` to the directory path, optionally `recursive: true` to watch subdirectories, `regExp` to filter by pattern, and `on` to specify event types (`CREATE`, `UPDATE`, `CREATE_OR_UPDATE`, or `DELETE`).

**Important:** UPDATE events are coalesced for each path after 500 ms without a further MODIFY event. CREATE events are emitted immediately and may observe a partial file; with `CREATE_OR_UPDATE`, a newly written file commonly produces a `CREATE` followed by an `UPDATE`. On macOS, WatchService uses a polling-based fallback and may have delayed event detection compared to Linux or Windows.
