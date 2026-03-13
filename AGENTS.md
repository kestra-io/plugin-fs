# Kestra FS Plugin

## What

Perform file system tasks in Kestra data orchestration pipelines. Exposes 51 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with File system, allowing orchestration of File system-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `fs`

Infrastructure dependencies (Docker Compose services):

- `another-samba-data`
- `ftp`
- `ftps`
- `samba`
- `samba-data`
- `sftp`
- `ssh`

### Key Plugin Classes

- `io.kestra.plugin.fs.ftp.Delete`
- `io.kestra.plugin.fs.ftp.Download`
- `io.kestra.plugin.fs.ftp.Downloads`
- `io.kestra.plugin.fs.ftp.List`
- `io.kestra.plugin.fs.ftp.Move`
- `io.kestra.plugin.fs.ftp.Trigger`
- `io.kestra.plugin.fs.ftp.Upload`
- `io.kestra.plugin.fs.ftp.Uploads`
- `io.kestra.plugin.fs.ftps.Delete`
- `io.kestra.plugin.fs.ftps.Download`
- `io.kestra.plugin.fs.ftps.Downloads`
- `io.kestra.plugin.fs.ftps.List`
- `io.kestra.plugin.fs.ftps.Move`
- `io.kestra.plugin.fs.ftps.Trigger`
- `io.kestra.plugin.fs.ftps.Upload`
- `io.kestra.plugin.fs.ftps.Uploads`
- `io.kestra.plugin.fs.local.Copy`
- `io.kestra.plugin.fs.local.Delete`
- `io.kestra.plugin.fs.local.Download`
- `io.kestra.plugin.fs.local.Downloads`
- `io.kestra.plugin.fs.local.List`
- `io.kestra.plugin.fs.local.Move`
- `io.kestra.plugin.fs.local.Trigger`
- `io.kestra.plugin.fs.local.Upload`
- `io.kestra.plugin.fs.nfs.CheckMount`
- `io.kestra.plugin.fs.nfs.Copy`
- `io.kestra.plugin.fs.nfs.Delete`
- `io.kestra.plugin.fs.nfs.List`
- `io.kestra.plugin.fs.nfs.Move`
- `io.kestra.plugin.fs.nfs.Trigger`
- `io.kestra.plugin.fs.sftp.Delete`
- `io.kestra.plugin.fs.sftp.Download`
- `io.kestra.plugin.fs.sftp.Downloads`
- `io.kestra.plugin.fs.sftp.List`
- `io.kestra.plugin.fs.sftp.Move`
- `io.kestra.plugin.fs.sftp.Trigger`
- `io.kestra.plugin.fs.sftp.Upload`
- `io.kestra.plugin.fs.sftp.Uploads`
- `io.kestra.plugin.fs.smb.Delete`
- `io.kestra.plugin.fs.smb.Download`
- `io.kestra.plugin.fs.smb.Downloads`
- `io.kestra.plugin.fs.smb.List`
- `io.kestra.plugin.fs.smb.Move`
- `io.kestra.plugin.fs.smb.Trigger`
- `io.kestra.plugin.fs.smb.Upload`
- `io.kestra.plugin.fs.smb.Uploads`
- `io.kestra.plugin.fs.ssh.Command`
- `io.kestra.plugin.fs.tcp.RealtimeTrigger`
- `io.kestra.plugin.fs.tcp.Send`
- `io.kestra.plugin.fs.udp.RealtimeTrigger`
- `io.kestra.plugin.fs.udp.Send`

### Project Structure

```
plugin-fs/
├── src/main/java/io/kestra/plugin/fs/vfs/
├── src/test/java/io/kestra/plugin/fs/vfs/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
