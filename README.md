# Cleanup
This is a cleanup python script

## Usage

Before you start, you have to create an ini file with the following content:
```ini
[general]
threading = True
log_level = info

[./sample]
types = txt log
retention = 4d5h2s
action = delete

[/path/to/my/sample]
types = txt log
retention = 12s
action = archive
```

Each section represents a fileglob where the script searches for the file endings specified.
