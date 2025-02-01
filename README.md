# vscode_manager

`vscode_manager`: launch and manage VS Code HPC jobs

`vscode.py` controls the creation, deletion, and listing of `code-server` sessions
for the user, which are launched from installed Lmod modules

Three subcommands are exposed to the user:

- `vscode start`
  Launches an VS Code server session, returning the URL needed to connect

- `vscode stop`
  Graceful shutdown of existing sessions

- `vscode list`
  Lists the user's active sessions and their URLs
