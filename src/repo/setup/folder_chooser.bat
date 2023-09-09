:: fchooser.bat
:: launches a folder chooser and outputs choice to the console
:: https://stackoverflow.com/a/15885133/1683264
:: https://stackoverflow.com/questions/15885132/file-folder-chooser-dialog-from-a-windows-batch-script#answer-15885133
:: https://www.reddit.com/r/Batch/comments/gk6ucx/open_folder_selection_dialog/

@ECHO OFF

REM setlocal
REM set "psCommand="(new-object -COM 'Shell.Application')^
REM .BrowseForFolder(0, 'Please choose a folder.', 0, 0).self.path""
REM for /f "usebackq delims=" %%I in (`powershell %psCommand%`) do set "folder=%%I"
REM setlocal enabledelayedexpansion
REM echo You chose !folder!
REM endlocal

REM preparation command
SET "PScommand="POWERSHELL Add-Type -AssemblyName System.Windows.Forms; $FolderBrowse = New-Object System.Windows.Forms.OpenFileDialog -Property @{ValidateNames = $false;CheckFileExists = $false;RestoreDirectory = $true;FileName = 'Selected Folder';};$null = $FolderBrowse.ShowDialog();$FolderName = Split-Path -Path $FolderBrowse.FileName;Write-Output $FolderName""
REM exec commands powershell and get result in FileName variable
FOR /F "usebackq tokens=*" %%Q in (`%PScommand%`) DO set "folder=%%Q"