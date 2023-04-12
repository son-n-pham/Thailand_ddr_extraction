$WshShell = New-Object -comObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut
$User = $env:USERNAME
$Shortcut = $WshShell.CreateShortcut("C:\Users\$User\Desktop\DDR_Extraction.lnk")
$Shortcut.TargetPath = "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
$Shortcut.Arguments = "-noexit -ExecutionPolicy Bypass -file C:\development\Thailand_ddr_extraction\PowerShell_Scripts\Run_Python_script.ps1"
$Shortcut.Save()

