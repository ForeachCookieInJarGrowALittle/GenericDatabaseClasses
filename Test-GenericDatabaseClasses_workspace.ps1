#region Setup
. C:\Users\Thiros\Dropbox\WindowsPowerShell\Modules\SQL\GenerischerAnsatz.ps1
if (-not (Test-Path $profile)) {
  new-item $profile -ItemType file
}

function Get-DropBoxRootFolder 
{
  $jsonfile = Join-Path -Path $ENV:LOCALAPPDATA -ChildPath Dropbox\info.json
  if (Test-Path $jsonfile) 
  {
    return Get-Content $jsonfile -ErrorAction Stop |
    ConvertFrom-Json |
    ForEach-Object -MemberName 'personal' |
    ForEach-Object -MemberName 'path'
  }
  else 
  {
    throw 'DropBox Json File wurde nicht gefunden'
  }
}

$DropBoxRootPath=Get-DropBoxRootFolder $DropBoxRootPath

$time = [System.Diagnostics.Stopwatch]::StartNew()

$file=get-item $profile

$secs = $time.Elapsed.TotalSeconds
Write-Output "Getting Item Took $secs"
$time = [System.Diagnostics.Stopwatch]::StartNew()

$info=[Dynamicmasterclass]::Convert($file)


$definition = ([dynamicmasterclass]::new($file)).definition
Invoke-Expression $definition

#([fileinfo]$file).ToString()

$secs = $time.Elapsed.TotalSeconds
Write-Output "Defining Class Took $secs"
$time = [System.Diagnostics.Stopwatch]::StartNew()


$Info=([fileinfo]$file)
$table=$info.GetDataTable()


$secs = $time.Elapsed.TotalSeconds
Write-Output "Defining Class Took $secs"
$time = [System.Diagnostics.Stopwatch]::StartNew()
$filearray=New-Object System.Collections.Arraylist
$newEntries = dir $DropBoxRootPath -Recurse -file
$filearray.AddRange($newEntries)

$secs = $time.Elapsed.TotalSeconds
Write-Output "Retrieving $($newentries.count) Files Took $secs"
$time = [System.Diagnostics.Stopwatch]::StartNew()
$newtable=[sqltable]::new([fileinfo]$newEntries[0])

#endregion
#
# Here we already have $filearray filled with the item we want to add to the Database
#
#region no runspaces involved
$totaltimer = [diagnostics.StopWatch]::StartNew()
if (-not $newtable.Test()) {
  $newtable.Create()
}

$FileInfoArray = New-Object System.Collections.ArrayList
foreach ($file in $newentries) {
  $null = $FileInfoArray.add([Fileinfo]$file)
}


$secs = $time.Elapsed.TotalSeconds
Write-Output "Converting $($Fileinfoarray.count) Files to FileInfoObjects Took $secs"
$time = [System.Diagnostics.Stopwatch]::StartNew()



foreach ( $info in $FileInfoArray) {
  $newrow=$table.NewRow()
  $table.Columns.columnname.
  ForEach{
    if ([String]::IsNullOrEmpty($info.$_)) {
      write-verbose ('{0} is empty {1}' -f $_,$info.$_)
      $newrow.$_="null"
    } else {$newrow.$_ = $info.$_}
  }
  $table.rows.add($newrow)
}


$secs = $time.Elapsed.TotalSeconds
Write-Output "Converting $($newentries.count) FileInfoObjects to DataRow Took $secs"
$time = [System.Diagnostics.Stopwatch]::StartNew()


[sqlserverconnection]::DefaultDataBase.Open()
$sqlbulkcopy= [SqlBulkCopy]::new([sqlserverconnection]::DefaultDatabase)
$null = $table.Columns.columnname.ForEach{$sqlbulkcopy.ColumnMappings.add($_,$_)}
$sqlbulkcopy.DestinationTableName = "FileInfo"

$sqlbulkcopy.WriteToServer($table)
[sqlserverconnection]::DefaultDataBase.close()


$secs = $time.Elapsed.TotalSeconds
Write-Output "Writing to Database took $secs"
$newtable.Drop()
$totaltime = $totaltimer.elapsed.TotalSeconds
Write-Output "Totalseconds taken $totaltime"
#endregion



#region with runspaces
$totaltimer.Restart()
$count = 6
$maxRunspaces= $count * 2 + 1
$time = [Diagnostics.Stopwatch]::StartNew()
# Setup runspace pool and the scriptblock that runs inside each runspace

Function ConvertTo-Customobject {
  Param (
    $InputObjects
    ,
    $definitionfile
  )
  #dot source the file holding the definition
  . $definitionfile
  $outputarray = New-Object System.collections.arraylist
  #invoke-expression $definition
  foreach ($SingleObject in $InputObjects) {
    $null = $outputarray.add([Fileinfo]::new($SingleObject))
  } 
  return $outputarray

}

#Get body of function

$FunctionDefinition = Get-Content Function:\ConvertTo-Customobject -ErrorAction Stop

#Create a sessionstate function entry

$SessionStateFunction = New-Object System.Management.Automation.Runspaces.SessionStateFunctionEntry -ArgumentList 'ConvertTo-Customobject', $FunctionDefinition

#Create a SessionStateFunction

$InitialSessionState = [Initialsessionstate]::CreateDefault()

$InitialSessionState.Commands.Add($SessionStateFunction)

#Create the runspacepool by adding the sessionstate with the custom function
$RunSpacePool = [RunspaceFactory]::CreateRunspacePool(1,$maxRunspaces,$InitialSessionState,$Host)
$RunSpacePool.ApartmentState = "MTA"
$RunSpacePool.Open()
$runspaces = New-Object System.Collections.ArrayList
#region Convert usingrunspaces
# Setup scriptblock. This is the workhorse. Think of it as a function.
<#
$scriptblock = {
  Param (
    $File
    ,
    $definitionfile
  )
  #dot source the file holding the definition
  . $definitionfile
  $outputarray = New-Object System.collections.arraylist
  #invoke-expression $definition
  foreach ($singlefile in $file) {
    $null = $outputarray.add([Fileinfo]$singlefile)
  } 
  return $outputarray
}
#>
$index = 0
$divider = $maxRunspaces * 2
$stepsize = [int]($filearray.count / $divider) 
$definitionfile = "c:\temp\tempdefinition.ps1"
set-content $definitionfile -Value $definition

do {
  $Parameters = @{
    InputObjects = $(
      if (($index + $stepsize) -gt $filearray.count) {
        $end = $filearray.count - $index
      } else {
        $end = $stepsize
      }
      # Write-Verbose -verbose "$index - $end"
      $filearray.GetRange($index,$end)
    )
    Definitionfile = $definitionfile
  }
  $index += $stepsize

  $runspace = [PowerShell]::Create()
  [void]$runspace.AddParameters($Parameters)
  $runspace.RunspacePool = $RunSpacePool
  $Null =$Runspaces.Add([PSCustomObject]@{ Pipe = $runspace; Status = $runspace.BeginInvoke() })

} while ($index -lt $filearray.count)


# Wait for runspaces to complete
# End timer

while ($runspaces.Status.IsCompleted -notcontains $true) {}

[Collections.Arraylist]$fileinfoarray= @(
  foreach ($runspace in $runspaces ) { 
    ($runspace.Pipe.EndInvoke($runspace.Status)) # EndInvoke method retrieves the results of the asynchronous call
    $runspace.Pipe.Dispose()
  }
)

$secs = $time.Elapsed.TotalSeconds
write-verbose -verbose "$($runspaces.count) runspaces completed after $secs"

$RunSpacePool.Close()
$RunSpacePool.Dispose()
$FileInfoArray.count
#endregion
if (-not $newtable.Test()) {
  $newtable.Create()
}

#write using runspaces

$totaltimer.Restart()
$count = 6
$maxRunspaces= $count * 2 + 1
$time = [Diagnostics.Stopwatch]::StartNew()
# Setup runspace pool and the scriptblock that runs inside each runspace
$RunSpacePool2 = [RunspaceFactory]::CreateRunspacePool(1,$maxRunspaces)
$RunSpacePool2.ApartmentState = "MTA"
$RunSpacePool2.Open()
$runspaces2 = New-Object System.Collections.ArrayList
# Setup scriptblock. This is the workhorse. Think of it as a function.

$newscriptblock = {
  Param (
    [string]$connstring,
    [object]$dtbatch,
    [int]$batchsize
  )
   
  $bulkcopy = New-Object Data.SqlClient.SqlBulkCopy($connstring,"TableLock")
  $bulkcopy.DestinationTableName = "allcountries"
  $bulkcopy.BatchSize = $batchsize
  $bulkcopy.WriteToServer($dtbatch)
  $bulkcopy.Close()
  $dtbatch.Clear()
  $bulkcopy.Dispose()
  $dtbatch.Dispose()
}

$connectionstring = [connectionstring]::new("Localhost\SQLExpress","Coremanaged").ConnectionString
$index = 0
$stepsize = [int]($filearray.count / $divider) 

do
{
  if (($index + $stepsize) -gt $fileinfoarray.count) {
    $stepsize = $fileinfoarray.count - $index
  }
  foreach ($info in $FileInfoArray.GetRange($index,$Stepsize)) {
    $newrow=$table.NewRow()
    $table.Columns.columnname.
    ForEach{
      if ([String]::IsNullOrEmpty($info.$_)) {
        write-verbose ('{0} is empty {1}' -f $_,$info.$_)
        $newrow.$_="null"
      } else {$newrow.$_ = $info.$_}
    }
    $table.rows.add($newrow)
  }
  $runspace2 = [PowerShell]::Create()
  [void]$runspace2.AddScript($newscriptblock)
  [void]$runspace2.AddArgument($connectionstring)
  [void]$runspace2.AddArgument("Fileinfo") # <-- Send datatable
  [void]$runspace2.AddArgument($Stepsize)
  $runspace2.RunspacePool = $RunSpacePool2
  $Null =$Runspaces2.Add([PSCustomObject]@{ Pipe = $runspace2; Status = $runspace2.BeginInvoke() })
  # Overwrite object with a shell of itself
  $table = $table.Clone() # <-- Create new datatable object
  $index += $stepsize
} while ($index -lt $fileinfoarray.count)

# Wait for runspaces to complete
# End timer

while ($runspaces2.Status.IsCompleted -notcontains $true) {}

foreach ($runspace in $runspaces2 ) { 
  $null = ($runspace.Pipe.EndInvoke($runspace.Status)) # EndInvoke method retrieves the results of the asynchronous call
  $runspace.Pipe.Dispose()
}

$totaltime = $totaltimer.elapsed.TotalSeconds
Write-Output "Totalseconds taken $totaltime"
$RunSpacePool2.Close()
$RunSpacePool2.Dispose()
[GC]::Collect()
# Cleanup runspaces 
#endregion
