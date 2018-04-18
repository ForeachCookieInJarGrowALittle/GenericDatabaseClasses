#requires -Version 5.0
using namespace System.Data.SqlClient
using namespace System.Data


#Available DataTypes for SQL Conversion
enum SQLDataType { 
  nvarchar
  datetime2
  bigint
  bit
  int
  varchar
  xml
}

enum DefaultSQLDataTypeStrings { 
  nvarchar
  datetime2
  bigint
  bit
  int
  varchar
  xml
}

#Converts an Object to Hashtable for use with Databases
class ObjectConverter {
  static [HashTable] GetPropertyValueHashTable([object]$Object) {
    $members=(Get-Member -MemberType Properties -inputobject $Object)
    $StringComparer = [StringComparer]::InvariantCultureIgnoreCase
    $HashTable      = [Collections.Hashtable]::new($members.count,$StringComparer)  
    $members.Foreach{
      $HashTable.Add($_.Name,$Object.($_.Name))
    }
    return $HashTable
  }
}

#Converts an Object to an Instance of a generic-Class
Class DBConvert {
  static [SQLClass] ToSQLClass([DataRow]$DataRow) {
    return [SQLClass]::new()
  }
  static [Collections.ArrayList] ToSQLClass([Object[]]$InputObjects) {
    $InputArray = [Collections.Arraylist]::new()
    $InputArray.AddRange($InputObjects)
    write-verbose -verbose $InputArray.count
    $SQLClassInstance = [Dynamicmasterclass]::Convert($InputArray[0])
    $TypeName = $SQLClassInstance.GetType().fullname
    $definition = ([dynamicmasterclass]::new($InputArray[0])).StandAloneDefinition
    
    #region with runspaces
    $NumberOfLogicalProcessors = (Get-WmiObject -query "select NumberOfLogicalProcessors from Win32_processor").NumberOfLogicalProcessors
    $maxRunspaces= $NumberOfLogicalProcessors + 1
    # Setup runspace pool and the scriptblock that runs inside each runspace
    $SessionState = [InitialSessionState]::CreateDefault()
    #$SessionState.StartupScripts="$PSScriptRoot\GenerischerAnsatz.ps1"
    $RunSpacePool = [RunspaceFactory]::CreateRunspacePool(1,$maxRunspaces,$SessionState,(get-host))
    $RunSpacePool.ApartmentState = "MTA"
    $RunSpacePool.Open()
    $runspaces = New-Object System.Collections.ArrayList
    #region Convert usingrunspaces
    # Setup scriptblock. This is the workhorse. Think of it as a function.
    $script=@"
{
  Param (
  `$InputObjects
  )
  $($Definition -replace ":SQLCLASS","")
  `$OutputArray = New-Object System.collections.arraylist
  foreach (`$SingleObject in `$InputObjects) {
  `$null = `$OutputArray.add([$Typename]`$SingleObject)
  } 
  return `$OutputArray
}
"@
    [scriptblock]$scriptBlock = Invoke-Expression $script
    $index = 0
    $divider = $maxRunspaces * 2
    $stepsize = [int]($InputArray.count / $divider) 

    do {
      $Parameters = @{
        InputObjects = $(
          if (($index + $stepsize) -gt $InputArray.count) {
            $end = $InputArray.count - $index
          } else {
            $end = $stepsize
          }
          # Write-Verbose -verbose "$index - $end"
          $InputArray.GetRange($index,$end)
        )
      }
      $index += $stepsize

      $runspace = [PowerShell]::Create()
      [void]$runspace.AddScript($scriptBlock)
      [void]$runspace.AddParameters($Parameters)
      $runspace.RunspacePool = $RunSpacePool
      $Null =$Runspaces.Add([PSCustomObject]@{ Pipe = $runspace; Status = $runspace.BeginInvoke() })

    } while ($index -lt $InputArray.count)

    while ($runspaces.Status.IsCompleted -notcontains $true) {}
    
    [Collections.Arraylist]$SQLClassArray= @(
      foreach ($runspace in $runspaces ) { 
        ($runspace.Pipe.EndInvoke($runspace.Status)) # EndInvoke method retrieves the results of the asynchronous call
        $runspace.Pipe.Dispose()
      }
    )
    $RunSpacePool.Close()
    $RunSpacePool.Dispose()
    Return $SQLClassArray
    #endregion
  }
}

#Convert any given object to a SQLClass descendant
Class DynamicMasterClass {
  [String] $Definition
  [String] $ClassName
  [String] $StandAloneDefinition
  
  Static [object] Convert($object) {
    $newinstance = [DynamicMasterClass]$object
    
    Invoke-Expression $newInstance.Definition
    return Invoke-Expression "[$($newInstance.ClassName)]`$object"
  }

  DynamicMasterClass([object] $object) {
       
    if ($object -is [object[]]) {
      #$Properties=[ObjectConverter]::GetPropertyValueHashTable($object[0])
      $this.ClassName = $object[1]
      #Input Type for the generated Classes Constructor
      $FullClassName = "object"
    } 
    else {
      #$Properties=[ObjectConverter]::GetPropertyValueHashTable($object)
      $this.ClassName = $object.GetType().Name
      $FullClassName = $object.GetType().FullName
    }
    
    $Constructor        = [Text.StringBuilder]::New()
    $ClassDefinition    = [Text.StringBuilder]::New()
    $GetDataTableMethod = [Text.StringBuilder]::New()
    $ToStringMethod     = [Text.StringBuilder]::New()
    $FormatInstructions = [Text.StringBuilder]::New()
    
    $ConvertableDefinition = [Text.StringBuilder]::New()
    
    <# 
        $GetDataTableMethod = [DataTable]GetDataTable() {
        $datatable = new-object Data.datatable  
     
    #>
    $null = $ClassDefinition.AppendFormat('Class {0}:SQLClass ',$this.ClassName).Append('{').Append([Environment]::NewLine)
    $null = $Constructor.AppendFormat('  {0}([{1}]$object) ',$this.ClassName,$FullClassName).Append('{').Append([Environment]::NewLine)
    $null = $GetDataTableMethod.AppendLine('  [DataTable] GetDataTable() {')
    $null = $GetDataTableMethod.AppendFormat('    $datatable = [Data.datatable]::New("{0}")', $this.ClassName).Append([Environment]::NewLine)
    $null = $ToStringMethod.AppendLine('  [String] ToString() {').Append('    return ("')
    $null = $FormatInstructions.Append(' -f ')
    $needscomma = $false
    
    $types = @(
      'System.String',
      'System.Boolean',
      'System.Byte[]',
      'System.Byte',
      'System.Char',
      'System.Datetime',
      'System.Decimal',
      'System.Double',
      'System.Guid',
      'System.Int16',
      'System.Int32',
      'System.Int64',
      'System.Single',
      'System.UInt16',
      'System.UInt32',
      'System.UInt64'
    )
    $index=0
    foreach ($Property in $object.PSObject.Get_Properties()) {
      $Name=$property.Name.ToString()  
      $LongTypeName = $Property.TypeNameOfValue
      
      if ($Types.IndexOf( $LongTypeName) -eq -1) {
        $LongTypeName = 'System.String'
      }
      $ShortTypeName  = $LongTypeName.Split('\.')[1]

      $null = $ClassDefinition.AppendFormat('  [{0}] ${1}',$ShortTypeName,$Name).
      Append([Environment]::NewLine)
      if ($needscomma) {
        $null = $ToStringMethod.Append('|')
        $null = $FormatInstructions.Append(',')
      } else {
        $needscomma = $true
      }  
      
      $null = $ToStringMethod.Append('{').AppendFormat('{0}',$index).Append('}')
      $index++
      switch ($ShortTypeName) {
        'Boolean' {
          
          $null = $FormatInstructions.AppendFormat('$(if ($this.{0}) ',$Name).Append('{"1"} else {"0"})')
        }
        default {
          $null = $FormatInstructions.AppendFormat('$this.{0}',$Name)
        }
      }
       

      #if value cannot be casted to String, convert to xml
      if ($ShortTypeName -eq 'String' -and $Property.TypeNameofValue -ne 'System.String') {
        if (($Property.value -as [string]) -eq $null -or ($Property.value -as [string]) -eq $Property.TypeNameOfValue) {
          $null = $constructor.AppendFormat('    $This.{0} = ($object.{0}| ConvertTo-XML -AS String -NoTypeInformation -Depth 1)',$name)
        } else {
          $null = $Constructor.AppendFormat('    $This.{0} = $object.{0}',$Name)
        }
      } else {
        $null = $Constructor.AppendFormat('    $This.{0} = ($object.{0} -as [String])',$Name)
      }  
      $null = $Constructor.Append([Environment]::NewLine)
      
      $null = $GetDataTableMethod.AppendFormat('    $datatable.Columns.Add([DataColumn]::new("{0}",[Type]::GetType("{1}")))',$Name,$LongTypeName).Append([Environment]::NewLine) 
    }
    
    $null = $Constructor.AppendLine('  }')
    $null = $GetDataTableMethod.AppendLine('    return $datatable').AppendLine('  }')
    $null = $ToStringMethod.Append('"').Append($FormatInstructions.ToString()).Append(')').AppendLine('  }')

    $null = $ConvertableDefinition.Append($ClassDefinition.ToString()).
    Append([Environment]::NewLine).
    Append($Constructor.ToString()).
    Appendline('}')
    
    $this.StandAloneDefinition = $ConvertableDefinition.ToString()
    
    $null = $ClassDefinition.Append([Environment]::NewLine).
    Append($Constructor.ToString()).
    Append([Environment]::NewLine).
    Append($GetDataTableMethod.ToString()).
    Append([Environment]::NewLine).
    Append($ToStringMethod.ToString()).
    AppendLine('}')
   
    $This.Definition = $ClassDefinition.ToString()
  }  
  
  #blank Constructor only to show the overload
  DynamicMasterClass([object] $object,[String]$ClassName) {}
 
}

#noch nicht fertig
Class SQLEnum {
  
}

#noch nicht fertig
Class SQLClassDefinition {
  [String]   $ClassName
  [String[]] $PropertySet
  [String]   $Definition
  
  SQLClassDefinition([String]$ClassName) {
    ([SQLQuery]("Select ClassName,PropertySet,Definition from ClassDefinitions where Classname = '$ClassName'",[SQLServerConnection]::DefaultDataBase)).ExecuteQuery()
  }
  
  SQLClassDefinition([DataRow[]]$DataRow) {
    
  }
  
  Static [SQLClassDefinition[]] GetClassDefinitions() {
    return [SQLCLassDefinition]::new("Test")
  }
  
  [void] Invoke() {
    . $this.Definition
  }
}

Class SQLClass {
  hidden [string[]]  $PrimaryKey
  hidden [string[]]  $Indizes
  hidden [string]    $TableName
  hidden [Hashtable] $Properties     
  #region Konstruktoren  
  
  SQLClass () {}
  
  SQLClass ([object] $object) {
    if ($object -is [Object[]]) {
      $this.Properties = [ObjectConverter]::GetPropertyValueHashTable($object[0])
      
      switch ($object.count) {
        2 {
          $this.PrimaryKey = $Object[-1]
          $this.Indizes    = 'None'

        }
        3 {
          $this.PrimaryKey = $Object[-2]
          $this.Indizes    = $Object[-1]
        }
        default {throw "This Should never happen"}
      }
    }
    else {
      $this.Properties = [ObjectConverter]::GetPropertyValueHashTable($object)
      $this.PrimaryKey = 'UseIdentity'
      $this.Indizes    = 'None'
    }
    if (($this.Indizes| where {!$this.Properties.ContainsKey($_)}) -and $this.Indizes -ne 'None')
    {
      throw ('Indizes {1} Out of Range!`nValid Fields would be {0}' -f $($this.Properties.Keys -join ','),$($this.Indizes))
    }
    if (($this.PrimaryKey|where {!$this.Properties.ContainsKey($_)}) -and $this.PrimaryKey -ne 'UseIdentity') {
      throw ('`nPrimarykey {1} Out of Range!`nValid Fields would be {0}' -f $($this.Properties.Keys -join ','),$($this.Primarykey))
    }
  }
  
  #Diese Überladungen werden zwar nie aufgerufen, aber sie führen zu einer richtigen Anzeige
  SQLClass ([object] $object, [String[]] $PrimaryKey) {
    #$this.PrimaryKey = $PrimaryKey
    #$this.Indizes    = 'None'
    #$this.Properties = [ObjectConverter]::GetPropertyValueHashTable($object)
    
  }  

  SQLClass ([object] $object, [String[]] $PrimaryKey, [String[]] $Indizes) {
    #$this.PrimaryKey = $PrimaryKey
    #$this.Indizes    = $Indizes
    #$this.Properties = [ObjectConverter]::GetPropertyValueHashTable($object)
    
  }
  #endregion
  
  [SQLColumn[]] ConvertToSQLColumn() {
    $columns=[collections.Arraylist]::new()
    foreach ($Name in $this.properties.keys)
    {
      $column = [SQLColumn]::New($Name)
      $value = ($this.Properties."$Name").GetType().Name
      switch ($value) {
        'DateTime' {$column.DataType = [SQLDataType]::datetime2.ToString()}
        'Boolean' {$column.DataType = [SQLDataType]::bit.ToString()}
        default   {$column.DataType = [SQLDataType]::nvarchar.ToString()}
      }
      if ($column.datatype -match "char")
      {
        $column.TypeDataString = '{0}(MAX)' -f $column.DataType     
      }
      else {
        $column.TypeDataString = $column.DataType
      }
      $null = $columns.add($column)
    }
    return $columns
  }
   
  [void] GetProperties() {
    $HashTable = [objectconverter]::GetPropertyValueHashTable($this)
    $HashTable.Remove('PrimaryKey')
    $HashTable.Remove('Properties')
    $HashTable.Remove('TableName')
    $HashTable.Remove('Indizes')
    $this.Properties = $HashTable
  }
  
  [void] Edit() {}
}

Class ConnectionString {
  [string] $Server = "Localhost\SQLEXPRESS"
  [int]    $Port        
  [string] $DataBase   
  [bool]   $Trusted = $true
  [string] $ConnectionString
  
  ConnectionString () {
    $this.ConnectionString = "Server=$($this.Server);Trusted_Connection=$($this.Trusted);"
  }
  
  ConnectionString ([string]$Server) {
    $this.Server       = $Server
    $this.ConnectionString = "Server=$($this.Server);Trusted_Connection=$($this.Trusted);"
  }
  
  ConnectionString ([string]$Server,[int]$Port) {
    $this.Server       = $Server
    $this.ConnectionString = "Server=$($this.Server),$($this.Port);Trusted_Connection=$($this.Trusted);"
  }

  ConnectionString ([string]$Server,[String]$DataBase) {
    $this.Server       = $Server
    $this.DataBase         = $Database
    $this.ConnectionString = "Server=$($this.Server);Database=$($this.Database);Trusted_Connection=$($this.Trusted);"
  }
  
  ConnectionString ([string]$Server,[int]$Port,[String]$DataBase) {
    $this.Server       = $Server
    $this.DataBase         = $Database
    $this.ConnectionString = "Server=$($this.ServerName),$($this.Port);Database=$($this.Database);Trusted_Connection=$($this.Trusted);"
  }
  
  [String] ToString() {
    return $this.ConnectionString
  }
  
}

Class SQLServerConnection {
  static [SqlConnection] $DefaultDataBase   = [SQLConnection]::New([ConnectionString]::New("Localhost\SQLEXPRESS","Coremanaged").ConnectionString)
  static [SqlConnection] $DefaultServer = [SQLConnection]::New([ConnectionString]::New("Localhost\SQLEXPRESS").ConnectionString)
  static [BOOL] $IsSQLInstalled = $(
    if (Get-ItemProperty 'HKLM:\Software\Microsoft\Microsoft SQL Server\Instance Names\SQL') {
      $true
    } else {
      $false
    }
  )
  static [bool] SetDefaultServerConnection() {
    if ([SQLServerConnection]::IsSQLInstalled) {
      [SQLServerConnection]::DefaultDataBase = [SQLConnection]::new([ConnectionString]::new("Localhost\$((get-item 'HKLM:\Software\Microsoft\Microsoft SQL Server\Instance Names\SQL')[0].property)"))
      return $true
    } else {
      Write-Warning "Cannot set DefaultServerConnection, because SQL is not installed on localhost"
      return $false
    }
  }
  static [BOOL] Test() {
    try {
      [SQLServerConnection]::DefaultServer.open()
      return $true
    } catch {
      return $false
    }
  }
  #Usage: [SQLServerConnection]::Connect([ConnectionString]::new('Localhost\SQLEXPRESS','Pester'))
  static [void] Connect([String]$ConnectionString) {
    [SQLServerConnection]::DefaultDatabase = [SQLConnection]::New($ConnectionString)
    [SQLServerConnection]::DefaultServer = [SQLConnection]::New(($ConnectionString -replace ';Database=[^;]{1,}',''))
  }
  
  static [String] ToString() {
    return 'Currently Connected to {0}' -f [SqlServerConnection]::DefaultServer.DataSource
  }  
}
#fertig
Class SQLObject {
  [SQLConnection]$Connection
  
  [void] ExecuteNonQuery([String]$Query) {
    [SQLQuery]::New($Query,$this.Connection).ExecuteNonQuery()
  }
  
  [DataRow[]] ExecuteQuery([String]$Query) {
    return [SQLQuery]::New($Query,$this.Connection).ExecuteQuery()
  }
}
#fertig  
Class SQLQuery {
  [String]$Query
  [SQLConnection]$Connection = [SQLServerConnection]::DefaultServer
  
  SQLQuery([String]$Query) {
    $this.Query = $Query
  }
  
  SQLQuery([String]$Query,[String]$ConnectionString) {
    $this.Connection = [SQLConnection]::New($ConnectionString)
    $this.Query = $Query
  }
  
  SQLQuery([String]$Query,[SQLConnection]$Connection) {
    $this.Connection = $Connection
    $this.Query = $Query
  }
  
  [String] ToString() {return $this.Query}
  
  [void] ExecuteNonQuery() {
    Try {
      $null = $this.Connection.Open()
    }
    catch {
      Write-Host -ForegroundColor Magenta "The Query failed with the following excuse:`n$($Error[0].Exception.InnerException)"
      throw
    }
    try {
      $command=[SqlCommand]::New($this.Query,$this.Connection)
      $null = $command.ExecuteNonQuery()
    } catch [Data.SqlClient.SqlException] {
      Write-Host -ForegroundColor Magenta "The Query failed with the following excuse:`n$($Error[0].Exception.InnerException)"
      throw
    }
    finally {
      $this.Connection.close()
    }
  }
  
  [DataRow[]] ExecuteQuery() {
    Try {
      $null = $this.Connection.Open()
    }
    catch {
      Write-Host -ForegroundColor Magenta "The Query failed with the following excuse:`n$($Error[0].Exception.InnerException)"
      throw
    }
    try {
      $adapter=[SqlDataAdapter]::new($this.Query,$this.Connection)
      
      $dataset = [DataSet]::new()
      $null = $adapter.Fill($dataset)
      return $dataset.Tables[0].Rows
    }
    catch [Data.SqlClient.SqlException] {
      Write-Host -ForegroundColor Magenta "The Query failed with the following excuse:`n$($Error[0].Exception.InnerException)"
      throw
    }
    finally {
      $this.Connection.close()
    }
  }
}
#fertig
Class SQLDatabase:SQLObject {
  [String]$Name
  [SqlConnection]$Connection = [SQLServerConnection]::DefaultServer
  
  SQLDatabase ([String]$Name) {
    $this.Name = $Name
  }
  
  SQLDatabase ([String]$Name,[String]$ConnectionString) {
    $this.Name = $Name
    $this.Connection = [SQLServerConnection]::New($ConnectionString)
  }
  
  [Bool] Test() {
    if (($this.ExecuteQuery("SELECT * FROM sys.sysdatabases where Name = '$($this.Name)'"))) {
      return $true
    } else {
      return $false
    }
  }
  
  [void] Create() {
    $this.ExecuteNonQuery("Create Database [$($this.Name)]")
  }
  
  [void] Drop() {
    $this.ExecuteNonQuery("ALTER DATABASE [$($this.Name)] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;DROP DATABASE [$($this.Name)]")
  }
  
  [String] ToString() {
    return '{0}' -f $this.Name
  }
}

Class SQLColumn:SQLObject {
  [String] $TypeDataString
  [String] $Database
  [String] $Schema
  [String] $TableName
  [String] $ColumnName
  [Int]    $OrdinalPosition
  [String] $DefaultValue
  [Bool]   $nullable
  [String] $DataType
  [Int]    $MaxLength
  [String] $PrimaryKey
  
  SQLColumn([String]$ColumName) {
    $this.ColumnName = $ColumName
  }
  
  SQLColumn([DataRow]$DataRow) {
    #SQLColumn ([DataRow]$DataRow) {
    $this.TypeDataString  = $DataRow.TypeDataString  #-as [String]
    $this.Database        = $DataRow.Database        -as [String]
    $this.Schema          = $DataRow.Schema          -as [String]
    $this.TableName       = $DataRow.TableName       -as [String]
    $this.ColumnName      = $DataRow.ColumnName      -as [String]
    $this.OrdinalPosition = $DataRow.OrdinalPosition -as [Int]
    $this.DefaultValue    = $DataRow.DefaultValue    -as [String]
    $this.nullable        = ($DataRow.nullable -eq "Yes")
    $this.DataType        = $DataRow.DataType        -as [String]
    $this.MaxLength       = $DataRow.MaxLength       -as [Int]
    $this.PrimaryKey      = $DataRow.PrimaryKey      -as [String]
  }
 
  SQLColumn([object]$DataRow) {
    #SQLColumn ([DataRow]$DataRow) {
    $this.TypeDataString  = $DataRow.TypeDataString  #-as [String]
    $this.Database        = $DataRow.Database        -as [String]
    $this.Schema          = $DataRow.Schema          -as [String]
    $this.TableName       = $DataRow.TableName       -as [String]
    $this.ColumnName      = $DataRow.ColumnName      -as [String]
    $this.nullable        = $true
    $this.DataType        = $Datarow.DataType        -as [String]
    $this.MaxLength       = $DataRow.MaxLength       -as [Int]
    $this.PrimaryKey      = $DataRow.PrimaryKey      -as [String]
  }
  
  [void] Connect() {
    if ($null -eq $this.Connection) {
      $this.Connection = [ConnectionString]::New($this.Database)
    }
  }
  
  [void] SetDefaultValue() {
    
    $query= "ALTER TABLE [{0}].[{1}].[{2}] ADD  DEFAULT [{3}] FOR [{4}]`n" -f $this.Database,$this.Schema,$this.TableName,$this.DefaultValue,$this.ColumnName
    $this.Connect().ExecuteNonQuery($Query)
  }
  
  [void] SetDefaultValue([String]$DefaultValue) {
    $this.DefaultValue = $DefaultValue
    $query= "ALTER TABLE [{0}].[{1}].[{2}] ADD  DEFAULT [{3}] FOR [{4}]`n" -f $this.Database,$this.Schema,$this.TableName,$this.DefaultValue,$this.ColumnName
    $this.Connect().ExecuteNonQuery($Query)
  }
  
  [void] Update([SQLColumn]$SQLColumn) {
    $Query='ALTER TABLE [{0}].[{1}].[{2}] ALTER COLUMN [{3}] {4} {5}' -f $this.Database,$this.Schema,$this.TableName,$this.ColumnName,$this.DataType,(
      if($this.nullable) {'NULL'} else {'NOT NULL'}
    )
    $this.Connect().ExecuteNonQuery($Query)
  }
  
  [String] ToString() {
    return '{0}' -f $this.ColumnName
  }
}

Class SQLTable:SQLObject {
  [String]$Name
  [String]$Schema = "dbo"
  [SqlConnection]$Connection = [SQLServerConnection]::DefaultDatabase
  [SQLColumn[]]$Columns
  
  
  SQLTable([String]$TableName) {
    $this.Name = $TableName
  }
  
  SQLTable([SQLClass]$SQLClass) {
    $this.Name = $SQLClass.gettype().Name
    $SQLClass.GetProperties()
    $this.Columns = $SQLClass.ConvertToSQLColumn()
  }
  
  SQLTable([String]$TableName,[String]$ConnectionString) {
    $this.Connection = [SQLServerConnection]::New($ConnectionString)
    $this.Name = $TableName
  }
  
  SQLTable([SQLColumn[]]$ColumnDefinition) {
    $this.Name = $ColumnDefinition[0].TableName
    $this.Columns = $ColumnDefinition
  }
  
  SQLTable([SQLColumn[]]$ColumnDefinition,$ConnectionString) {
    $this.Name = $ColumnDefinition[0].TableName
    $this.Connection = [SQLServerConnection]::New($ConnectionString)
    $this.Columns = $ColumnDefinition
  }
  
  [SQLColumn[]] GetColumns() {
    $Query=@"
SELECT '[' + data_type + ']' +
    case
        when data_type like '%text' or data_type in ('image', 'sql_variant' ,'xml')
            then ''
        when data_type in ('float')
            then '(' + cast(coalesce(numeric_precision, 18) as varchar(11)) + ')'
        when data_type in ('datetime2', 'datetimeoffset', 'time')
            then '(' + cast(coalesce(datetime_precision, 7) as varchar(11)) + ')'
        when data_type in ('decimal', 'numeric')
            then '(' + cast(coalesce(numeric_precision, 18) as varchar(11)) + ',' + cast(coalesce(numeric_scale, 0) as varchar(11)) + ')'
        when (data_type like '%binary' or data_type like '%char') and character_maximum_length = -1
            then '(max)'
        when character_maximum_length is not null
            then '(' + cast(character_maximum_length as varchar(11)) + ')'
        else ''
    end                       as 'TypeDataString'
	,c.[TABLE_CATALOG]          as 'Database'
  ,c.[TABLE_SCHEMA]           as 'Schema'
  ,c.[TABLE_NAME]             as 'TableName'
  ,c.[COLUMN_NAME]            as 'ColumnName'
  ,c.[ORDINAL_POSITION]       as 'OrdinalPosition'
  ,[COLUMN_DEFAULT]           as 'DefaultValue'
  ,[IS_NULLABLE]              as 'nullable'
  ,[DATA_TYPE]                as 'DataType'
  ,[CHARACTER_MAXIMUM_LENGTH] as 'MaxLength'
  ,k.CONSTRAINT_NAME          as 'PrimaryKey'
  --,[CHARACTER_OCTET_LENGTH]
  --,[NUMERIC_PRECISION]
  --,[NUMERIC_PRECISION_RADIX]
  --,[NUMERIC_SCALE]
  --,[DATETIME_PRECISION]
  --,[CHARACTER_SET_CATALOG]
  --,[CHARACTER_SET_SCHEMA]
  --,[CHARACTER_SET_NAME]
  --,[COLLATION_CATALOG]
  --,[COLLATION_SCHEMA]
  --,[COLLATION_NAME]
  --,[DOMAIN_CATALOG]
  --,[DOMAIN_SCHEMA]
  --,[DOMAIN_NAME]
	  
  FROM [INFORMATION_SCHEMA].[COLUMNS] c
  left join [information_schema].[KEY_COLUMN_USAGE] k
  on c.Table_Catalog = k.CONSTRAINT_CATALOG and c.TABLE_NAME = k.TABLE_Name and c.TABLE_SCHEMA = k.TABLE_Schema and c.COLUMN_NAME = k.COLUMN_NAME
  where c.table_name like '%$($this.Name)'
  order by c.table_schema, c.table_name, c.ordinal_position
"@
    $this.Columns = $this.ExecuteQuery($query)
    return $this.Columns
  }
  
  [Void] Create() {
    $this.ExecuteQuery(('CREATE TABLE [{0}].[{1}] (id_{1} int not null identity(1,1) constraint pk_AutoGen{1} primary key)' -f $this.Schema,$this.Name))    
    if ($this.Columns) {
      foreach ($column in $this.Columns) {
        if ($column.ColumnName -ne ('id_{0}' -f $this.name)) {
          $column.Database = $this.Connection.Database
          $column.Schema = $this.Schema
          $column.TableName = $this.Name
          $this.AddColumn($column)
        }
      }
    }
    if ($KeyColumns=$this.Columns|where {$_.PrimaryKey}) {
      $this.SetPrimaryKey($KeyColumns)
    }
    if ($DefaulValues=$this.Columns|where {$_.DefaultValue}) {
      foreach ($DefaulValue in $DefaulValues) {
        $DefaulValue.SetDefaultValue()
      }
    }
    
  }
  
  [Void] Drop() {
    $this.ExecuteNonQuery(('DROP TABLE [{0}].[{1}]' -f $this.Schema,$this.Name))
  }
  
  [void] AddColumn([SQLColumn]$SQLColumn) {
    $Query='ALTER TABLE [{0}].[{1}].[{2}] ADD [{3}] {4}' -f $SQLColumn.Database,$SQLColumn.Schema,$SQLColumn.TableName,$SQLColumn.ColumnName,$SQLColumn.TypeDataString
    $this.ExecuteNonQuery($Query)
  }
  
  [void] RemoveColumn([SQLColumn]$SQLColumn) {
    $Query='ALTER TABLE [{0}].[{1}].[{2}] DROP COLUMN [{3}]' -f $SQLColumn.Database,$SQLColumn.Schema,$SQLColumn.TableName,$SQLColumn.ColumnName
    $this.ExecuteNonQuery($Query)
  }
  
  [SQLColumn[]] GetPrimaryKeyColumns() {
    return $this.Columns.where{-not [string]::IsNullOrEmpty($_.PrimaryKey)}
  }
  
  [Bool] SetPrimaryKey([SQLColumn[]]$KeyColumns) {
    if ($keys=$this.GetPrimaryKeyColumns()) {
      if ($keys.PrimaryKey -match "AutoGen") {
        $this.DropPrimaryKey()
        $this.RemoveColumn($keys)
      } else {
        return $false
      }
    } 
    $MaxNVarCharLength=450/$KeyColumns.Count
    $KeyColumns.where{$_.TypeData -like '*char*'}.ForEach{
      $_.Length = $_.Length/3
      $_.TypeDataString = $_.TypeDataString -replace "MAX","$MaxNVarCharLength"
      $_.nullable = $false
      $_.Update()
    }
    
    $query=@"
ALTER TABLE [{0}].[{1}].[{2}]
ADD CONSTRAINT pk_[{4}]_auto PRIMARY KEY CLUSTERED ({5}) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
)  
"@ -f $KeyColumns[0].Database,$KeyColumns[0].Schema,$KeyColumns[0].TableName,($KeyColumns[0].TableName -replace '\s',''),($KeyColumns.ColumnName -replace "^(.{1,})$",'[$1]' -join ",")
    $this.ExecuteNonQuery($Query)
    return $true
    
  }
  
  [Void] DropPrimaryKey() {
    $SQLColumn=$this.Columns.where{-not [string]::IsNullOrEmpty($_.PrimaryKey)}
    $Query='ALTER TABLE [{0}].[{1}].[{2}] DROP CONSTRAINT [{3}]' -f $SQLColumn.Database,$SQLColumn.Schema,$SQLColumn.TableName,$SQLColumn.PrimaryKey
    [SQLQuery]::new($Query,$this.Connection).ExecuteNonQuery()
  }
  
  [Bool] Test() {
    $Query = "SELECT COUNT(*) AS num FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$($this.Schema)' AND LOWER(TABLE_NAME) = '$($this.Name.tolower())'"
    if ($this.ExecuteQuery($Query).num -ne 0) {
      return $true
    }else {
      return $false
    }
  }
  
  [Void] Insert([SQLClass[]]$SQLClassInstances) {
    foreach ($Instance in $SQLClassInstances) {
      
    }
  } 
}