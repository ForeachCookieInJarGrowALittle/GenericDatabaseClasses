#requires -version 5
# this is a Pester test file

#region Further Reading
# http://www.powershellmagazine.com/2014/03/27/testing-your-powershell-scripts-with-pester-assertions-and-more/
#endregion
#region LoadScript
# load the script file into memory
# attention: make sure the script only contains function definitions
# and no active code. The entire script will be executed to load
# all functions into memory
. ($PSCommandPath -replace '\.tests\.ps1$', '.ps1')
#endregion
$Script:file=get-item "$($env:SystemRoot)\windowsupdate.log"
$Script:definition=[DynamicMasterClass]$file
[SQLServerConnection]::Connect([ConnectionString]::new('Localhost\SQLEXPRESS','Pester').ConnectionString)
# describes the function SQLDatabase
Describe 'DynamicSQL'  {
  # scenario 1: call the function without arguments
  # test 1: it does not throw an exception:
  Context  'Verify Method functionality'    {
    
    
    It  'Create Database Pester'  {
      # Gotcha: to use the "Should Not Throw" assertion,
      # make sure you place the command in a 
      # scriptblock (braces):
      # call function SQLDatabase and pipe the result to an assertion
      # Example:
      # SQLDatabase | Should Be 'Expected Output'
      # Hint: 
      # Once you typed "Should", press CTRL+J to see
      # available code snippets. You can also click anywhere
      # inside a "Should" and press CTRL+J to change assertion.
      # However, make sure the module "Pester" is
      # loaded to see the snippets. If the module is not loaded yet,
      # no snippets will show.
      { [SQLDatabase]::new('Pester').Create() } | Should Not Throw
    }
    
    It 'Database Pester should exist' {
      [SQLDatabase]::new('Pester').test() | Should be $true
    }
    
    It  'Convert System.IO.FileInfo to Instance of FileInfo:SQLClass'  {  
      Invoke-Expression $definition.Definition
      $script:fileinfo=Invoke-Expression "[$($definition.ClassName)]`$file"
      $fileinfo.GetType().fullname|should be 'FileInfo'
   
    }
    #$a|select Fullname,CreationTime,Exists,Directory,length,psparentpath
    It  'Create Table from Fileinfo'  {
      Invoke-Expression $definition.Definition
      $script:SQLTable = [SQLTable]::new($script:fileinfo)
      {$SQLTable.Create()}|should not throw
    }
    It  'Write FileInfo to Database'  {
      Invoke-Expression $definition.Definition
      $script:fileinfo.Insert()|should not throw
      $script:fileinfo.Select()|should not throw
    }
    It  'Get Fileinfo from Database by defining' {
      $RetrievedItem = Invoke-Expression "[SQLClass]::SelectFrom($($definition.ClassName),'ID = 1')"
      $RetrievedItem.gettype().fullname |should be 'Fileinfo'
      
    }
    It  'Get Fileinfo from Database' {
      Invoke-Expression $definition.Definition
      $newfileinfo=Invoke-Expression "[$($definition.ClassName)]::Select('ID = 1')"
      $newfileinfo.gettype().fullname|should be 'Fileinfo'
    }
    It  'Delete FileInfo from Database'  {
      Invoke-Expression $definition.Definition
      $Script:fileinfo.Delete()
    }
    It  'SetPrimaryKey and try to insert the item two more times'  {
      Invoke-Expression $definition.Definition
    }
    It  'Add NewColumn'  {
      Invoke-Expression $definition.Definition  
    }
    It  'Drop Column'  {
      Invoke-Expression $definition.Definition
    }
    It  'Drop Table and Definition'  {
      Invoke-Expression $definition.Definition
    }
    It  'Drop Database Pester'  {
      { [SQLDatabase]::new('Pester').Drop() } | Should Not Throw
    }
    It 'Database Pester should be gone' {
      [SQLDatabase]::new('Pester').test() | Should be $false
    }
  }
}
