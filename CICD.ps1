# usage for the CI script for ADF pipeline
# 1) how to link the script code with the actual py/jar reference in ADF pipeline?
    # get reference of execution code from adf_published templates
    # merge with the local scripts update --- need a clear script code tree:
    # sample: executoncode/type/pipeline/activity/<your_code>
    # used sample codes to download the current scripts (how to mark the changes of the code?)

# 2) how to update script code in devops?
    # step 1: update 
    # step 2: backup code with current version / upload script code
    
# 3) how to merger the script update within ADF pipeline in devops? 
    # update the adf_publish branch
    # update the execution code and ARM template
    
# 4) how to use the script code more efficiently?
    # need discusion


# scripts to dump execution code for selected ADF pipeline activity or source dataset
# input ADF pipeline template file

# Sample parameters:
# $armtemplate = "C:\GitHub\adftest\idgmcadf001dev\ARMTemplateForFactory.json"
# $parameters = "C:\GitHub\adftest\idgmcadf001dev\ARMTemplateParametersForFactory.json"
# get_adf_executon_code_map -armtemplate $armtemplate -parameters $parameters


param
(
    [parameter(Mandatory = $false)] [String] $armTemplate,
    [parameter(Mandatory = $false)] [String] $parameters,
    [parameter(Mandatory = $false)] [String] $ResourceGroupName,
    [parameter(Mandatory = $false)] [String] $DataFactoryName,
    [parameter(Mandatory = $false)] [Bool] $predeployment=$true,
    [parameter(Mandatory = $false)] [Bool] $deleteDeployment=$false,
    [parameter(Mandatory = $false)] [Bool] $updateexecutioncode=$false,
    [parameter(Mandatory = $false)] [string] $rootexecutioncodepath="executioncode",
    [Parameter(Mandatory=$false)][string]$kvurl
)

function getPipelineDependencies {
    param([System.Object] $activity)
    if ($activity.Pipeline) {
        return @($activity.Pipeline.ReferenceName)
    } elseif ($activity.Activities) {
        $result = @()
        $activity.Activities | ForEach-Object{ $result += getPipelineDependencies -activity $_ }
        return $result
    } elseif ($activity.ifFalseActivities -or $activity.ifTrueActivities) {
        $result = @()
        $activity.ifFalseActivities | Where-Object {$_ -ne $null} | ForEach-Object{ $result += getPipelineDependencies -activity $_ }
        $activity.ifTrueActivities | Where-Object {$_ -ne $null} | ForEach-Object{ $result += getPipelineDependencies -activity $_ }
        return $result
    } elseif ($activity.defaultActivities) {
        $result = @()
        $activity.defaultActivities | ForEach-Object{ $result += getPipelineDependencies -activity $_ }
        if ($activity.cases) {
            $activity.cases | ForEach-Object{ $_.activities } | ForEach-Object{$result += getPipelineDependencies -activity $_ }
        }
        return $result
    } else {
        return @()
    }
}

function pipelineSortUtil {
    param([Microsoft.Azure.Commands.DataFactoryV2.Models.PSPipeline]$pipeline,
    [Hashtable] $pipelineNameResourceDict,
    [Hashtable] $visited,
    [System.Collections.Stack] $sortedList)
    if ($visited[$pipeline.Name] -eq $true) {
        return;
    }
    $visited[$pipeline.Name] = $true;
    $pipeline.Activities | ForEach-Object{ getPipelineDependencies -activity $_ -pipelineNameResourceDict $pipelineNameResourceDict}  | ForEach-Object{
        pipelineSortUtil -pipeline $pipelineNameResourceDict[$_] -pipelineNameResourceDict $pipelineNameResourceDict -visited $visited -sortedList $sortedList
    }
    $sortedList.Push($pipeline)

}

function Get-SortedPipelines {
    param(
        [string] $DataFactoryName,
        [string] $ResourceGroupName
    )
    $pipelines = Get-AzDataFactoryV2Pipeline -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
    $ppDict = @{}
    $visited = @{}
    $stack = new-object System.Collections.Stack
    $pipelines | ForEach-Object{ $ppDict[$_.Name] = $_ }
    $pipelines | ForEach-Object{ pipelineSortUtil -pipeline $_ -pipelineNameResourceDict $ppDict -visited $visited -sortedList $stack }
    $sortedList = new-object Collections.Generic.List[Microsoft.Azure.Commands.DataFactoryV2.Models.PSPipeline]
    
    while ($stack.Count -gt 0) {
        $sortedList.Add($stack.Pop())
    }
    $sortedList
}

function triggerSortUtil {
    param([Microsoft.Azure.Commands.DataFactoryV2.Models.PSTrigger]$trigger,
    [Hashtable] $triggerNameResourceDict,
    [Hashtable] $visited,
    [System.Collections.Stack] $sortedList)
    if ($visited[$trigger.Name] -eq $true) {
        return;
    }
    $visited[$trigger.Name] = $true;
    if ($trigger.Properties.DependsOn) {
        $trigger.Properties.DependsOn | Where-Object {$_ -and $_.ReferenceTrigger} | ForEach-Object{
            triggerSortUtil -trigger $triggerNameResourceDict[$_.ReferenceTrigger.ReferenceName] -triggerNameResourceDict $triggerNameResourceDict -visited $visited -sortedList $sortedList
        }
    }
    $sortedList.Push($trigger)
}

function Get-SortedTriggers {
    param(
        [string] $DataFactoryName,
        [string] $ResourceGroupName
    )
    $triggers = Get-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName
    $triggerDict = @{}
    $visited = @{}
    $stack = new-object System.Collections.Stack
    $triggers | ForEach-Object{ $triggerDict[$_.Name] = $_ }
    $triggers | ForEach-Object{ triggerSortUtil -trigger $_ -triggerNameResourceDict $triggerDict -visited $visited -sortedList $stack }
    $sortedList = new-object Collections.Generic.List[Microsoft.Azure.Commands.DataFactoryV2.Models.PSTrigger]
    
    while ($stack.Count -gt 0) {
        $sortedList.Add($stack.Pop())
    }
    $sortedList
}

function Get-SortedLinkedServices {
    param(
        [string] $DataFactoryName,
        [string] $ResourceGroupName
    )
    $linkedServices = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName
    $LinkedServiceHasDependencies = @('HDInsightLinkedService', 'HDInsightOnDemandLinkedService', 'AzureBatchLinkedService')
    $Akv = 'AzureKeyVaultLinkedService'
    $HighOrderList = New-Object Collections.Generic.List[Microsoft.Azure.Commands.DataFactoryV2.Models.PSLinkedService]
    $RegularList = New-Object Collections.Generic.List[Microsoft.Azure.Commands.DataFactoryV2.Models.PSLinkedService]
    $AkvList = New-Object Collections.Generic.List[Microsoft.Azure.Commands.DataFactoryV2.Models.PSLinkedService]

    $linkedServices | ForEach-Object {
        if ($_.Properties.GetType().Name -in $LinkedServiceHasDependencies) {
            $HighOrderList.Add($_)
        }
        elseif ($_.Properties.GetType().Name -eq $Akv) {
            $AkvList.Add($_)
        }
        else {
            $RegularList.Add($_)
        }
    }

    $SortedList = New-Object Collections.Generic.List[Microsoft.Azure.Commands.DataFactoryV2.Models.PSLinkedService]($HighOrderList.Count + $RegularList.Count + $AkvList.Count)
    $SortedList.AddRange($HighOrderList)
    $SortedList.AddRange($RegularList)
    $SortedList.AddRange($AkvList)
    $SortedList
}



function get_adf_executon_code_map {
    param (
        [Parameter(Mandatory=$true)][string]$armtemplate,
        [Parameter(Mandatory=$true)][string]$parameters,
        [Parameter(Mandatory=$false)][string]$kvurl=""
    )

    # load template content 
    $adftemplate = get-content -path $armtemplate 
    $adfparameters =  $(get-content -path $parameters | convertfrom-json).parameters

    # go through each parameters and update adftemplate with extracted parameter value 
    foreach ($param in  $($adfparameters | get-member -type NoteProperty).name) {
        $value = $adfparameters.$($param).value
        $paramstring =  "[parameters('"+$param+"')]"
        $adftemplate = $adftemplate.replace($paramstring,$value)
    }

    $resources = $($adftemplate | convertfrom-json).resources
    # temnplate objects ARM structure referring:
    # https://docs.microsoft.com/en-us/azure/templates/microsoft.datafactory/2018-06-01/factories/pipelines?tabs=json

    $resourcehasharray = @{}
    $execution_code_hash=@{}

    # build hash table for adf resource list
    foreach ( $resource in $resources ) {

        if (!$resourcehasharray[$resource.type]) {
            $resourcehasharray[$resource.type] = @()
        }
            
        $tname = $resource.name 
        $oname = $tname.split(",")[-1].split("'")[1].trim("/").tostring()
        $resourcehasharray[$resource.type]+=[PSCustomObject]@{
                name = $oname
                properties =  $resource.properties   
        }    
    }



    # go through pipeline hash. get script properties for selected resource type. Like HDinsightSpark

    foreach ($pipeline  in $resourcehasharray['Microsoft.DataFactory/factories/pipelines']) {
        foreach ($activity in  $pipeline.properties.activities) {
                # only filter selected resource type. For example: HDInsightSpark
                if ($activity.type -eq 'HDInsightSpark') {

                    # map execution script path
                    $rootpath = $activity.typeProperties.rootPath
                    $container = $rootpath.split('/',2)[0].tostring()
                    $blobpath = $rootpath.split('/',2)[1]
                    if ($blobpath) { 
                        $blob = $blobpath.tostring()+"/"+$activity.typeProperties.entryFilePath
                    } else { 
                        $blob = $activity.typeProperties.entryFilePath 
                    }

                    # get the target storage linked service
                    $execution_storage_reference = $activity.typeProperties.sparkJobLinkedService.referenceName
                    # get the linked service for storage
                    $storagelink = $resourcehasharray['Microsoft.DataFactory/factories/linkedservices'] | where {$_.name -like $execution_storage_reference}
                    
                    $connectstring = $storagelink.properties.typeProperties.connectionString

                    # map to the exact storage connect string
                    # support to use keyvault secret only for storage accessing
                    # New-AzStorageContext -ConnectionString "<connect_stirng_from_keyvault_secret>"
                    if ( $connectstring.type -eq 'AzureKeyVaultSecret') {
                        # get the keyvault store url and secret name for connect string
                        $keyvaultlink = $resourcehasharray['Microsoft.DataFactory/factories/linkedservices'] | where {$_.name -like $connectstring.store.referenceName} 
                        
                        # override vault url if existing
                        if ($kvurl -ne "") {
                            $keyvault = $kvurl
                        } else {
                            $keyvault = $keyvaultlink.properties.typeProperties.baseUrl
                        }

                        $secret = $connectstring.secretName

                        # add the execution code reference
                        if (!$execution_code_hash[$activity.type]) {
                            $execution_code_hash[$activity.type] = @()
                        }
                        
                        # map the execution code for the target execution code 
                        $execution_code_hash[$activity.type]+=[PSCustomObject]@{
                            pipeline = $pipeline.name
                            activity =  $activity.name
                            keyvault =  $keyvault
                            secret = $secret
                            container = $container
                            blob = $blob
                        }    
                    } else {
                        # skip as the script storage is not using keyvault secret for connect string reference
                        continue;
                    }
                }

                if ($activity.type -eq 'HDInsightHive') {

                    # map execution script path
                    $rootpath = $activity.typeProperties.scriptPath
                    $container =  $rootpath.split('/',2)[0].tostring()
                    $blob = $rootpath.split('/',2)[1].tostring()
                    

                    # get the target storage linked service
                    $execution_storage_reference = $activity.typeProperties.scriptLinkedService.referenceName
                    # get the linked service for storage
                    $storagelink = $resourcehasharray['Microsoft.DataFactory/factories/linkedservices'] | where {$_.name -like $execution_storage_reference}
                    
                    $connectstring = $storagelink.properties.typeProperties.connectionString

                    # map to the exact storage connect string
                    # support to use keyvault secret only for storage accessing
                    # New-AzStorageContext -ConnectionString "<connect_stirng_from_keyvault_secret>"
                    if ( $connectstring.type -eq 'AzureKeyVaultSecret') {
                        # get the keyvault store url and secret name for connect string
                        $keyvaultlink = $resourcehasharray['Microsoft.DataFactory/factories/linkedservices'] | where {$_.name -like $connectstring.store.referenceName} 

                        # override vault url if existing
                        if ($kvurl -ne "") {
                            $keyvault = $kvurl
                        } else {
                            $keyvault = $keyvaultlink.properties.typeProperties.baseUrl
                        }

                        $secret = $connectstring.secretName

                        # add the execution code reference
                        if (!$execution_code_hash[$activity.type]) {
                            $execution_code_hash[$activity.type] = @()
                        }
                        
                        # map the execution code for the target execution code 
                        $execution_code_hash[$activity.type]+=[PSCustomObject]@{
                            pipeline = $pipeline.name
                            activity =  $activity.name
                            keyvault =  $keyvault
                            secret = $secret
                            container = $container
                            blob = $blob
                        }    
                    } else {
                        # skip as the script storage is not using keyvault secret for connect string reference
                        continue;
                    }
                }
        }
    }
    
    return  $execution_code_hash
}


# pull current version execution code to local
# sample code:
# download_adf_execution_code -armtemplate $armtemplate -parameters $parameters
function download_adf_execution_code {

    param (
        [Parameter(Mandatory=$true)][string]$armtemplate,
        [Parameter(Mandatory=$true)][string]$parameters,
        [Parameter(Mandatory=$false)][string]$kvurl="",
        [Parameter(Mandatory=$false)][string]$rootexecutioncodepath="executioncode"
    )

    $execution_code_hash = get_adf_executon_code_map -armtemplate $armtemplate -parameters $parameters -kvurl $kvurl

    foreach ($key in $execution_code_hash.keys) {

       foreach ($executionfile in $execution_code_hash[$key]) {
           
            # add local folder if not exists
            $localpath = "$rootexecutioncodepath/$($executionfile.pipeline)/$($executionfile.activity)"
            if(!(test-path -path $localpath)) {
                mkdir $localpath
            }

            $vaultname = $executionfile.keyvault.split(".",2)[0].split("//")[-1].tostring()
            $secretname =$executionfile.secret
            $secret = Get-AzKeyVaultSecret -VaultName $vaultname -name $secretname 
            $connectstring = $secret.SecretValueText

            # make destination as a local file path
            $filename =  $executionfile.blob.split("/")[-1].tostring()
            $destination = $localpath + "/" + $filename 

            # download the execution code from storage and save to local path
            $ctx = New-AzStorageContext -ConnectionString $connectstring
            get-AzStorageBlobContent -Container $executionfile.container -Blob $executionfile.blob -Context $ctx -Destination $destination -force
       }

    }

}



# push execution code to storage
function upload_adf_execution_code {

    param (
        [Parameter(Mandatory=$true)][string]$armtemplate,
        [Parameter(Mandatory=$true)][string]$parameters,
        [Parameter(Mandatory=$false)][string]$kvurl="",
        [Parameter(Mandatory=$false)][string]$rootexecutioncodepath="executioncode"
    )


    $execution_code_hash = get_adf_executon_code_map -armtemplate $armtemplate -parameters $parameters -kvurl $kvurl

    foreach ($key in $execution_code_hash.keys) {
       foreach ($executionfile in $execution_code_hash[$key]) {
           
            # add local folder if not exists
            $localpath = "$rootexecutioncodepath/$($executionfile.pipeline)/$($executionfile.activity)"
            write-host "execution code is in $localpath"

            # only upload the executon code if local folder existing
            if(test-path -path $localpath) {
                $files = get-childitem $localpath -erroraction ignore | where {$_.Attributes -ne 'Directory'}

                # local files existing
                if ($files) {
                    
                    $vaultname = $executionfile.keyvault.split(".",2)[0].split("//")[-1].tostring()
                    $secretname =$executionfile.secret
                    $secret = Get-AzKeyVaultSecret -VaultName $vaultname -name $secretname 
                    $connectstring = $secret.SecretValueText

                    write-host $connectstring
        
                    $ctx = New-AzStorageContext -ConnectionString $connectstring
                    $filename =  $executionfile.blob.split("/")[-1].tostring()

                    foreach ($file in $files) {
                        # upload the exact file only
                        if ($file.name -like $filename) {
                            # make a copy of current script to archived or with a release number
                            write-host "updated file $filename to " $executionfile.blob
                            $archived_blob = $executionfile.blob.tostring() +"_archived"
                            Start-AzStorageBlobCopy -SrcBlob  $executionfile.blob -SrcContainer  $executionfile.container -DestContainer  $executionfile.container -DestBlob $archived_blob -Context $ctx  -erroraction ignore
                            # upload the script content
                            set-AzStorageBlobContent -File $file.fullname -Container $executionfile.container -Blob $executionfile.blob -Context $ctx -force
                        }
                    }
                }
            }
      }
    }
}



$templateJson = Get-Content $armTemplate | ConvertFrom-Json
$resources = $templateJson.resources

#Triggers 
Write-Host "Getting triggers"
$triggersInTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/triggers" }
$triggerNamesInTemplate = $triggersInTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}

$triggersDeployed = Get-SortedTriggers -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName

$triggersToStop = $triggersDeployed | Where-Object { $triggerNamesInTemplate -contains $_.Name } | ForEach-Object { 
    New-Object PSObject -Property @{
        Name = $_.Name
        TriggerType = $_.Properties.GetType().Name 
    }
}
$triggersToDelete = $triggersDeployed | Where-Object { $triggerNamesInTemplate -notcontains $_.Name } | ForEach-Object { 
    New-Object PSObject -Property @{
        Name = $_.Name
        TriggerType = $_.Properties.GetType().Name 
    }
}
$triggersToStart = $triggersInTemplate | Where-Object { $_.properties.runtimeState -eq "Started" -and ($_.properties.pipelines.Count -gt 0 -or $_.properties.pipeline.pipelineReference -ne $null)} | ForEach-Object { 
    New-Object PSObject -Property @{
        Name = $_.name.Substring(37, $_.name.Length-40)
        TriggerType = $_.Properties.type
    }
}

if ($predeployment -eq $true) {
    #Stop all triggers
    Write-Host "Stopping deployed triggers`n"
    $triggersToStop | ForEach-Object {
        if ($_.TriggerType -eq "BlobEventsTrigger" -or $_.TriggerType -eq "CustomEventsTrigger") {
            Write-Host "Unsubscribing" $_.Name "from events"
            $status = Remove-AzDataFactoryV2TriggerSubscription -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name
            while ($status.Status -ne "Disabled"){
                Start-Sleep -s 15
                $status = Get-AzDataFactoryV2TriggerSubscriptionStatus -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name
            }
        }
        Write-Host "Stopping trigger" $_.Name
        Stop-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name -Force
    }
} else {
    if ($updateexecutioncode -eq $true) {
        # update executon code with giving params in kvurl
        Write-Host "upload execution code from local git source"
        upload_adf_execution_code -armtemplate $armtemplate -parameters $parameters -kvurl $kvurl

    } else {
        
        #Deleted resources
        #pipelines
        Write-Host "Getting pipelines"
        $pipelinesADF = Get-SortedPipelines -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
        $pipelinesTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/pipelines" }
        $pipelinesNames = $pipelinesTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}
        $deletedpipelines = $pipelinesADF | Where-Object { $pipelinesNames -notcontains $_.Name }
        #dataflows
        $dataflowsADF = Get-AzDataFactoryV2DataFlow -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
        $dataflowsTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/dataflows" }
        $dataflowsNames = $dataflowsTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40) }
        $deleteddataflow = $dataflowsADF | Where-Object { $dataflowsNames -notcontains $_.Name }
        #datasets
        Write-Host "Getting datasets"
        $datasetsADF = Get-AzDataFactoryV2Dataset -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
        $datasetsTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/datasets" }
        $datasetsNames = $datasetsTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40) }
        $deleteddataset = $datasetsADF | Where-Object { $datasetsNames -notcontains $_.Name }
        #linkedservices
        Write-Host "Getting linked services"
        $linkedservicesADF = Get-SortedLinkedServices -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
        $linkedservicesTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/linkedservices" }
        $linkedservicesNames = $linkedservicesTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}
        $deletedlinkedservices = $linkedservicesADF | Where-Object { $linkedservicesNames -notcontains $_.Name }
        #Integrationruntimes
        Write-Host "Getting integration runtimes"
        $integrationruntimesADF = Get-AzDataFactoryV2IntegrationRuntime -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName
        $integrationruntimesTemplate = $resources | Where-Object { $_.type -eq "Microsoft.DataFactory/factories/integrationruntimes" }
        $integrationruntimesNames = $integrationruntimesTemplate | ForEach-Object {$_.name.Substring(37, $_.name.Length-40)}
        $deletedintegrationruntimes = $integrationruntimesADF | Where-Object { $integrationruntimesNames -notcontains $_.Name }

        #Delete resources
        Write-Host "Deleting triggers"
        $triggersToDelete | ForEach-Object { 
            Write-Host "Deleting trigger "  $_.Name
            $trig = Get-AzDataFactoryV2Trigger -name $_.Name -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName
            if ($trig.RuntimeState -eq "Started") {
                if ($_.TriggerType -eq "BlobEventsTrigger" -or $_.TriggerType -eq "CustomEventsTrigger") {
                    Write-Host "Unsubscribing trigger" $_.Name "from events"
                    $status = Remove-AzDataFactoryV2TriggerSubscription -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name
                    while ($status.Status -ne "Disabled"){
                        Start-Sleep -s 15
                        $status = Get-AzDataFactoryV2TriggerSubscriptionStatus -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name
                    }
                }
                Stop-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name -Force 
            }
            Remove-AzDataFactoryV2Trigger -Name $_.Name -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force 
        }
        Write-Host "Deleting pipelines"
        $deletedpipelines | ForEach-Object { 
            Write-Host "Deleting pipeline " $_.Name
            Remove-AzDataFactoryV2Pipeline -Name $_.Name -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force 
        }
        Write-Host "Deleting dataflows"
        $deleteddataflow | ForEach-Object { 
            Write-Host "Deleting dataflow " $_.Name
            Remove-AzDataFactoryV2DataFlow -Name $_.Name -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force 
        }
        Write-Host "Deleting datasets"
        $deleteddataset | ForEach-Object { 
            Write-Host "Deleting dataset " $_.Name
            Remove-AzDataFactoryV2Dataset -Name $_.Name -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force 
        }
        Write-Host "Deleting linked services"
        $deletedlinkedservices | ForEach-Object { 
            Write-Host "Deleting Linked Service " $_.Name
            Remove-AzDataFactoryV2LinkedService -Name $_.Name -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force 
        }
        Write-Host "Deleting integration runtimes"
        $deletedintegrationruntimes | ForEach-Object { 
            Write-Host "Deleting integration runtime " $_.Name
            Remove-AzDataFactoryV2IntegrationRuntime -Name $_.Name -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Force 
        }

        if ($deleteDeployment -eq $true) {
            Write-Host "Deleting ARM deployment ... under resource group: " $ResourceGroupName
            $deployments = Get-AzResourceGroupDeployment -ResourceGroupName $ResourceGroupName
            $deploymentsToConsider = $deployments | Where { $_.DeploymentName -like "ArmTemplate_master*" -or $_.DeploymentName -like "ArmTemplateForFactory*" } | Sort-Object -Property Timestamp -Descending
            
            if ($deploymentsToConsider) { 

                $deploymentName = $deploymentsToConsider[0].DeploymentName

                Write-Host "Deployment to be deleted: " $deploymentName
                $deploymentOperations = Get-AzResourceGroupDeploymentOperation -DeploymentName $deploymentName -ResourceGroupName $ResourceGroupName
                $deploymentsToDelete = $deploymentOperations | Where { $_.properties.targetResource.id -like "*Microsoft.Resources/deployments*" }
    
                $deploymentsToDelete | ForEach-Object { 
                    Write-host "Deleting inner deployment: " $_.properties.targetResource.id
                    Remove-AzResourceGroupDeployment -Id $_.properties.targetResource.id
                }
                Write-Host "Deleting deployment: " $deploymentName
                Remove-AzResourceGroupDeployment -ResourceGroupName $ResourceGroupName -Name $deploymentName

            } else {
                write-host "skip as no new ARM deployment from devops"
            }

        }

        #Start active triggers - after cleanup efforts
        Write-Host "Starting active triggers"
        $triggersToStart | ForEach-Object { 
            if ($_.TriggerType -eq "BlobEventsTrigger" -or $_.TriggerType -eq "CustomEventsTrigger") {
                Write-Host "Subscribing" $_.Name "to events"
                $status = Add-AzDataFactoryV2TriggerSubscription -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name
                while ($status.Status -ne "Enabled"){
                    Start-Sleep -s 15
                    $status = Get-AzDataFactoryV2TriggerSubscriptionStatus -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name
                }
            }
            Write-Host "Starting trigger" $_.Name
            Start-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $_.Name -Force
        }
    }
}


