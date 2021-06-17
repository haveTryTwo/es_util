#!/bin/sh

sleep_time=2

./es_tool -version
if [[ $? != 0 ]]; then
    exit -1
fi

function ExecEsTool() 
{
    if [[ $# != 1 ]]; then
        echo "Please input exec cfgpath"
        exit -1;
    fi
    echo "\n\n\n\n\n\n\n\n"

    local exec_cfg=$1
    ./es_tool -cfg $exec_cfg
    if [[ $? != 0 ]]; then
        exit -1
    fi

    sleep $sleep_time
}

ExecEsTool ./conf/HaveTryTwo_First_One/GetClusterHealth.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetClusterSettings.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/CheckClusterName.cfg


ExecEsTool ./conf/HaveTryTwo_First_One/CreateIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetCurrentRecovery.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/CloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetCloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/OpenIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetCloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetIndiceStatus.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetUnhealthIndicesWithNoClose.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/RecoveryUnhealthIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetUnhealthIndicesWithNoClose.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/SetIndiceSettings.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetIndiceSettings.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/SetIndiceMapping.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetIndiceMapping.cfg


ExecEsTool ./conf/HaveTryTwo_First_One/CloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetCloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/OpenIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetCloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetUnhealthIndicesWithNoClose.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/SetIndiceAllocationOnAndOff.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetUnhealthIndicesWithNoClose.cfg

ExecEsTool ./conf/HaveTryTwo_First_One/CloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetCloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/OpenIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetCloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetUnhealthIndicesWithNoClose.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/DataSink.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/GetUnhealthIndicesWithNoClose.cfg

ExecEsTool ./conf/HaveTryTwo_First_One/CloseIndices.cfg
ExecEsTool ./conf/HaveTryTwo_First_One/DeleteClosedIndices.cfg

ExecEsTool ./conf/HaveTryTwo_First_One/GetWholeIndices.cfg
