#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


# /data/atlpilo2/VOMSRenew.sh
export X509_USER_PROXY=/tmp/x509up
export RUCIO_ACCOUNT=atlpilo2

export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
localSetupPython
localSetupRucioClients

source /cvmfs/atlas.cern.ch/repo/sw/local/setup-yampl.sh
