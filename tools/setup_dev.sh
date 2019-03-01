#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RootDir="$( dirname "$CurrentDir" )"

source ${RootDir}/.venv/bin/activate
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${RootDir}/lib/externels/mysqlclient/

#export RUCIO_HOME=etc/rucio_client
export ESS_HOME=/afs/cern.ch/user/w/wguan/workdisk/ESS
