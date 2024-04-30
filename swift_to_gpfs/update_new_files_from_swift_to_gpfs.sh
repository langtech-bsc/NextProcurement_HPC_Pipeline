#!/bin/bash

# Execute in dt cluster without sbatch!

# WARNING: RSYNC will delete in dest!!!!!!!!!!
#          use --dry-run will tell you files it wants to transfer and files 
#          it wants to delete on the remote.

export OS_AUTH_TYPE=v3applicationcredential
export OS_AUTH_URL=https://ncloud.bsc.es:5000/v3/
export OS_IDENTITY_API_VERSION=3
export OS_REGION_NAME="RegionOne"
export OS_INTERFACE=public
export OS_APPLICATION_CREDENTIAL_ID=<CRED_ID>
export OS_APPLICATION_CREDENTIAL_SECRET=<CRED_ID>



DESTINATION_GPFS_PATH=<INTERNAL PATH>data



dtrclone copy --progress nextp:PLACE/documentos $DESTINATION_GPFS_PATH
