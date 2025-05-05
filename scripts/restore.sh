#!/bin/bash
set -e
echo "Restoring dump …"
mongorestore --username "$MONGO_INITDB_ROOT_USERNAME" \
             --password "$MONGO_INITDB_ROOT_PASSWORD" \
             --authenticationDatabase admin \
             /dump && \
touch /data/db/RESTORE_DONE