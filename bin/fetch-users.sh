#!/bin/sh

EXPORT_JOB_ID=$(curl -L "${AUTH0_TENANT_DOMAIN_URL}/api/v2/jobs/users-exports" \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-H "Authorization: Bearer ${AUTH0_API_TOKEN}" \
-d '{"format":"json","fields":[{"name":"user_id"},{"name":"email"},{"name":"email_verified"},{"name":"name"},{"name":"family_name"},{"name":"given_name"},{"name":"nickname"},{"name":"picture"},{"name":"identities[0].connection","export_as":"provider"},{"name":"created_at"},{"name":"updated_at"},{"name":"app_metadata.region","export_as":"region"},{"name":"app_metadata.workos_user_id","export_as":"workos_user_id"}]}' \
-s | jq -r .id)

echo Export job ID: ${EXPORT_JOB_ID}

while true; do
  STATUS=$(curl -L "${AUTH0_TENANT_DOMAIN_URL}/api/v2/jobs/${EXPORT_JOB_ID}" \
  -H 'Accept: application/json' \
  -H "Authorization: Bearer ${AUTH0_API_TOKEN}" \
  -s | jq -r .status)
  
  echo "Current status: ${STATUS}"
  
  if [ "${STATUS}" = "completed" ]; then
    break
  fi
  
  sleep 5
done

URL=$(curl -L "${AUTH0_TENANT_DOMAIN_URL}/api/v2/jobs/${EXPORT_JOB_ID}" \
  -H 'Accept: application/json' \
  -H "Authorization: Bearer ${AUTH0_API_TOKEN}" \
  -s | jq -r .location)

echo "Downloading users from ${URL}"

curl -L "${URL}" -o out/users.jsonl.gz
gunzip out/users.jsonl.gz

echo "Done"
