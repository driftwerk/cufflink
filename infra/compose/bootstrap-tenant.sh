#!/bin/sh
set -eu

PLATFORM_URL="${PLATFORM_URL:-http://platform:8080}"
KEYCLOAK_URL="${KEYCLOAK_URL:-http://keycloak:8080}"
TENANT_KEYCLOAK_URL="${TENANT_KEYCLOAK_URL:-http://keycloak:8080}"
TENANT_SLUG="${TENANT_SLUG:-acme}"
TENANT_NAME="${TENANT_NAME:-Acme}"
TENANT_REALM="${TENANT_REALM:-acme}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"

echo "Waiting for platform at $PLATFORM_URL ..."
i=0
until curl -fsS "$PLATFORM_URL/ready" >/dev/null 2>&1; do
  i=$((i+1)); [ "$i" -gt 120 ] && { echo "platform did not become ready"; exit 1; }
  sleep 2
done

echo "Waiting for Keycloak realm 'cufflink' at $KEYCLOAK_URL ..."
i=0
until curl -fsS "$KEYCLOAK_URL/realms/cufflink/.well-known/openid-configuration" >/dev/null 2>&1; do
  i=$((i+1)); [ "$i" -gt 120 ] && { echo "keycloak realm not available"; exit 1; }
  sleep 2
done

TOKEN=$(curl -fsS -X POST "$KEYCLOAK_URL/realms/cufflink/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=cufflink-cli \
  -d "username=$ADMIN_USER" -d "password=$ADMIN_PASSWORD" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
[ -n "$TOKEN" ] || { echo "could not obtain platform admin token"; exit 1; }

STATUS=$(curl -sS -o /tmp/resp.json -w '%{http_code}' -X POST "$PLATFORM_URL/api/tenants" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"name\":\"$TENANT_NAME\",\"slug\":\"$TENANT_SLUG\",\"keycloak_url\":\"$TENANT_KEYCLOAK_URL\",\"keycloak_realm\":\"$TENANT_REALM\",\"deploy_role\":\"cufflink-deploy\"}")

case "$STATUS" in
  200|201) echo "Created tenant '$TENANT_SLUG' (realm $TENANT_REALM)";;
  409)     echo "Tenant '$TENANT_SLUG' already exists";;
  *)       echo "Tenant creation failed (HTTP $STATUS):"; cat /tmp/resp.json; exit 1;;
esac
