#!/bin/bash
set -e

BASE=http://localhost:8080

echo "=== 1. Health Check ==="
curl -s $BASE/health
echo -e "\n"

echo "=== 2. Deploy todo-service ==="
DEPLOY=$(curl -s -X POST $BASE/api/services/deploy \
  -H "Content-Type: application/json" \
  -d @- <<'EOF'
{
  "tenant_slug": "acme",
  "manifest": {
    "name": "todo-service",
    "version": "0.1.0",
    "tables": [{
      "name": "todos",
      "columns": [
        {"name":"id","column_type":"uuid","primary_key":true,"nullable":false,"auto_generate":true},
        {"name":"title","column_type":"text","primary_key":false,"nullable":false,"auto_generate":false},
        {"name":"completed","column_type":"boolean","primary_key":false,"nullable":false,"auto_generate":false,"default_value":"false"},
        {"name":"created_at","column_type":"timestamp","primary_key":false,"nullable":false,"auto_generate":true},
        {"name":"updated_at","column_type":"timestamp","primary_key":false,"nullable":false,"auto_generate":true}
      ]
    }],
    "cells": [],
    "events": [],
    "subscriptions": [],
    "custom_routes": [],
    "mode": "crud"
  }
}
EOF
)
echo "$DEPLOY" | python3 -m json.tool
echo ""

echo "=== 3. Create a todo ==="
TODO=$(curl -s -X POST $BASE/svc/acme/todo-service/todos \
  -H "Content-Type: application/json" \
  -d '{"title":"Buy milk","completed":false}')
echo "$TODO" | python3 -m json.tool
TODO_ID=$(echo "$TODO" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null || echo "")
echo ""

echo "=== 4. Create another todo ==="
curl -s -X POST $BASE/svc/acme/todo-service/todos \
  -H "Content-Type: application/json" \
  -d '{"title":"Walk the dog","completed":true}' | python3 -m json.tool
echo ""

echo "=== 5. List all todos ==="
curl -s "$BASE/svc/acme/todo-service/todos" | python3 -m json.tool
echo ""

if [ -n "$TODO_ID" ]; then
  echo "=== 6. Get todo by ID ==="
  curl -s "$BASE/svc/acme/todo-service/todos/$TODO_ID" | python3 -m json.tool
  echo ""

  echo "=== 7. Update todo ==="
  curl -s -X PUT "$BASE/svc/acme/todo-service/todos/$TODO_ID" \
    -H "Content-Type: application/json" \
    -d '{"completed":true}' | python3 -m json.tool
  echo ""

  echo "=== 8. Filter: completed=false ==="
  curl -s "$BASE/svc/acme/todo-service/todos?filter=%5B%7B%22n%22%3A%22completed%22%2C%22f%22%3A%22%3D%22%2C%22c%22%3A%22AND%22%2C%22v%22%3A%22false%22%7D%5D" | python3 -m json.tool
  echo ""

  echo "=== 9. Delete todo ==="
  curl -s -X DELETE "$BASE/svc/acme/todo-service/todos/$TODO_ID"
  echo -e "\n"

  echo "=== 10. List after delete ==="
  curl -s "$BASE/svc/acme/todo-service/todos" | python3 -m json.tool
  echo ""
fi

echo "=== Done! ==="
