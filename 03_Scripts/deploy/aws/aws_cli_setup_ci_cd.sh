#!/usr/bin/env bash
set -euo pipefail

# Purpose:
# - Setup/update IAM role for GitHub OIDC deployment
# - Ensure ECR repository exists
# - Render ECS task definition from template
#
# Required env vars:
#   AWS_REGION
#   AWS_ACCOUNT_ID
#   GITHUB_ORG
#   GITHUB_REPO
#   ECR_REPOSITORY
#   GITHUB_DEPLOY_ROLE_NAME
#   TASK_EXECUTION_ROLE_NAME
#   TASK_ROLE_NAME
#   EFS_FILE_SYSTEM_ID
#
# Optional env vars:
#   ECS_CLUSTER
#   ECS_SERVICE
#   ECS_CONTAINER_NAME (default: jato-dashboard)
#   IMAGE_TAG (default: bootstrap)

required_vars=(
  AWS_REGION
  AWS_ACCOUNT_ID
  GITHUB_ORG
  GITHUB_REPO
  ECR_REPOSITORY
  GITHUB_DEPLOY_ROLE_NAME
  TASK_EXECUTION_ROLE_NAME
  TASK_ROLE_NAME
  EFS_FILE_SYSTEM_ID
)

for key in "${required_vars[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    echo "[ERROR] Missing required env: $key"
    exit 1
  fi
done

ECS_CONTAINER_NAME="${ECS_CONTAINER_NAME:-jato-dashboard}"
IMAGE_TAG="${IMAGE_TAG:-bootstrap}"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${SCRIPT_DIR}/rendered"
mkdir -p "$WORK_DIR"

render_template() {
  local src="$1"
  local dst="$2"
  cp "$src" "$dst"
  sed -i.bak "s#<AWS_ACCOUNT_ID>#${AWS_ACCOUNT_ID}#g" "$dst"
  sed -i.bak "s#<AWS_REGION>#${AWS_REGION}#g" "$dst"
  sed -i.bak "s#<GITHUB_ORG>#${GITHUB_ORG}#g" "$dst"
  sed -i.bak "s#<GITHUB_REPO>#${GITHUB_REPO}#g" "$dst"
  sed -i.bak "s#<ECR_REPOSITORY>#${ECR_REPOSITORY}#g" "$dst"
  sed -i.bak "s#<TASK_EXECUTION_ROLE_NAME>#${TASK_EXECUTION_ROLE_NAME}#g" "$dst"
  sed -i.bak "s#<TASK_ROLE_NAME>#${TASK_ROLE_NAME}#g" "$dst"
  sed -i.bak "s#<EFS_FILE_SYSTEM_ID>#${EFS_FILE_SYSTEM_ID}#g" "$dst"
  sed -i.bak "s#<IMAGE_URI>#${IMAGE_URI}#g" "$dst"
  rm -f "$dst.bak"
}

echo "[INFO] Checking OIDC provider token.actions.githubusercontent.com"
oidc_found=0
while IFS= read -r arn; do
  [[ -z "$arn" ]] && continue
  url=$(aws iam get-open-id-connect-provider \
    --open-id-connect-provider-arn "$arn" \
    --query 'Url' \
    --output text 2>/dev/null || true)
  if [[ "$url" == "token.actions.githubusercontent.com" ]]; then
    oidc_found=1
    break
  fi
done < <(aws iam list-open-id-connect-providers --query 'OpenIDConnectProviderList[].Arn' --output text | tr '\t' '\n')

if [[ "$oidc_found" -ne 1 ]]; then
  echo "[ERROR] GitHub OIDC provider not found in this AWS account."
  echo "Create it first, then rerun:"
  echo "aws iam create-open-id-connect-provider --url https://token.actions.githubusercontent.com --client-id-list sts.amazonaws.com --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1"
  exit 2
fi

TRUST_JSON="${WORK_DIR}/iam-github-oidc-trust-policy.rendered.json"
POLICY_JSON="${WORK_DIR}/iam-github-ecs-deploy-policy.rendered.json"
TASKDEF_JSON="${WORK_DIR}/ecs-taskdef.rendered.json"

render_template "${SCRIPT_DIR}/iam-github-oidc-trust-policy.json" "$TRUST_JSON"
render_template "${SCRIPT_DIR}/iam-github-ecs-deploy-policy.json" "$POLICY_JSON"
render_template "${SCRIPT_DIR}/ecs-taskdef.template.json" "$TASKDEF_JSON"

echo "[INFO] Creating/updating IAM role: ${GITHUB_DEPLOY_ROLE_NAME}"
if aws iam get-role --role-name "$GITHUB_DEPLOY_ROLE_NAME" >/dev/null 2>&1; then
  aws iam update-assume-role-policy \
    --role-name "$GITHUB_DEPLOY_ROLE_NAME" \
    --policy-document "file://${TRUST_JSON}" >/dev/null
else
  aws iam create-role \
    --role-name "$GITHUB_DEPLOY_ROLE_NAME" \
    --assume-role-policy-document "file://${TRUST_JSON}" >/dev/null
fi

aws iam put-role-policy \
  --role-name "$GITHUB_DEPLOY_ROLE_NAME" \
  --policy-name github-actions-ecs-deploy-inline \
  --policy-document "file://${POLICY_JSON}" >/dev/null

echo "[INFO] Ensuring ECR repository exists: ${ECR_REPOSITORY}"
if ! aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" >/dev/null 2>&1; then
  aws ecr create-repository --repository-name "$ECR_REPOSITORY" >/dev/null
fi

echo "[INFO] Ensuring CloudWatch log group exists: /ecs/jato-dashboard"
aws logs create-log-group --log-group-name /ecs/jato-dashboard >/dev/null 2>&1 || true

echo "[INFO] Rendered task definition at: ${TASKDEF_JSON}"

if aws ecr describe-images --repository-name "$ECR_REPOSITORY" --image-ids imageTag="$IMAGE_TAG" >/dev/null 2>&1; then
  echo "[INFO] Registering ECS task definition (image tag: ${IMAGE_TAG})"
  taskdef_arn=$(aws ecs register-task-definition --cli-input-json "file://${TASKDEF_JSON}" --query 'taskDefinition.taskDefinitionArn' --output text)
  echo "[INFO] task definition registered: ${taskdef_arn}"

  if [[ -n "${ECS_CLUSTER:-}" && -n "${ECS_SERVICE:-}" ]]; then
    if aws ecs describe-services --cluster "$ECS_CLUSTER" --services "$ECS_SERVICE" --query 'services[0].status' --output text >/dev/null 2>&1; then
      echo "[INFO] Updating ECS service to new task definition"
      aws ecs update-service \
        --cluster "$ECS_CLUSTER" \
        --service "$ECS_SERVICE" \
        --task-definition "$taskdef_arn" \
        --force-new-deployment >/dev/null
    else
      echo "[WARN] ECS service not found: ${ECS_CLUSTER}/${ECS_SERVICE}"
    fi
  fi
else
  echo "[WARN] ECR image tag '${IMAGE_TAG}' not found yet; skip task definition registration."
  echo "[WARN] Push image first (or set IMAGE_TAG to an existing tag), then rerun this script."
fi

role_arn=$(aws iam get-role --role-name "$GITHUB_DEPLOY_ROLE_NAME" --query 'Role.Arn' --output text)

echo ""
echo "[NEXT] Configure GitHub repository settings:"
echo "Secret  AWS_ROLE_TO_ASSUME=${role_arn}"
echo "Variable AWS_REGION=${AWS_REGION}"
echo "Variable ECR_REPOSITORY=${ECR_REPOSITORY}"
if [[ -n "${ECS_CLUSTER:-}" ]]; then
  echo "Variable ECS_CLUSTER=${ECS_CLUSTER}"
fi
if [[ -n "${ECS_SERVICE:-}" ]]; then
  echo "Variable ECS_SERVICE=${ECS_SERVICE}"
fi
echo "Variable ECS_CONTAINER_NAME=${ECS_CONTAINER_NAME}"
echo "Variable ECS_TASK_DEFINITION=jato-dashboard-task"
