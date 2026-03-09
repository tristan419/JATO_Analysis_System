# AWS Deployment Templates

These templates support GitHub OIDC + ECR + ECS deployment.

## Files

- `iam-github-oidc-trust-policy.json`: IAM trust policy for GitHub OIDC role.
- `iam-github-ecs-deploy-policy.json`: IAM permissions policy for ECR push + ECS deploy.
- `ecs-taskdef.template.json`: ECS task definition template (Fargate + EFS mount).
- `bootstrap_ubuntu.sh`: EC2 bootstrap script.
- `aws_cli_setup_ci_cd.sh`: AWS CLI script to setup IAM role/ECR/render taskdef.

## Replace Placeholders

Replace all placeholders before applying:

- `<AWS_ACCOUNT_ID>`
- `<AWS_REGION>`
- `<GITHUB_ORG>`
- `<GITHUB_REPO>`
- `<ECR_REPOSITORY>`
- `<TASK_EXECUTION_ROLE_NAME>`
- `<TASK_ROLE_NAME>`
- `<EFS_FILE_SYSTEM_ID>`

## Apply Order

1. Create IAM OIDC provider in AWS account (if not already present).
2. Create IAM role with `iam-github-oidc-trust-policy.json` trust policy.
3. Attach `iam-github-ecs-deploy-policy.json` to the role.
4. Put the role ARN into GitHub secret `AWS_ROLE_TO_ASSUME`.
5. Register ECS task definition using `ecs-taskdef.template.json` (after placeholder replacement).

## Quick Start (AWS CLI)

```bash
export AWS_REGION="ap-southeast-1"
export AWS_ACCOUNT_ID="123456789012"
export GITHUB_ORG="your-org"
export GITHUB_REPO="JATO_Analysis_System"
export ECR_REPOSITORY="jato-dashboard"
export GITHUB_DEPLOY_ROLE_NAME="github-actions-ecs-deploy"
export TASK_EXECUTION_ROLE_NAME="ecsTaskExecutionRole"
export TASK_ROLE_NAME="jatoDashboardTaskRole"
export EFS_FILE_SYSTEM_ID="fs-xxxxxxxx"

# Optional if already decided:
export ECS_CLUSTER="jato-dashboard-cluster"
export ECS_SERVICE="jato-dashboard-service"
export ECS_CONTAINER_NAME="jato-dashboard"

bash 03_Scripts/deploy/aws/aws_cli_setup_ci_cd.sh
```

After script success, copy printed values to GitHub repo settings:

- Secret: `AWS_ROLE_TO_ASSUME`
- Variables: `AWS_REGION`, `ECR_REPOSITORY`, `ECS_CLUSTER`, `ECS_SERVICE`, `ECS_CONTAINER_NAME`, `ECS_TASK_DEFINITION`
