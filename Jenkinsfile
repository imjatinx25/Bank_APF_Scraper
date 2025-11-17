#!/bin/bash
set -e

echo "===== Config ====="
AWS_REGION="ap-south-1"
DOCKER_REGISTRY="676206929524.dkr.ecr.ap-south-1.amazonaws.com"
DOCKER_IMAGE="dev-orbit-pem"
DOCKER_TAG="${DOCKER_IMAGE}:${BUILD_NUMBER}"

echo "Region: $AWS_REGION"
echo "Registry: $DOCKER_REGISTRY"
echo "Image: $DOCKER_IMAGE"
echo "Tag: $DOCKER_TAG"

echo "===== Debug branch ====="
git rev-parse --abbrev-ref HEAD || true

echo "===== Inject .env from Jenkins secret file (main only) ====="
if [ -n "$ENV_FILE" ]; then
  echo "Using ENV_FILE: $ENV_FILE"
  cp "$ENV_FILE" .env
  # Optional sanity check:
  [ -s .env ] || { echo ".env missing or empty"; exit 1; }
else
  echo "ENV_FILE not set – skipping .env injection"
fi

echo "===== Setup Python virtualenv & install deps ====="
python3 -m venv venv
# shellcheck source=/dev/null
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "===== Login to AWS ECR ====="
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$DOCKER_REGISTRY"

echo "===== Build Docker image ====="
docker build -t "$DOCKER_TAG" .

echo "===== Tag Docker image with registry path ====="
docker tag "$DOCKER_TAG" "${DOCKER_REGISTRY}/${DOCKER_TAG}"

echo "===== Push Docker image to ECR ====="
docker push "${DOCKER_REGISTRY}/${DOCKER_TAG}"

echo "===== Stop & remove old container on port 6000 ====="
container_id=$(docker ps -q --filter "publish=6000")
if [ -n "$container_id" ]; then
  docker stop "$container_id"
  docker rm "$container_id"
  echo "Old container stopped and removed"
else
  echo "No container running on port 6000"
fi

echo "===== Run new container on port 6000 ====="
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$DOCKER_REGISTRY"

docker run -d -p 6000:6000 \
  --label app=pem-api \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  "${DOCKER_REGISTRY}/${DOCKER_TAG}"

echo "===== Done ====="
