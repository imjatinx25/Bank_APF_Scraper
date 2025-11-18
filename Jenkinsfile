
pipeline {
    agent any

    triggers {
        githubPush()  // Trigger build on GitHub push
    }


    environment {
        AWS_REGION = 'ap-south-1'
        DOCKER_REGISTRY = '676206929524.dkr.ecr.ap-south-1.amazonaws.com'
        DOCKER_IMAGE = 'dev-orbit-pem'
        DOCKER_NAME = 'jatin'
        DOCKER_TAG = "${DOCKER_IMAGE}:${DOCKER_NAME}${BUILD_NUMBER}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Check Secret Content') {
        steps {
            withCredentials([file(credentialsId: 'apf_scrapping', variable: 'ENV_FILE2')]) {
                sh '''
                    echo "Secret file content:"
                    cat $ENV_FILE2
                '''
                }
            }
        }

        stage('Inject .env from Jenkins Secret') {
            steps {
                withCredentials([file(credentialsId: 'jatin_env', variable: 'ENV_FILE2')]) {
                    sh '''
                        rm -f .env
                        cp $ENV_FILE2 .env
                    '''
                }
            }
        }

        stage('Login to AWS ECR') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'aws-credentials', usernameVariable: 'AWS_ACCESS_KEY_ID', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                    sh '''
                        aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
                        aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
                        aws configure set default.region ${AWS_REGION}
                        aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${DOCKER_REGISTRY}
                    '''
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                sh 'docker build -t ${DOCKER_TAG} .'
            }
        }

        stage('Tag Docker Image') {
            steps {
                sh 'docker tag ${DOCKER_TAG} ${DOCKER_REGISTRY}/${DOCKER_TAG}'
            }
        }

        stage('Push Docker Image to ECR') {
            steps {
                sh 'docker push ${DOCKER_REGISTRY}/${DOCKER_TAG}'
            }
        }

        stage('Stop and Remove Old Docker Container Running on Port 4000') {
            steps {
                sh '''
                    container_id=$(docker ps -q --filter "publish=4000")
                    if [ -n "$container_id" ]; then
                        docker stop $container_id
                        docker rm $container_id
                        echo 'Old container stopped and removed'
                    else
                        echo 'No container running on port 4000'
                    fi
                '''
            }
        }

        stage('Run New Docker Container on Port 4000') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'aws-credentials', usernameVariable: 'AWS_ACCESS_KEY_ID', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                    sh '''
                        bash -c '
                        export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
                        export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
                        docker run -d -p 4000:4000 \
                            -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
                            -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
                            --env-file .env \
                            ${DOCKER_REGISTRY}/${DOCKER_TAG}
                        '
                    '''
                }
            }
        }
    }

    post {
        success {
            slackSend (
                tokenCredentialId: 'slack_channel_secret',
                message: "✅ Build SUCCESSFUL: ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
                channel: '#jenekin_update',
                color: 'good',
                iconEmoji: ':white_check_mark:',
                username: 'Jenkins'
            )
        }
        failure {
            slackSend (
                tokenCredentialId: 'slack_channel_secret',
                message: "❌ Build FAILED: ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
                channel: '#jenekin_update',
                color: 'danger',
                iconEmoji: ':x:',
                username: 'Jenkins'
            )
            
        }
        always {
            cleanWs()
        }
    }
}
