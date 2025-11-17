pipeline {
    agent any

    triggers {
        githubPush()  // Trigger build on GitHub push
    }

    environment {
        BRANCH_NAME = "${env.BRANCH_NAME}"  // Correctly quote the variable
    }

    stages {
        stage('Checkout') {
            when {
                branch 'main'  // Only trigger on main
            }
            steps {
                checkout scm
            }
        }

        stage('Inject .env from Jenkins Secret File') {
            when {
                branch 'main'
            }
            steps {
                withCredentials([file(credentialsId: 'apf_scrapping', variable: 'ENV_FILE')]) {
                    sh 'cp $ENV_FILE .env'
                }
            }
        }

        stage('Setup Python Env & Install Dependencies') {
            when {
                branch 'main'
            }
            steps {
                sh '''
                python3 -m venv venv
                source venv/bin/activate
                pip install --upgrade pip
                pip install -r requirements.txt
                '''
            }
        }
    }

    post {
        success {
            slackSend (
            tokenCredentialId: 'slack_channel_secret',  // The ID of the bot token credential in Jenkins
            message: "✅ Build SUCCESSFUL: ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
            channel: '#orbit',
            channel: '#jenekin_update',
            color: 'good',  // Optional: Green color for success
            iconEmoji: ':white_check_mark:',  // Optional: Emoji
            username: 'Jenkins'
        )
        }
        failure {
            slackSend (
            tokenCredentialId: 'slack_channel_secret',
            message: "❌ Build FAILED: ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
            channel: '#orbit',
            channel: '#jenekin_update',
            color: 'danger',  // Optional: Red color for failure
            iconEmoji: ':x:',  // Optional: Emoji
            username: 'Jenkins'
        )
        }
        always {
            cleanWs()
        }
    }
}