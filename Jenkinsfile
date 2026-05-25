pipeline {
    agent any

    environment {
        DEPLOY_HOST = 'user@server'
        DEPLOY_PATH = '/opt/airflow/dags/etl-template'
        GIT_BRANCH  = 'main'
        GCHAT_WEBHOOK = credentials('gchat-webhook-url')
    }

    options {
        timestamps()
        timeout(time: 15, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Setup') {
            steps {
                sh '''
                    curl -LsSf https://astral.sh/uv/install.sh | sh
                    export PATH="$HOME/.local/bin:$PATH"
                    uv sync
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                    export PATH="$HOME/.local/bin:$PATH"
                    uv run pytest tests/ -v --tb=short --junitxml=test-results.xml
                '''
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'test-results.xml'
                }
            }
        }

        stage('Deploy') {
            when {
                branch "${GIT_BRANCH}"
            }
            steps {
                sshagent(credentials: ['deploy-ssh-key']) {
                    sh """
                        ssh -o StrictHostKeyChecking=no ${DEPLOY_HOST} '
                            cd ${DEPLOY_PATH} && \
                            git pull origin ${GIT_BRANCH}
                        '
                    """
                }
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        success {
            echo 'Pipeline completed successfully'
            sh """
                curl -s -X POST "${GCHAT_WEBHOOK}" \
                    -H 'Content-Type: application/json' \
                    -d '{
                        "cards": [{
                            "header": { "title": "✅ Deploy Success", "subtitle": "${env.JOB_NAME}" },
                            "sections": [{ "widgets": [{ "keyValue": { "topLabel": "Branch", "content": "${GIT_BRANCH}" }}, { "keyValue": { "topLabel": "Build", "content": "#${env.BUILD_NUMBER}" }}, { "buttons": [{ "textButton": { "text": "Open Jenkins", "onClick": { "openLink": { "url": "${env.BUILD_URL}" }}}}]}]}]
                        }]
                    }'
            """
        }
        failure {
            echo 'Pipeline failed'
            sh """
                curl -s -X POST "${GCHAT_WEBHOOK}" \
                    -H 'Content-Type: application/json' \
                    -d '{
                        "cards": [{
                            "header": { "title": "❌ Deploy Failed", "subtitle": "${env.JOB_NAME}" },
                            "sections": [{ "widgets": [{ "keyValue": { "topLabel": "Branch", "content": "${GIT_BRANCH}" }}, { "keyValue": { "topLabel": "Build", "content": "#${env.BUILD_NUMBER}" }}, { "buttons": [{ "textButton": { "text": "Open Jenkins", "onClick": { "openLink": { "url": "${env.BUILD_URL}" }}}}]}]}]
                        }]
                    }'
            """
        }
    }
}
