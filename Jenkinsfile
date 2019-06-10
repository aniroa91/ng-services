pipeline {
    agent { node { label 'agent15' } }
    
    environment {
        HTTP_PROXY = 'http://proxy.hcm.fpt.vn:80'
        HTTPS_PROXY = 'http://proxy.hcm.fpt.vn:80'
        NO_PROXY = '172.0.0.1,*.local,172.27.11.0/24'
        GIT_COMMIT = sh(returnStdout: true, script: "git log -n 1 --pretty=format:'%h'").trim()
        K8S_SERVICE_NAME = 'web-churn'
        DOCKER_IMAGE = "${env.JOB_NAME}:${env.BUILD_ID}-${env.BUILD_TIMESTAMP}-${GIT_COMMIT}"
        EXPOSE_PORT = 9013
        CONTAINER_PORT = 9000
    }

    
    stages {
        stage('Build') {
            steps {
                echo "Compiling..."
                // sh 'printenv'
                sh "${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt dist"
            }
        }
        stage('Docker Publish') {
            steps {
                // Generate Jenkinsfile and prepare the artifact files.
                sh "${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt docker:stage"
                script {
                    docker.withRegistry('https://bigdata-registry.local:5043', '010ed969-34b5-473b-bcd9-01a207e7e382') {
                        def app = docker.build("${env.JOB_NAME}:${env.BUILD_ID}-${env.BUILD_TIMESTAMP}-${GIT_COMMIT}")
                        /* Push the container to the custom Registry */
                        app.push()
                    }
                }
            }
        }
        stage('Deploying'){
            steps {
                script {
                    docker.withRegistry('https://bigdata-registry.local:5043', '010ed969-34b5-473b-bcd9-01a207e7e382') {
                        sh "ssh -o StrictHostKeyChecking=no root@172.27.11.197 '/bin/sh /root/deploy_docker.sh ${env.JOB_NAME} ${DOCKER_IMAGE} ${EXPOSE_PORT} ${CONTAINER_PORT}'"
                    }
                }
            }
        }
    }
}
