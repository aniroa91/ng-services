pipeline {

    agent { node { label 'master' } }

    stages {
        stage('Build') {
            steps {
                echo "Compiling..."
                sh "${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt dist"
            }
        }
        stage('Docker Publish') {
            steps {
                // Generate Jenkinsfile and prepare the artifact files.
                sh "${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt docker:stage"

                script {
                    docker.withRegistry('https://bigdata-registry.local:5043', '010ed969-34b5-473b-bcd9-01a207e7e382') {
                        def app = docker.build("${env.JOB_NAME}:${env.BUILD_ID}")
                        /* Push the container to the custom Registry */
                        app.push()
                    }
                }
            }
        }
        
        
    }
}