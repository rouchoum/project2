pipeline {
    agent any
    options {
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }
    stages {
        stage('Build and deploy project (jar) ') {
            steps {
                    sh 'make build_deploy'
                }
            }
        stage('Packaging') {
            steps {
                        sh 'HTTP_PROXY=${JENKINS_HTTP_PROXY} \
                        make package'
            }
        }

    }
}