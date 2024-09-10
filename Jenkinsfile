// Copyright (c) YugaByte, Inc.

properties([
    parameters([
        string(defaultValue: 'main', description: 'Specify the Branch name', name: 'BRANCH'),
        string(defaultValue: '2.19.1.0', description: 'YB DB version', name: 'YB_VERSION'),
        booleanParam(defaultValue: false, description: 'If checked release builds are uploaded to s3 bucket. (debezium-connector -> s3://releases.yugabyte.com/debezium-connector-yugabytedb)', name: 'PUBLISH_TO_S3')
    ])
])

pipeline {
     agent {
        node { label 'cdcsdk-docker-agent' }
    }
    options {
        timeout(time: 8, unit: 'HOURS')
    }
    environment {
        RELEASE_BUCKET_PATH = "s3://releases.yugabyte.com/debezium-connector-yugabytedb"
        YB_VERSION = "${params.YB_VERSION}"
    }
    stages {
        stage('Clone Project') {
            steps {
                git branch: '${BRANCH}', url: 'https://github.com/yugabyte/debezium-connector-yugabytedb.git'
            }
        }
        stage("Setup environment") {
            steps {
                withCredentials([file(credentialsId: 'debezium-quay-auth', variable: 'debezium_quay_auth')]) {
                    sh 'mkdir -p $HOME/.docker'
                    sh 'cp ${debezium_quay_auth} $HOME/.docker/config.json'
                    sh 'chmod 600 $HOME/.docker/config.json'
                }
                script{
                    sh './.github/scripts/install_prerequisites.sh'
                    env.BUILD_NUMBER = sh(script: "curl -Lsk https://release.dev.yugabyte.com/releases/latest?version=${YB_VERSION}", returnStdout: true).trim()
                    env.YB_DOCKER_IMAGE="quay.io/yugabyte/yugabyte-itest:${YB_VERSION}-b${BUILD_NUMBER}"
                }
            }
        }
        stage("Check environment") {
            steps {
                script{
                    sh 'java -version'
                    sh 'mvn -version'
                }
            }
        }
        stage("Cache Dependencies") {
            steps {
                cache (path: "$HOME/.m2/repository", key: "debezium-connector-${hashFiles('pom.xml')}") {
                    sh 'mvn verify --fail-never -DskipTests -DskipITs'
                }
            }
        }
        stage('Build and Test') {
            steps {
                script{
                    env.PKG_VERSION = sh(script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout", returnStdout: true).trim()
                    env.ARTIFACT_ID = sh(script: "mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout", returnStdout: true).trim()
                    sh '''mvn clean test package \
                    -Dtest=!YugabyteDBColocatedTablesTest#shouldWorkWithMixOfColocatedAndNonColocatedTables,!YugabyteDBDatatypesTest#testEnumValue,!YugabyteDBConfigTest#shouldThrowExceptionWithWrongIncludeList
                    '''
                }
            }
        }
        stage('Publish artifacts'){
            steps {
                script {
                    if (params.PUBLISH_TO_S3) {
                        sh '''
                        SHORT_COMMIT=$(git rev-parse --short HEAD)
                        mv target/${ARTIFACT_ID}-${PKG_VERSION}.jar target/${ARTIFACT_ID}-${PKG_VERSION}-${SHORT_COMMIT}.jar
                        aws s3 cp --recursive --exclude="*" --include="*.jar" target ${RELEASE_BUCKET_PATH}/${PKG_VERSION}
                        '''
                    }
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '**/*Test.txt', fingerprint: true
            cleanWs()
        }
        success {
            slackSend(
                color: "good",
                channel: "#cdc-jenkins-runs",
                message: "Debezium connector daily master test Job Passed - ${BUILD_URL}."
            )
        }
        aborted {
            slackSend(
                color: "danger",
                channel: "#cdc-jenkins-runs",
                message: "Debezium connector daily master test Job Aborted - ${BUILD_URL}."
            )
        }
        failure {
            slackSend(
                color: "danger",
                channel: "#cdc-jenkins-runs",
                message: "Debezium connector daily master test Job Failed - ${BUILD_URL}."
            )
        }
    }
}