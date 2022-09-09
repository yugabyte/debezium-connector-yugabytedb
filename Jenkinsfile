// Copyright (c) YugaByte, Inc.

properties([
    parameters([
        string(defaultValue: 'main', description: 'Specify the Branch name', name: 'BRANCH'),
        booleanParam(defaultValue: false, description: 'If checked release builds are uploaded to s3 bucket. (debezium-connector -> s3://releases.yugabyte.com/debezium-connector-yugabytedb)', name: 'PUBLISH_TO_S3')
    ])
])

pipeline {
     agent {
        node { label 'ybc-docker-agent' }
    }
    environment {
        RELEASE_BUCKET_PATH = "s3://releases.yugabyte.com/debezium-connector-yugabytedb"
    }
    stages {
        // stage('Clone Project') {
        //     steps {                  
        //         git branch: '${BRANCH}', url: 'https://github.com/yugabyte/debezium-connector-yugabytedb.git'
        //     }
        // }
        stage('Build and Test') {
            steps {
                script{
                    env.PKG_VERSION = sh(script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout", returnStdout: true).trim()
                    env.ARTIFACT_ID = sh(script: "mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout", returnStdout: true).trim()
                    sh 'mvn clean test package'
                }
            }
        }
        stage('Publish artifacts'){
            steps {
                script {
                    if (env.PUBLISH_TO_S3) {
                        // sh '''
                        // SHORT_COMMIT=$(git rev-parse --short HEAD)
                        // mv target/${ARTIFACT_ID}-${PKG_VERSION}.jar target/${ARTIFACT_ID}-${PKG_VERSION}-${SHORT_COMMIT}.jar
                        // aws s3 cp --recursive --exclude="*" --include="*.jar" target ${RELEASE_BUCKET_PATH}/${PKG_VERSION}
                        // '''
                        sh '''
                        mvn release:clean
                        mvn release:prepare -DignoreSnapshots=True -Darguments="-DskipTests -Ddocker.skip=True -Dmaven.javadoc.skip=true"
                        mvn release:perform -DignoreSnapshots=True -Darguments="-DskipTests -Ddocker.skip=True -Dmaven.javadoc.skip=true"
                        '''
                    }
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '**/*Test.txt', fingerprint: true
            // cleanWs()
        }
    }
}