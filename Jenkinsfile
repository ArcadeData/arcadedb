#!/usr/bin/env groovy

/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */


node {
    try {
        properties([buildDiscarder(logRotator(artifactDaysToKeepStr: '10', artifactNumToKeepStr: '10', daysToKeepStr: '10', numToKeepStr: '10')), disableConcurrentBuilds()])

        cleanWs()

        stage('checkout') {
            checkout scm
        }
        docker.image('adoptopenjdk:11-jdk-hotspot').inside(' -u root -v /home/player/volumes/jenkins_home/.m2:/root/.m2"') {

            stage('check java') {
                sh "./mvnw -version"
            }

            stage('build') {
                try {
                    sh "./mvnw -fae --quiet -B clean install -DskipTests "
                } catch (err) {
                    throw err
                } finally {
//                    junit '**/surefire-reports/**/*.xml'
//                    step([$class       : 'JacocoPublisher',
//                          execPattern  : '**/**.exec',
//                          classPattern : '**/classes',
//                          sourcePattern: '**/src/main/java'])
                }
            }

            stage('build downstream') {
                build job: "trader/${env.BRANCH_NAME}", wait: false
            }
        }
        googlechatnotification url: 'id:chat_jenkins_id', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})"
    } catch (e) {
        currentBuild.result = 'FAILURE'
        googlechatnotification url: 'id:chat_jenkins_id', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}"
        throw e
    }

}
