def label = "sje-${UUID.randomUUID().toString()}"

podTemplate(label: label, inheritFrom: 'default',
yaml:'''
apiVersion: v1
kind: Pod
spec:
  shareProcessNamespace: true
''',
        volumes: [
                secretVolume(secretName: 'maven-settings', mountPath: '/maven-settings'),
                emptyDirVolume(mountPath: '/var/run')
        ],
        containers: [
                containerTemplate(name: 'build', image: 'eu.gcr.io/vg1np-pf-phenix-caas-1a/phenix/build/maven-builder:2',
                        ttyEnabled: true, privileged: true, runAsUser: '1000',
                        command: 'cat')
        ]) {

    node(label) {
        container('build') {

            checkout scm

            stage('set up maven') {
                sh 'mkdir -p ${HOME}/.m2 && cp /maven-settings/settings.xml  /maven-settings/settings-security.xml ${HOME}/.m2/'
            }

            stage('Compile') {
                sh('${MAVEN_HOME}/bin/mvn clean compile')
            }

            stage('Tests') {
                sh('${MAVEN_HOME}/bin/mvn clean verify')
            }

            stage('Long Tests') {
                sh('''#!/bin/bash -le
                    ${MAVEN_HOME}/bin/mvn -B -Pmedium \
                        -Dtest=ProvidedQueryCCMIT \
                        -Ddsbulk.ccm.CCM_VERSION=3.11.11 \
                        -Ddsbulk.ccm.CCM_IS_DSE=false \
                        -Dmaven.test.failure.ignore=false \
                        -Dmax.simulacron.clusters=2 \
                        -Dmax.ccm.clusters=1 \
                        -DfailIfNoTests=false \
                                clean verify
                ''')
            }

            stage('Deploy') {
                sh('''
                    ${MAVEN_HOME}/bin/mvn \
                    -DaltReleaseDeploymentRepository=carrefour-releases::default::http://nexus:31100/content/repositories/stables \
                    -DaltSnapshotDeploymentRepository=carrefour-snapshots::default::http://nexus:31100/content/repositories/snapshots \
                    deploy
                    ''')
            }
        }
    }
}
