pipeline {
    agent any
    stages{
    stage ('Build') {
    steps{
    git url: 'https://github.com/rouchoum/project2.git'
    withMaven {
      bat "mvn clean install"
    } // withMaven will discover the generated Maven artifacts, JUnit Surefire & FailSafe reports and FindBugs reports
  }
  }
  }
}
