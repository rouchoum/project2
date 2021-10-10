pipeline {
    agent any
    stages{
    stage ('Build') {
    steps{
    git url: 'https://github.com/rouchoum/spark-job1.git'
    withMaven {
      bat "mvn clean verify"
    } // withMaven will discover the generated Maven artifacts, JUnit Surefire & FailSafe reports and FindBugs reports
  }
  }
  }
}
