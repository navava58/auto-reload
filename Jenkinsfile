pipeline{
    agent any
    stages{
        // stage("Clone"){
        //     steps{
        //         git "https://github.com/navava58/auto-reload.git"
        //     }
        // }
        stage("Build"){
            steps{
                withDockerRegistry(credentialsId: "docker-repo", url: "https://index.docker.io/v1/") {
                sh "docker build -t anhnn91:v10."
                sh "docker push anhnn91:v10."
                }
            }
        }
    }
}
