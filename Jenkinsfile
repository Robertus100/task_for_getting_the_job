pipeline {
  agent {
       label 'linux'
    }
  environment {
	  ZIP_NAME = 'pipe-idd-flex.zip'
      REPO_NAME = "${scm.getUserRemoteConfigs()[0].getUrl().tokenize('/').last().split("\\.")[0]}"
      BRANCH = "${env.GIT_BRANCH}"
    }
  stages {
//     stage('test') {
//       steps {
//         echo 'Repository name: ' + REPO_NAME
//         echo 'Branch: ' + BRANCH
//         checkParameters(BRANCH)
        
        
        
//         // readJSON file: 'parameters' ++'/input.json'
//       }
//     }
//   }
// }

// def checkParameters(String branch){
//   echo 'Checking parameters in branch: ' + branch
//   // def parameters_dir = new File()
//   def parameter_files = 'ls parameters/' + branch.execute()
//   parameter_files.text.eachLine {
//     echo it
//   }
// }

  	stage('Delete zip if exist') {
          steps {
            sh 'rm -rf ${ZIP_NAME}'
            sh 'ls -la'
          }
        }
    stage('Create zip from Artifacts') {
          steps {
            zip zipFile: "${ZIP_NAME}", archive: true
          }
        }
    stage('upload zip file to S3 DEV env') {
        when { 
            branch 'dev' 
            }
          steps {
              script{withAWS(region:'eu-west-1', credentials:'enterprise-landing-ingestion-dev') {
              s3Upload bucket: 'enterprise-landing-ing-pipeline-source-bucket-dev', includePathPattern:"${ZIP_NAME}"
					}
				}
          }
    }
    stage('upload zip file to S3 QA env') {
        when { 
            branch 'qa' 
            }
          steps {
              script{withAWS(region:'eu-west-1', credentials:'enterprise-landing-ingestion-qa') {
              s3Upload bucket: 'enterprise-landing-ing-pipeline-source-bucket-qa', includePathPattern:"${ZIP_NAME}"
					}
				}
          }
    }
    stage('upload zip file to S3 Prod env') {
        when { 
            branch 'master' 
            }
          steps {
              script{withAWS(region:'eu-west-1', credentials:'enterprise-landing-ingestion-prd') {
              s3Upload bucket: 'enterprise-landing-ing-pipeline-source-bucket-prd', includePathPattern:"${ZIP_NAME}"
					}
				}
            }
        }
    }
}
