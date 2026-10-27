properties([
    parameters([
        booleanParam(
            name: "BUILD_AWS_PLUGIN",
            defaultValue: true,
            description: "Build and push velero-plugin-for-aws image"
        ),
        booleanParam(
            name: "PUSH_TO_ACR",
            defaultValue: true,
            description: "Also push the image to Azure Container Registry (ACR)"
        ),
        booleanParam(
            name: "UPDATE_VELERO_REPO",
            defaultValue: true,
            description: "Update the aws plugin image in plugins.ini of the velero repo"
        ),
        string(
            name: "VELERO_TARGET_BRANCH",
            defaultValue: "ks-v1.18.4.x",
            description: "Velero repo branch to update (defaults to the current branch name)"
        ),
        string(
            name: "IMAGE_TAG_OVERRIDE",
            defaultValue: "",
            description: "Optional explicit image tag override"
        )
    ])
])

node("cloudcasa-build") {
    stage("Checkout") {
        cleanWs()
        checkout scm
    }

    def sourceBranch = env.BRANCH_NAME ?: "unknown"
    def sanitizedBranch = sourceBranch.replaceAll('[^0-9A-Za-z-]', '-')

    // Follow amds-veleroplugin style: <baseVersion>-<branch>.<buildNumber>
    def baseVersion = "1.14.4"
    if (sourceBranch ==~ /^v\d+\.\d+\.\d+\.x$/) {
        baseVersion = sourceBranch.substring(1, sourceBranch.length() - 2)
    }

    def computedTag = "v${baseVersion}.${env.BUILD_NUMBER}"
    def imageTag = (params.IMAGE_TAG_OVERRIDE ?: "").trim() ? (params.IMAGE_TAG_OVERRIDE ?: "").trim() : computedTag

    def allowedBranches = ["v1.14.4.x"]
    def shouldBuild = (params.BUILD_AWS_PLUGIN ?: false) && allowedBranches.contains(sourceBranch)

    // Internal registry
    def dockerRegistryInternal = env.DOCKER_REGISTRY_INTERNAL
    def dockerRegistryCredsInternal = env.DOCKER_REGISTRY_CREDENTIALS_INTERNAL
    def dockerPrefixInternal = "${dockerRegistryInternal}/catalogicsoftware"

    // ACR (set these in Jenkins env / folder properties)
    //   CLOUDCASA_ACR_REGISTRY         e.g. myregistry.azurecr.io
    //   CLOUDCASA_ACR_CREDENTIALS_ID   Jenkins "Username with password" credential (SP id / secret)
    //   ACR_REPO_PREFIX                optional, defaults to catalogicsoftware
    def acrRegistry = env.CLOUDCASA_ACR_REGISTRY
    def acrCredsId = env.CLOUDCASA_ACR_CREDENTIALS_ID
    def acrPrefix = "${acrRegistry}/${env.ACR_REPO_PREFIX ?: 'catalogicsoftware'}"
    def shouldPushAcr = (params.PUSH_TO_ACR ?: false) && acrRegistry && acrCredsId

    def goBuilderImage = env.GO_BUILDER_IMAGE ?: "golang:1.26.0-bookworm"
    def commonBuildParams = """--rm \\
        -u \$(id -u):\$(id -g) \\
        -v \${WORKSPACE}:/workspace \\
        -w /workspace \\
        -e GOPATH=/workspace/.go \\
        -e GOMODCACHE=/workspace/.go/pkg/mod \\
        -e GOCACHE=/workspace/.go/cache"""
    def imageName = "velero-plugin-for-aws"
    def imageRef = "${dockerPrefixInternal}/${imageName}:${imageTag}"
    def acrImageRef = "${acrPrefix}/${imageName}:${imageTag}"

    // One -t flag per destination
    def tagFlags = "-t ${imageRef}"
    if (shouldPushAcr) {
        tagFlags += " -t ${acrImageRef}"
    }

    def runBuildAndPush = {
        sh """
            set -eu
            git rev-parse --short HEAD > .gitsha
            GIT_SHA_SHORT=\$(cat .gitsha)

            echo "Using go builder image: ${goBuilderImage}"
            docker run \
                ${commonBuildParams} \
                ${goBuilderImage} bash -c "
                    set -eu
                    go version
                    make local GOOS=linux GOARCH=amd64 VERSION=${imageTag}
                    make local GOOS=linux GOARCH=arm64 VERSION=${imageTag}
                "

            docker buildx inspect multiarch >/dev/null 2>&1 || docker buildx create --name multiarch
            docker buildx use multiarch

            docker buildx build \
                --build-arg COMMIT=\${GIT_SHA_SHORT} \
                --build-arg IMAGE_TAG=${imageTag} \
                --build-arg PLUGIN_PLATFORM=AWS \
                ${tagFlags} \
                --platform=linux/amd64,linux/arm64 \
                -f Dockerfile-common \
                --push \
                .
        """
    }

    stage("Build and push aws plugin image") {
        if (shouldBuild) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"

            docker.withRegistry("https://${dockerRegistryInternal}", dockerRegistryCredsInternal) {
                if (shouldPushAcr) {
                    withCredentials([usernamePassword(
                        credentialsId: acrCredsId,
                        usernameVariable: "ACR_USER",
                        passwordVariable: "ACR_PASS"
                    )]) {
                        // Single quotes so the secret is expanded by the shell, not Groovy
                        sh 'echo "$ACR_PASS" | docker login ' + acrRegistry + ' -u "$ACR_USER" --password-stdin'
                        try {
                            runBuildAndPush()
                        } finally {
                            sh "docker logout ${acrRegistry} || true"
                        }
                    }
                } else {
                    runBuildAndPush()
                }
            }

            writeFile file: "image-version", text: "${imageTag}\n"
            archiveArtifacts artifacts: "image-version", onlyIfSuccessful: true
            currentBuild.description = "${imageName}:${imageTag}"
            echo "Pushed internal image: ${imageRef}"
            if (shouldPushAcr) {
                echo "Pushed ACR image: ${acrImageRef}"
            } else {
                echo "ACR push skipped (PUSH_TO_ACR=false or ACR registry / credentials not set)"
            }
        } else {
            echo "Skipping build for branch '${sourceBranch}'. Allowed: ${allowedBranches.join(', ')}"
        }
    }

    stage("Update plugins.ini in velero repo") {
        if (shouldBuild && (params.UPDATE_VELERO_REPO ?: false)) {
            def veleroBranch = (params.VELERO_TARGET_BRANCH ?: "").trim() ?: sourceBranch
            def veleroRepoUrl = env.VELERO_REPO_URL   // e.g. https://github.com/catalogicsoftware/velero.git

            withCredentials([usernamePassword(
                credentialsId: 'github-access-token',
                usernameVariable: 'GIT_USER',
                passwordVariable: 'GIT_PASS'
            )]) {
                dir('velero-repo') {
                    git url: veleroRepoUrl, branch: veleroBranch, credentialsId: 'github-access-token'

                    sh """
                        git config user.name 'cloudcasabot'
                        git config user.email 'cloudcasabot@catalogicsoftware.com'
                    """

                    withEnv(["AWS_IMAGE=catalogicsoftware/${imageName}:${imageTag}"]) {
                        sh '''
                            set -eu
                            awk '
                                BEGIN { in_aws = 0 }
                                $0 == "[plugin:aws]" { in_aws = 1; print; next }
                                substr($0, 1, 1) == "[" { in_aws = 0 }
                                in_aws && $0 ~ /^image[[:space:]]*=/ {
                                    print "image = " ENVIRON["AWS_IMAGE"]
                                    next
                                }
                                { print }
                            ' plugins.ini > plugins.ini.tmp
                            mv plugins.ini.tmp plugins.ini
                        '''
                    }

                    sh """
                        set -eu
                        if [ -n "\$(git status --porcelain)" ]; then
                            git add plugins.ini
                            git commit -m "ci: bump ${imageName} to ${imageTag} [${env.JOB_NAME} #${env.BUILD_NUMBER}]"
                            git pull --rebase https://\${GIT_USER}:\${GIT_PASS}@${veleroRepoUrl.replaceFirst('https://', '')} ${veleroBranch}
                            git push https://\${GIT_USER}:\${GIT_PASS}@${veleroRepoUrl.replaceFirst('https://', '')} ${veleroBranch}
                        else
                            echo "plugins.ini already up to date"
                        fi
                    """
                }
            }
            echo "Updated [plugin:aws] in velero repo plugins.ini (${veleroBranch}) to ${imageName}:${imageTag}"
        } else {
            echo "Skipping velero repo update"
        }
    }
}
