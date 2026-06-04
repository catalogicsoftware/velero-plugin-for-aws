properties([
    parameters([
        booleanParam(
            name: "BUILD_AWS_PLUGIN",
            defaultValue: true,
            description: "Build and push velero-plugin-for-aws image"
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
    def baseVersion = "1.10.0"
    if (sourceBranch ==~ /^v\d+\.\d+\.\d+\.x$/) {
        baseVersion = sourceBranch.substring(1, sourceBranch.length() - 2)
    }

    def computedTag = "${baseVersion}-${sanitizedBranch}.${env.BUILD_NUMBER}"
    def imageTag = (params.IMAGE_TAG_OVERRIDE ?: "").trim() ? (params.IMAGE_TAG_OVERRIDE ?: "").trim() : computedTag

    def allowedBranches = ["v1.10.0.x", "jg-KUBEDR-7845"]
    def shouldBuild = (params.BUILD_AWS_PLUGIN ?: false) && allowedBranches.contains(sourceBranch)

    def dockerRegistryInternal = env.DOCKER_REGISTRY_INTERNAL
    def dockerRegistryCredsInternal = env.DOCKER_REGISTRY_CREDENTIALS_INTERNAL
    def dockerPrefixInternal = "${dockerRegistryInternal}/catalogicsoftware"
    def builderImage = env.BUILDER_IMAGE ?: "cc-nexus.ad.catalogic.us:8085/builder:latest"
    def imageName = "velero-plugin-for-aws"
    def imageRef = "${dockerPrefixInternal}/${imageName}:${imageTag}"

    stage("Build and push aws plugin image") {
        if (shouldBuild) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"
            docker.withRegistry("https://${dockerRegistryInternal}", dockerRegistryCredsInternal) {
                sh """
                    set -eu
                    git rev-parse --short HEAD > .gitsha
                    GIT_SHA_SHORT=\$(cat .gitsha)

                    docker run --rm \
                        -u \$(id -u):\$(id -g) \
                        -v \${WORKSPACE}:/workspace \
                        -w /workspace \
                        -e GOPATH=/workspace/.go \
                        -e GOMODCACHE=/workspace/.go/pkg/mod \
                        -e GOCACHE=/workspace/.go/cache \
                        ${builderImage} \
                        /bin/bash -lc 'set -eu; make local GOOS=linux GOARCH=amd64 VERSION=${imageTag}; make local GOOS=linux GOARCH=arm64 VERSION=${imageTag}'

                    docker buildx inspect multiarch >/dev/null 2>&1 || docker buildx create --name multiarch
                    docker buildx use multiarch

                    docker buildx build \
                        --build-arg COMMIT=\${GIT_SHA_SHORT} \
                        --build-arg IMAGE_TAG=${imageTag} \
                        --build-arg PLUGIN_PLATFORM=AWS \
                        -t ${imageRef} \
                        --platform=linux/amd64,linux/arm64 \
                        -f Dockerfile-common \
                        --push \
                        .
                """
            }

            writeFile file: "image-version", text: "${imageTag}\n"
            archiveArtifacts artifacts: "image-version", onlyIfSuccessful: true
            currentBuild.description = "${imageName}:${imageTag}"
            echo "Pushed internal image: ${imageRef}"
        } else {
            echo "Skipping build for branch '${sourceBranch}'. Allowed: ${allowedBranches.join(', ')}"
        }
    }
}
