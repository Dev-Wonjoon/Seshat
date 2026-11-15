def checkoutRevision(String directory, String repository, String revision) {
    dir(directory) {
        def result = checkout scmGit(
            branches: [[name: revision]],
            extensions: [cleanBeforeCheckout()],
            userRemoteConfigs: [[
                url: "https://github.com/${repository}.git",
                credentialsId: 'seshat-github-read',
                refspec: '+refs/heads/main:refs/remotes/origin/main'
            ]]
        )
        if (result.GIT_COMMIT != revision) {
            error("Expected ${repository} at ${revision}, got ${result.GIT_COMMIT}")
        }
    }
}

pipeline {
    agent { label 'seshat-deploy' }

    options {
        disableConcurrentBuilds()
        skipDefaultCheckout()
        timeout(time: 30, unit: 'MINUTES')
    }

    parameters {
        string(name: 'BACKEND_SHA', trim: true, description: 'Full commit SHA from Seshat main')
        string(name: 'WEB_SHA', trim: true, description: 'Full commit SHA from Seshat-web main')
        string(name: 'SESHAT_PRIVATE_IP', trim: true, description: 'Private IPv4 address assigned to the deploy host')
    }

    stages {
        stage('Verify revisions') {
            steps {
                script {
                    if (!(params.BACKEND_SHA ==~ /[0-9a-f]{40}/) ||
                        !(params.WEB_SHA ==~ /[0-9a-f]{40}/)) {
                        error('Enter full 40-character lowercase commit SHAs for both repositories.')
                    }
                }
                withCredentials([usernamePassword(
                    credentialsId: 'seshat-github-read',
                    usernameVariable: 'GITHUB_USER',
                    passwordVariable: 'GITHUB_TOKEN'
                )]) {
                    sh(label: 'Check GitHub Actions results', script: '''#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import ipaddress
import json
import os
import urllib.error
import urllib.parse
import urllib.request

try:
    address = ipaddress.IPv4Address(os.environ["SESHAT_PRIVATE_IP"])
except ValueError:
    raise SystemExit("Enter the deploy host's private IPv4 address.")
if address.is_global or address.is_loopback or address.is_unspecified or address.is_multicast:
    raise SystemExit(f"Use the host's private network address for deployment, got {address}.")

for repository, variable in (
    ("Dev-Wonjoon/Seshat", "BACKEND_SHA"),
    ("Dev-Wonjoon/Seshat-web", "WEB_SHA"),
):
    revision = os.environ[variable]
    query = urllib.parse.urlencode({
        "branch": "main",
        "event": "push",
        "head_sha": revision,
        "per_page": 1,
    })
    url = f"https://api.github.com/repos/{repository}/actions/workflows/ci.yml/runs?{query}"
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + os.environ["GITHUB_TOKEN"],
        "X-GitHub-Api-Version": "2022-11-28",
    })
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            runs = json.load(response)["workflow_runs"]
    except urllib.error.HTTPError as error:
        raise SystemExit(f"Could not read {repository} CI status (HTTP {error.code}). "
                         "Check the credential's repository access and Actions read permission.")

    if not runs or runs[0]["head_sha"] != revision:
        raise SystemExit(f"No main push CI run found for {repository} at {revision}.")
    run = runs[0]
    if run["status"] != "completed" or run["conclusion"] != "success":
        raise SystemExit(f"CI has not passed for {repository} at {revision}: "
                         f"{run['status']} / {run['conclusion']} ({run['html_url']})")
    print(f"CI passed for {repository} at {revision}: {run['html_url']}")
PY
''')
                }
            }
        }

        stage('Check out releases') {
            steps {
                script {
                    checkoutRevision('seshat', 'Dev-Wonjoon/Seshat', params.BACKEND_SHA)
                    checkoutRevision('seshat-web', 'Dev-Wonjoon/Seshat-web', params.WEB_SHA)
                }
            }
        }

        stage('Deploy and verify') {
            steps {
                dir('seshat') {
                    sh(label: 'Deploy Seshat with Compose', script: '''#!/usr/bin/env bash
set -euo pipefail

host_addresses=$(ip -o -4 addr show scope global | awk '{split($4, address, "/"); print address[1]}')
if ! grep -Fxq "$SESHAT_PRIVATE_IP" <<< "$host_addresses"; then
    echo "SESHAT_PRIVATE_IP is not assigned to this Jenkins agent: $SESHAT_PRIVATE_IP" >&2
    exit 1
fi

docker compose -f compose.experiment.yml config --quiet
docker compose -f compose.experiment.yml build central web
docker compose -f compose.experiment.yml up -d --wait --wait-timeout 120 central web
docker compose -f compose.experiment.yml ps

curl -fsS --noproxy '*' --retry 12 --retry-connrefused --retry-delay 2 \
  --retry-max-time 45 --max-time 5 http://127.0.0.1:5000/ -o /dev/null
curl -fsS --noproxy '*' --retry 12 --retry-connrefused --retry-delay 2 \
  --retry-max-time 45 --max-time 5 "http://${SESHAT_PRIVATE_IP}:8080/api/system/status" \
  | python3 -c 'import json, sys; status = json.load(sys.stdin); sys.exit(0 if status.get("instance_id") and status.get("metrics_status") else "Unexpected system status response")'

printf '%s\n' "Deployed backend: $BACKEND_SHA" "Deployed frontend: $WEB_SHA"
''')
                }
            }
        }
    }
}
