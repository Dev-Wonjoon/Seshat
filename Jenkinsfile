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
            error("Expected ${repository} at ${revision}, got ${result.GIT_COMMIT}.")
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

    stages {
        stage('Check main revisions and CI') {
            steps {
                script {
                    env.SESHAT_DEPLOY_NEEDED = 'false'
                    def decision
                    withCredentials([usernamePassword(
                        credentialsId: 'seshat-github-read',
                        usernameVariable: 'GITHUB_USER',
                        passwordVariable: 'GITHUB_TOKEN'
                    )]) {
                        decision = sh(label: 'Check GitHub for a deployable revision pair', returnStdout: true, script: '''#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import ipaddress
import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request

private_ip = os.environ.get("SESHAT_PRIVATE_IP")
if not private_ip:
    raise SystemExit("Set SESHAT_PRIVATE_IP on the seshat-deploy Jenkins node.")
try:
    address = ipaddress.IPv4Address(private_ip)
except ValueError:
    raise SystemExit("SESHAT_PRIVATE_IP on the Jenkins node must be a private IPv4 address.")
if address.is_global or address.is_loopback or address.is_unspecified or address.is_multicast:
    raise SystemExit(f"Use the host's private network address for deployment, got {address}.")

def read_github(repository, path):
    url = f"https://api.github.com/repos/{repository}/{path}"
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + os.environ["GITHUB_TOKEN"],
        "X-GitHub-Api-Version": "2022-11-28",
    })
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise SystemExit(f"Could not read {repository} on GitHub (HTTP {error.code}). "
                         "Check repository access and Contents/Actions read permissions.")


def skip(reason):
    print(f"skip {reason}")
    raise SystemExit(0)


repositories = ("Dev-Wonjoon/Seshat", "Dev-Wonjoon/Seshat-web")
revisions = [read_github(repository, "git/ref/heads/main")["object"]["sha"]
             for repository in repositories]
state_file = Path.home() / ".local/state/seshat-deploy/revisions"
if state_file.exists() and state_file.read_text(encoding="utf-8").split() == revisions:
    skip("Both main revisions are already deployed.")

for repository, revision in zip(repositories, revisions):
    query = urllib.parse.urlencode({
        "branch": "main",
        "event": "push",
        "head_sha": revision,
        "per_page": 1,
    })
    runs = read_github(repository, f"actions/workflows/ci.yml/runs?{query}")["workflow_runs"]

    if not runs or runs[0]["head_sha"] != revision:
        skip(f"Waiting for a main push CI run for {repository} at {revision}.")
    run = runs[0]
    if run["status"] != "completed" or run["conclusion"] != "success":
        skip(f"CI has not passed for {repository} at {revision}: "
             f"{run['status']} / {run['conclusion']} ({run['html_url']})")
    print(f"CI passed for {repository} at {revision}: {run['html_url']}", file=sys.stderr)

print("deploy " + " ".join(revisions))
PY
''').trim()
                    }
                    if (decision.startsWith('skip ')) {
                        def reason = decision.substring(5)
                        currentBuild.description = reason
                        echo "No deployment: ${reason}"
                    } else {
                        if (!(decision ==~ /deploy [0-9a-f]{40} [0-9a-f]{40}/)) {
                            error('Unexpected deployment decision.')
                        }
                        def revisions = decision.split(' ')
                        env.BACKEND_SHA = revisions[1]
                        env.WEB_SHA = revisions[2]
                        env.SESHAT_DEPLOY_NEEDED = 'true'
                        echo "Selected backend ${env.BACKEND_SHA} and frontend ${env.WEB_SHA}"
                    }
                }
            }
        }

        stage('Check out verified revisions') {
            when { expression { env.SESHAT_DEPLOY_NEEDED == 'true' } }
            steps {
                script {
                    checkoutRevision('seshat', 'Dev-Wonjoon/Seshat', env.BACKEND_SHA)
                    checkoutRevision('seshat-web', 'Dev-Wonjoon/Seshat-web', env.WEB_SHA)
                }
            }
        }

        stage('Deploy and verify') {
            when { expression { env.SESHAT_DEPLOY_NEEDED == 'true' } }
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

state_dir="$HOME/.local/state/seshat-deploy"
mkdir -p "$state_dir"
revision_file=$(mktemp "$state_dir/revisions.XXXXXX")
trap 'rm -f "$revision_file"' EXIT
printf '%s %s\n' "$BACKEND_SHA" "$WEB_SHA" > "$revision_file"
mv -f "$revision_file" "$state_dir/revisions"

printf '%s\n' "Deployed backend: $BACKEND_SHA" "Deployed frontend: $WEB_SHA"
''')
                }
                script {
                    currentBuild.description = 'Deployed Seshat main revisions'
                }
            }
        }
    }
}
