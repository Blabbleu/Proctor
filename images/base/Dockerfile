# swe-pilot base runner image: OS tools, git, Python, Node, and the Claude Code harness binary.
# Project images build FROM this and add the project's dependencies only.
FROM node:24-trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        git rsync ca-certificates python3 python3-venv python3-pip procps ripgrep less \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -g 10001 swe && useradd -u 10001 -g 10001 -d /home/swe -m -s /bin/bash swe \
    && mkdir -p /work /job/in /job/out /job/data /opt/harness && chown swe:swe /work /job/out
COPY claude /opt/harness/claude
COPY codex codex-code-mode-host /opt/harness/
COPY runner/ /opt/swe/
RUN chmod 0755 /opt/harness/claude /opt/harness/codex /opt/harness/codex-code-mode-host /opt/swe/run.sh \
    && chmod -R a+rX /opt/swe
ENV PATH=/opt/harness:/usr/local/bin:/usr/bin:/bin \
    GIT_CONFIG_GLOBAL=/opt/swe/gitconfig \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    npm_config_update_notifier=false
WORKDIR /work
USER 10001:10001
