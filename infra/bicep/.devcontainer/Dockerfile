# [Choice] .NET Core version: 3.1, 2.1
ARG VARIANT=3.1
FROM mcr.microsoft.com/vscode/devcontainers/dotnet:0-${VARIANT}

COPY library-scripts/azcli-debian.sh /tmp/library-scripts/
COPY requirements.txt /tmp/
RUN bash /tmp/library-scripts/azcli-debian.sh \
    # && az config set extension.use_dynamic_install=yes_without_prompt \
    # && az extension add --name databricks \
    && curl -Lo /usr/local/bin/bicep https://github.com/Azure/bicep/releases/latest/download/bicep-linux-x64 \
    && chmod +x /usr/local/bin/bicep \
    && apt-get install -y  python3-pip \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/library-scripts

RUN pip install -r /tmp/requirements.txt
# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

